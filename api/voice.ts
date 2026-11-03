// POST /api/voice – Twilio's "A call comes in" webhook. Answers with ConversationRelay TwiML – except for a caller the
// Mac mini marked private: a fixed message and hang up. No AI, no transcription, no summary; the record says who and when.
import crypto from 'node:crypto'
import type { IncomingMessage, ServerResponse } from 'node:http'
import { digits, getConfig, K, LIMITS, lookup, privateMessages, publicUrl, readBody, RECORD_TTL, redis, twilioSignatureOk } from '../lib/util.js'

const esc = (s: string) => s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;')

// Female voice by default (ElevenLabs "Matilda", multilingual flash model – speaks Danish and English).
// Override per language in Vercel env: VOICE_DA / VOICE_EN, format <voiceId>-<model>-<speed>_<stability>_<similarity>
const VOICE_DA = process.env.VOICE_DA || 'XrExE9yKIg1WjnnlVkGX-flash_v2_5-0.9_0.6_0.8'
const VOICE_EN = process.env.VOICE_EN || 'XrExE9yKIg1WjnnlVkGX-flash_v2_5-0.9_0.6_0.8'

const xml = (res: ServerResponse, body: string) => {
  res.setHeader('Content-Type', 'text/xml')
  res.end(`<?xml version="1.0" encoding="UTF-8"?>\n<Response>${body}</Response>`)
}
// "busy" is rejected before the call is answered – no media minutes, no TTS, no Claude.
const busy = (res: ServerResponse) => xml(res, '<Reject reason="busy"/>')

/** Fixed-window counter; true while under the limit. Redis down → allow (the call agent stays reachable). */
async function under(key: string, limit: number) {
  try {
    const n = await redis.incr(key)
    if (n === 1) await redis.expire(key, 3600)
    return n <= limit
  } catch { return true }
}

export default async function handler(req: IncomingMessage, res: ServerResponse) {
  if (req.method !== 'POST') { res.statusCode = 405; return res.end() }
  let params: Record<string, string>
  try { params = Object.fromEntries(new URLSearchParams(await readBody(req, 20_000))) } catch { res.statusCode = 413; return res.end() }
  if (!twilioSignatureOk(publicUrl(req), params, req.headers['x-twilio-signature'] as string)) {
    res.statusCode = 403; return res.end('bad signature')
  }
  const sid = params.CallSid || ''
  if (!/^CA[0-9a-f]{32}$/.test(sid)) { res.statusCode = 400; return res.end() }
  // A signed webhook replayed later has the same CallSid: answer each CallSid only once.
  try {
    if ((await redis.set(`axella:sid:${sid}`, 1, { nx: true, ex: 6 * 3600 })) !== 'OK') return busy(res)
  } catch { /* Redis down – the nonce below fails too, so the relay refuses; nothing to gain from a replay */ }

  const from = params.From || ''
  const cfg = await getConfig()
  const who = lookup(cfg, from)
  const hour = Math.floor(Date.now() / 3_600_000)
  const d = digits(from) || 'anonymous'
  // VIPs aren't held to the per-number limit (caller ID can be faked, so the overall limit still applies to them)
  if (!(await under(`axella:rate:all:${hour}`, LIMITS.allHour))
    || (who?.tier !== 'VIP' && !(await under(`axella:rate:${d}:${hour}`, LIMITS.perNumberHour)))) {
    console.log('rate-limited', d === 'anonymous' ? 'anonymous' : 'number')   // never the number itself
    return busy(res)
  }

  if (who?.private) {
    const danishP = from.startsWith('+45') || !from.startsWith('+')
    const msg = privateMessages(cfg.owner)
    try {
      const id = 'call-' + sid
      await redis.set(K.call(id), {
        id, started: new Date().toISOString(), duration_s: null, private: true,
        from: { name: who.first || null, said_name: null, caller_id_match: true, number: from || null, tier: who.tier || null },
        context: null, priority: 'p3', urgent_claimed: false, urgent_capped: false, summary: '', callback: null, sms_draft: null,
        lang: danishP ? 'da' : 'en', transcript: [],
      }, { ex: RECORD_TTL })
      await redis.sadd(K.calls, id)
    } catch { /* Redis down: the caller still hears the message */ }
    return xml(res, danishP
      ? `<Say language="da-DK" voice="Polly.Naja">${esc(msg.da)}</Say><Hangup/>`
      : `<Say language="en-US" voice="Polly.Joanna">${esc(msg.en)}</Say><Hangup/>`)
  }

  // One-time ticket for the WebSocket: the relay only accepts a session whose ticket matches this CallSid.
  const nonce = crypto.randomBytes(16).toString('hex')
  try { await redis.set(`axella:nonce:${nonce}`, sid, { ex: 120 }) } catch { return busy(res) }

  const danish = from.startsWith('+45') || !from.startsWith('+')
  const host = (req.headers['x-forwarded-host'] as string) || req.headers.host
  const greeting = danish ? cfg.greeting_da : cfg.greeting_en
  xml(res, `
  <Connect>
    <ConversationRelay url="wss://${host}/api/relay" welcomeGreeting="${esc(greeting)}"
      language="${danish ? 'da-DK' : 'en-US'}" ttsProvider="ElevenLabs" voice="${danish ? VOICE_DA : VOICE_EN}" transcriptionProvider="Google"
      interruptible="speech" dtmfDetection="false">
      <Language code="da-DK" ttsProvider="ElevenLabs" voice="${VOICE_DA}" transcriptionProvider="Google"/>
      <Language code="en-US" ttsProvider="ElevenLabs" voice="${VOICE_EN}" transcriptionProvider="Google"/>
      <Parameter name="known" value="${esc(who ? who.first : '')}"/>
      <Parameter name="tier" value="${esc(who ? who.tier : '')}"/>
      <Parameter name="ticket" value="${nonce}"/>
    </ConversationRelay>
  </Connect>
`)
}
