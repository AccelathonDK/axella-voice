// POST /api/config – the Mac mini pushes greetings and the contact list. Kept in Redis only; contacts expire after 7 days.
// Preferred contact form: {h, first, tier} – h = HMAC-SHA256(VOICE_LOOKUP_KEY, E.164 number), first name only, so no
// full names or numbers leave the Mac. The plain form {name, number, tier} is still accepted from older Mac minis.
import type { IncomingMessage, ServerResponse } from 'node:http'
import { CONTACTS_TTL, DEFAULT_CONFIG, defaultGreetings, macMiniOk, readBody, redis } from '../lib/util.js'

const str = (v: unknown, max: number) => (typeof v === 'string' || typeof v === 'number' ? String(v) : '').trim().slice(0, max)

export default async function handler(req: IncomingMessage, res: ServerResponse) {
  res.setHeader('Content-Type', 'application/json'); res.setHeader('Cache-Control', 'no-store')
  if (!macMiniOk(req)) { res.statusCode = 401; return res.end('{"ok":false}') }
  if (req.method !== 'POST') { res.statusCode = 405; return res.end('{"ok":false}') }
  let p: any
  try { p = JSON.parse(await readBody(req, 500_000)) } catch { res.statusCode = 400; return res.end('{"ok":false}') }
  if (!p || typeof p !== 'object') { res.statusCode = 400; return res.end('{"ok":false}') }
  const owner = str(p.owner, 80) || DEFAULT_CONFIG.owner
  const contacts = (Array.isArray(p.contacts) ? p.contacts : []).slice(0, 3000).map((c: any) => {
    const tier = str(c?.tier, 10) === 'VIP' ? 'VIP' : 'Known'
    const h = str(c?.h, 64)
    if (/^[0-9a-f]{64}$/.test(h)) return { h, first: str(c.first, 40), tier }
    const name = str(c?.name, 80), number = str(c?.number, 30)
    return name && number ? { name, number, tier } : null
  }).filter(Boolean)
  const cfg = {
    owner,
    sign_off: str(p.sign_off, 60) || undefined,
    greeting_da: str(p.greeting_da, 500) || defaultGreetings(owner).greeting_da,
    greeting_en: str(p.greeting_en, 500) || defaultGreetings(owner).greeting_en,
  }
  await redis.set('axella:config', cfg)
  await redis.set('axella:contacts', contacts, { ex: CONTACTS_TTL })
  // the greetings callers actually hear go back to the Mac mini, so the dashboard shows the real ones
  res.end(JSON.stringify({ ok: true, contacts: contacts.length, hashed: contacts.filter((c: any) => c.h).length,
    lookup_key: (process.env.VOICE_LOOKUP_KEY || '').length >= 32, greeting_da: cfg.greeting_da, greeting_en: cfg.greeting_en }))
}
