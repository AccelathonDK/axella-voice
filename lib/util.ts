// Shared helpers: Twilio signature check, Redis, config, auth for the Mac mini, call-record checks.
import crypto from 'node:crypto'
import type { IncomingMessage } from 'node:http'
import { Redis } from '@upstash/redis'

export const env = (k: string, fallback?: string) => {
  const v = process.env[k] ?? fallback
  if (v === undefined) throw new Error(`missing env ${k}`)
  return v
}
const num = (k: string, d: number) => { const n = Number(process.env[k]); return Number.isFinite(n) && n > 0 ? n : d }

// Vercel Marketplace Redis (Upstash) sets KV_REST_API_* or UPSTASH_REDIS_REST_*
export const redis = new Redis({
  url: process.env.KV_REST_API_URL || process.env.UPSTASH_REDIS_REST_URL || '',
  token: process.env.KV_REST_API_TOKEN || process.env.UPSTASH_REDIS_REST_TOKEN || '',
})

export const DAY = 86400
export const RECORD_TTL = 7 * DAY            // every call / partial record expires on its own after 7 days
export const CONTACTS_TTL = 7 * DAY          // contacts only; the Mac mini re-pushes every 6 h. Owner + greetings
                                             // don't expire, so a long Mac mini outage never changes what callers hear
export const LIMITS = {
  perNumberHour: num('RATE_PER_NUMBER_HOUR', 3),   // calls answered per caller number per hour
  allHour: num('RATE_ALL_HOUR', 30),               // calls answered per hour in total
  idleNudgeMs: num('IDLE_NUDGE_MS', 20_000),       // silence after Axella spoke → one "are you still there?"
  idleEndMs: num('IDLE_END_MS', 45_000),           // silence → goodbye and hang up (counted from the same point)
  maxCallMs: 4 * 60_000,                           // Vercel closes functions at 5 min – wind down before that
}

/** Twilio's X-Twilio-Signature: base64(HMAC-SHA1(authToken, url + sorted key+value of POST params)). */
export function twilioSignatureOk(url: string, params: Record<string, string>, signature: string | undefined) {
  if (!signature) return false
  const data = url + Object.keys(params).sort().map(k => k + params[k]).join('')
  const expected = crypto.createHmac('sha1', env('TWILIO_AUTH_TOKEN')).update(data, 'utf8').digest('base64')
  const a = Buffer.from(expected), b = Buffer.from(signature)
  return a.length === b.length && crypto.timingSafeEqual(a, b)
}

/** The public URL Twilio used (Vercel sits behind a proxy). */
export function publicUrl(req: IncomingMessage, protocol = 'https') {
  const host = (req.headers['x-forwarded-host'] as string) || req.headers.host
  return `${protocol}://${host}${req.url}`
}

/** Only the Mac mini may fetch calls or push config: bearer token in AXELLA_SYNC_TOKEN. */
export function macMiniOk(req: IncomingMessage) {
  const want = env('AXELLA_SYNC_TOKEN')
  const got = (req.headers.authorization || '').replace(/^Bearer\s+/i, '')
  const a = Buffer.from(want), b = Buffer.from(got)
  return want.length >= 32 && a.length === b.length && crypto.timingSafeEqual(a, b)
}

export async function readBody(req: IncomingMessage, max = 200_000): Promise<string> {
  let s = ''
  for await (const c of req) { s += c; if (s.length > max) throw new Error('body too large') }
  return s
}

/** A contact as pushed by the Mac mini. Hashed form (preferred): h = HMAC-SHA256(VOICE_LOOKUP_KEY, E.164 number),
 *  first = first name only. Plain form (older Mac mini, or no lookup key yet): name + number. */
export interface Contact { h?: string; first?: string; name?: string; number?: string; tier: 'VIP' | 'Known' | string; private?: boolean }
export interface Config {
  greeting_da: string; greeting_en: string
  contacts: Contact[]          // pushed by the Mac mini from Contacts/contacts.md – never committed here
  owner: string                // pushed by the Mac mini (actions-config.json "owner"); OWNER_NAME env var as fallback
  sign_off?: string
}
export const firstName = (owner: string) => owner.trim().split(/\s+/)[0] || owner
export function defaultGreetings(owner: string) {
  const f = firstName(owner)
  return {
    greeting_da: `Hej, du har ringet til ${owner}. Jeg er Axella, ${f}s AI-assistent – ${f} kan ikke tage telefonen lige nu. Opkaldet bliver opsummeret til ${f}. Hvad drejer det sig om? You are also welcome to speak English.`,
    greeting_en: `Hi, you've reached ${owner}. I'm Axella, ${f}'s AI assistant – ${f} can't take your call right now. This call is summarised for ${f}. How can I help?`,
  }
}
const OWNER = process.env.OWNER_NAME || 'the person you called'
/** What a private caller hears (fixed text – no AI, no transcription, no summary). */
export function privateMessages(owner: string) {
  const f = firstName(owner)
  return {
    da: `Hej, du har ringet til ${owner}. ${f} kan ikke tage telefonen lige nu og ringer tilbage så hurtigt som muligt. Tak.`,
    en: `Hi, you've reached ${owner}. ${f} can't take your call right now and will call you back as soon as possible. Thank you.`,
  }
}
export const DEFAULT_CONFIG: Config = { owner: OWNER, ...defaultGreetings(OWNER), contacts: [] }
export async function getConfig(): Promise<Config> {
  try {
    const [c0, list] = await redis.mget<[Partial<Config> | null, Contact[] | null]>('axella:config', 'axella:contacts')
    const c = c0 || {}
    const owner = c.owner || OWNER
    // contacts live in their own key with an expiry; older configs kept them inside axella:config
    const contacts = Array.isArray(list) ? list : Array.isArray(c.contacts) ? c.contacts : []
    return { ...DEFAULT_CONFIG, ...defaultGreetings(owner), ...c, owner, contacts }
  } catch { return DEFAULT_CONFIG }
}

export const digits = (n?: string | null) => (n || '').replace(/[^\d]/g, '')
/** E.164 as Twilio sends it ("+4500000000"); null for anonymous / short / odd caller IDs. */
export const e164 = (n?: string | null) => { const d = digits(n); return d.length >= 6 && (n || '').trim().startsWith('+') ? '+' + d : null }
export const numberHash = (e: string, key: string) => crypto.createHmac('sha256', key).update(e, 'utf8').digest('hex')

/** Exact number match only (no suffix matching – a number from another country must not match a Danish contact). */
export function lookup(cfg: Config, from?: string | null): { first: string; tier: string; private: boolean } | null {
  const e = e164(from)
  if (!e) return null
  const key = process.env.VOICE_LOOKUP_KEY || ''
  const h = key.length >= 32 ? numberHash(e, key) : ''
  for (const c of cfg.contacts) {
    const hit = (h && c.h && c.h.length === h.length && crypto.timingSafeEqual(Buffer.from(c.h), Buffer.from(h)))
      || (c.number && e164(c.number) === e)
    if (hit) return { first: (c.first || firstName(c.name || '')).slice(0, 40), tier: String(c.tier || 'Known'), private: c.private === true }
  }
  return null
}

// ---------- call records: everything the summariser or a caller can influence is coerced here ----------
export const P = ['p0', 'p1', 'p2', 'p3'] as const
/** Plain text only: no markdown links/images, no HTML tags, no control characters; capped. */
export function clean(v: unknown, max: number): string | null {
  if (typeof v !== 'string') return null
  const s = v
    .replace(/!?\[([^\]]*)\]\([^)]*\)/g, '$1')        // [text](url) / ![alt](url) → text
    .replace(/<[^>]*>/g, '')                          // tags
    .replace(/[\u0000-\u0008\u000b-\u001f\u007f]/g, ' ')
    .replace(/\s+/g, ' ').trim()
  return s ? s.slice(0, max) : null
}
export const prio = (v: unknown, fallback: typeof P[number] = 'p2') =>
  (P as readonly string[]).includes(String(v).toLowerCase()) ? String(v).toLowerCase() as typeof P[number] : fallback

/** Keys: one key per record with its own TTL, plus an index set so GET /api/calls can list them. */
export const K = {
  call: (id: string) => `axella:call:${id}`, calls: 'axella:calls:index',
  partial: (id: string) => `axella:partial:${id}`, partials: 'axella:partial:index',
  legacyCalls: 'axella:calls', legacyPartial: 'axella:partial',   // hashes used before Nov 2026 – read, then emptied
}
