// GET /api/calls – finished calls for the Mac mini. DELETE /api/calls?id=… once it has saved one.
import type { IncomingMessage, ServerResponse } from 'node:http'
import { K, macMiniOk, redis } from '../lib/util.js'

const ID = /^call-[A-Za-z0-9_-]{1,80}$/
const MAX = 50                                          // per fetch; the rest comes a minute later

const parse = (v: unknown): any => {
  if (v && typeof v === 'object') return v
  if (typeof v !== 'string') return null
  try { return JSON.parse(v) } catch { return null }
}

/** Records under per-call keys (index set). Ids whose key has expired are dropped from the index. */
async function fromIndex(index: string, key: (id: string) => string) {
  const ids = ((await redis.smembers(index)) || []).filter(id => ID.test(id)).slice(0, MAX)
  if (!ids.length) return []
  const vals = await redis.mget<unknown[]>(...ids.map(key))
  const gone = ids.filter((_, i) => vals[i] == null)
  if (gone.length) await redis.srem(index, ...gone as [string, ...string[]])
  return vals.map(parse).filter(Boolean)
}
/** Records still in the hashes used before per-call keys (read until empty). */
async function fromLegacy(hash: string) {
  const all = (await redis.hgetall<Record<string, unknown>>(hash)) || {}
  return Object.values(all).map(parse).filter(Boolean)
}

export default async function handler(req: IncomingMessage, res: ServerResponse) {
  res.setHeader('Content-Type', 'application/json'); res.setHeader('Cache-Control', 'no-store')
  if (!macMiniOk(req)) { res.statusCode = 401; return res.end('{"ok":false}') }
  const url = new URL(req.url || '/', 'http://x')
  if (req.method === 'GET') {
    const calls: any[] = [...await fromIndex(K.calls, K.call), ...await fromLegacy(K.legacyCalls)].filter(c => ID.test(String(c.id)))
    // calls whose final summary was never written (function stopped early): hand over the transcript after 10 min
    const partial = [...await fromIndex(K.partials, K.partial), ...await fromLegacy(K.legacyPartial)]
    for (const p of partial) {
      if (!ID.test(String(p.id)) || calls.some(c => c.id === p.id) || Date.now() - (Number(p.updated) || 0) < 10 * 60_000) continue
      const spoke = (Array.isArray(p.transcript) ? p.transcript : []).some((t: any) => t?.who === 'Caller')
      calls.push({ ...p, duration_s: null, priority: spoke ? 'p2' : 'p3', context: null, callback: null, sms_draft: null,
        summary: spoke ? 'Message taken, but the summary failed – see the transcript.' : 'Hung up without leaving a message.' })
    }
    return res.end(JSON.stringify({ ok: true, calls: calls.slice(0, MAX) }))
  }
  if (req.method === 'DELETE') {
    const id = url.searchParams.get('id') || ''
    if (!ID.test(id)) { res.statusCode = 400; return res.end('{"ok":false}') }
    await redis.del(K.call(id), K.partial(id))
    await redis.srem(K.calls, id); await redis.srem(K.partials, id)
    await redis.hdel(K.legacyCalls, id); await redis.hdel(K.legacyPartial, id)
    return res.end(JSON.stringify({ ok: true }))
  }
  res.statusCode = 405; res.end('{"ok":false}')
}
