// Fake Upstash REST + fake Anthropic (SSE) for local tests.
import http from 'node:http'
export const store = new Map<string, any>()   // string values or Set or Map(hash)
export const log: any[] = []
export let anthropicScript: ((body: any) => { text?: string; status?: number; delayMs?: number }) = () => ({ text: '[DA] Hej.' })
export const setScript = (f: typeof anthropicScript) => { anthropicScript = f }
export const anthropicCalls: any[] = []

function cmd(c: any[]): any {
  const [op, ...a] = c.map(String); const k = a[0]
  switch (op.toUpperCase()) {
    case 'GET': return typeof store.get(k) === 'string' ? store.get(k) : null
    case 'SET': { const nx = a.map(x => x.toLowerCase()).includes('nx'); if (nx && store.has(k)) return null; store.set(k, a[1]); return 'OK' }
    case 'GETDEL': { const v = store.get(k); store.delete(k); return typeof v === 'string' ? v : null }
    case 'DEL': { let n = 0; for (const x of a) if (store.delete(x)) n++; return n }
    case 'INCR': { const v = Number(store.get(k) || 0) + 1; store.set(k, String(v)); return v }
    case 'EXPIRE': return 1
    case 'SADD': { const s = store.get(k) instanceof Set ? store.get(k) : new Set(); a.slice(1).forEach(x => s.add(x)); store.set(k, s); return 1 }
    case 'SREM': { const s = store.get(k); if (s instanceof Set) a.slice(1).forEach(x => s.delete(x)); return 1 }
    case 'SMEMBERS': { const s = store.get(k); return s instanceof Set ? [...s] : [] }
    case 'MGET': return a.map(x => typeof store.get(x) === 'string' ? store.get(x) : null)
    case 'HGETALL': { const h = store.get(k); if (!(h instanceof Map)) return []; return [...h].flat() }
    case 'HDEL': { const h = store.get(k); if (h instanceof Map) a.slice(1).forEach(x => h.delete(x)); return 1 }
    case 'HSET': { const h = store.get(k) instanceof Map ? store.get(k) : new Map(); for (let i = 1; i < a.length; i += 2) h.set(a[i], a[i + 1]); store.set(k, h); return 1 }
    default: throw new Error('unmocked ' + op)
  }
}
export function startRedis(port: number) {
  return new Promise<void>(r => http.createServer(async (req, res) => {
    let b = ''; for await (const c of req) b += c
    const body = JSON.parse(b); log.push(body)
    res.setHeader('content-type', 'application/json')
    if (req.url?.startsWith('/pipeline') || req.url?.startsWith('/multi-exec')) return res.end(JSON.stringify(body.map((c: any) => ({ result: cmd(c) }))))
    try { res.end(JSON.stringify({ result: cmd(body) })) } catch (e: any) { res.statusCode = 400; res.end(JSON.stringify({ error: e.message })) }
  }).listen(port, r))
}
export function startAnthropic(port: number) {
  return new Promise<void>(r => http.createServer(async (req, res) => {
    let b = ''; for await (const c of req) b += c
    const body = JSON.parse(b); anthropicCalls.push(body)
    const sc = anthropicScript(body)
    if (sc.delayMs) await new Promise(x => setTimeout(x, sc.delayMs))
    if (sc.status) { res.statusCode = sc.status; res.setHeader('content-type', 'application/json'); return res.end(JSON.stringify({ type: 'error', error: { type: 'api_error', message: 'x' } })) }
    const text = sc.text || ''
    const msg = { id: 'msg_1', type: 'message', role: 'assistant', model: body.model, content: [] as any[], stop_reason: null, stop_sequence: null, usage: { input_tokens: 1, output_tokens: 1 } }
    if (!body.stream) { res.setHeader('content-type', 'application/json'); return res.end(JSON.stringify({ ...msg, content: [{ type: 'text', text }], stop_reason: 'end_turn' })) }
    res.setHeader('content-type', 'text/event-stream')
    const ev = (t: string, d: any) => res.write(`event: ${t}\ndata: ${JSON.stringify({ type: t, ...d })}\n\n`)
    ev('message_start', { message: msg })
    ev('content_block_start', { index: 0, content_block: { type: 'text', text: '' } })
    for (const ch of text.match(/.{1,3}/gs) || []) { ev('content_block_delta', { index: 0, delta: { type: 'text_delta', text: ch } }); await new Promise(x => setTimeout(x, 5)) }
    ev('content_block_stop', { index: 0 })
    ev('message_delta', { delta: { stop_reason: 'end_turn', stop_sequence: null }, usage: { output_tokens: 1 } })
    ev('message_stop', {}); res.end()
  }).listen(port, r))
}
