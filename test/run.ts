import crypto from 'node:crypto'
import http from 'node:http'
import WebSocket from 'ws'
import * as M from './mocks.js'
Object.assign(process.env, { KV_REST_API_URL: 'http://127.0.0.1:7001', KV_REST_API_TOKEN: 't', ANTHROPIC_BASE_URL: 'http://127.0.0.1:7002',
  ANTHROPIC_API_KEY: 'k', TWILIO_AUTH_TOKEN: 'twtok', AXELLA_SYNC_TOKEN: 'a'.repeat(64), VOICE_LOOKUP_KEY: 'L'.repeat(40),
  IDLE_NUDGE_MS: '1000', IDLE_END_MS: '2000', OWNER_NAME: 'Alex Example', RATE_PER_NUMBER_HOUR: '3' })
let fails = 0
const ok = (c: any, m: string) => { console.log((c ? 'PASS ' : 'FAIL ') + m); if (!c) fails++ }
const sleep = (ms: number) => new Promise(r => setTimeout(r, ms))
const sig = (url: string, p: Record<string, string>) => crypto.createHmac('sha1', 'twtok').update(url + Object.keys(p).sort().map(k => k + p[k]).join('')).digest('base64')
const sid = () => 'CA' + crypto.randomBytes(16).toString('hex')

await M.startRedis(7001); await M.startAnthropic(7002)
const util = await import('../lib/util.js')
const voice = (await import('../api/voice.js')).default
const calls = (await import('../api/calls.js')).default
const config = (await import('../api/config.js')).default
const relay = (await import('../api/relay.js')).default
const H = 'test.local'
const mk = (h: any, port: number) => new Promise<void>(r => http.createServer(h).listen(port, r))
await mk(voice, 7101); await mk(calls, 7102); await mk(config, 7103)
await new Promise<void>(r => relay.listen(7104, r))
const req = (port: number, path: string, o: { method?: string; headers?: any; body?: string } = {}) => new Promise<{ s: number; b: string }>(r => {
  const q = http.request({ port, path, method: o.method || 'GET', headers: { host: H, ...(o.headers || {}) } }, res => { let b = ''; res.on('data', c => b += c); res.on('end', () => r({ s: res.statusCode!, b })) })
  q.end(o.body)
})
const auth = { authorization: 'Bearer ' + 'a'.repeat(64) }

// ---- config: hashed + plain contacts
const hVip = util.numberHash('+4511111111', 'L'.repeat(40))
let r = await req(7103, '/api/config', { method: 'POST', headers: auth, body: JSON.stringify({ owner: 'Alex Example', greeting_da: 'Hej, du har ringet til Alex.', greeting_en: 'Hi.', contacts: [
  { h: hVip, first: 'Jamie', tier: 'VIP' }, { name: 'Sam Rivera', number: '+4570000002', tier: 'Known' }, { h: 'zz', first: 'bad' }] }) })
ok(r.s === 200 && JSON.parse(r.b).contacts === 2 && JSON.parse(r.b).hashed === 1, 'config accepts hashed + plain, drops bad')
ok((await req(7103, '/api/config', { method: 'POST', headers: auth, body: '{bad' })).s === 400, 'config bad JSON → 400')
ok((await req(7103, '/api/config', { method: 'POST', body: '{}' })).s === 401, 'config no token → 401')
const cfg = await util.getConfig()
ok(util.lookup(cfg, '+4511111111')?.first === 'Jamie', 'hashed lookup')
ok(M.store.has('axella:contacts') && !JSON.parse(M.store.get('axella:config')).contacts, 'contacts in their own key, config without them')
ok(util.lookup(cfg, '+4570000002')?.first === 'Sam', 'plain lookup')
ok(util.lookup(cfg, '+4622222222') === null, 'other country, same 8 digits → no match')

// ---- voice webhook
const call = async (from: string, s = sid(), sign = true) => {
  const p = { CallSid: s, From: from, To: '+4500000000' }
  return req(7101, '/api/voice', { method: 'POST', headers: { 'content-type': 'application/x-www-form-urlencoded', ...(sign ? { 'x-twilio-signature': sig(`https://${H}/api/voice`, p) } : {}) }, body: new URLSearchParams(p).toString() })
}
ok((await call('+4570000003', sid(), false)).s === 403, 'voice unsigned → 403')
const s1 = sid(); r = await call('+4511111111', s1)
const ticket = r.b.match(/name="ticket" value="([0-9a-f]{32})"/)?.[1]
ok(r.s === 200 && ticket && /name="known" value="Jamie"/.test(r.b) && /value="VIP"/.test(r.b), 'voice signed → TwiML with ticket, known first name')
ok(/Reject/.test((await call('+4511111111', s1)).b), 'replayed CallSid → Reject')
for (let i = 0; i < 3; i++) ok(/ConversationRelay/.test((await call('+4570000004')).b), `call ${i + 1} from one number answered`)
ok(/Reject reason="busy"/.test((await call('+4570000004')).b), '4th call in the hour from one number → busy')
for (let i = 0; i < 4; i++) await call('+4511111111')
ok(/ConversationRelay/.test((await call('+4511111111')).b), 'VIP not held to per-number limit')

// ---- relay helpers
const wsUrl = `wss://${H}/api/relay`
function open(sign = true) {
  const ws = new WebSocket('ws://127.0.0.1:7104/api/relay', { headers: { host: H, ...(sign ? { 'x-twilio-signature': sig(wsUrl, {}) } : {}) } })
  const got: any[] = []; let closed = false
  ws.on('message', d => got.push(JSON.parse(String(d)))); ws.on('close', () => { closed = true }); ws.on('error', () => { closed = true })
  return { ws, got, isClosed: () => closed, ready: new Promise(r => { ws.on('open', r); ws.on('error', r); ws.on('unexpected-response', (_q, res) => { closed = true; r(res.statusCode) }) }) }
}
const said = (got: any[]) => got.filter(g => g.type === 'text').map(g => g.token).join('')

{ const c = open(false); const st = await c.ready; ok(st === 403, 'relay unsigned upgrade → 403') }
{ // bad ticket → closed, nothing saved
  const c = open(); await c.ready
  const s = sid(); c.ws.send(JSON.stringify({ type: 'setup', callSid: s, from: '+4570000005', customParameters: { ticket: 'f'.repeat(32) } }))
  await sleep(300); ok(c.isClosed(), 'relay bad ticket → socket closed')
}
async function session(from: string) {
  const s = sid(); const r = await call(from, s); const t = r.b.match(/name="ticket" value="([0-9a-f]{32})"/)![1]
  const known = r.b.match(/name="known" value="([^"]*)"/)![1], tier = r.b.match(/name="tier" value="([^"]*)"/)![1]
  const c = open(); await c.ready
  c.ws.send(JSON.stringify({ type: 'setup', callSid: s, from, customParameters: { ticket: t, known, tier } }))
  await sleep(200)
  return { ...c, sid: s }
}
const rec = async (s: string) => { const v = M.store.get('axella:call:call-' + s); return v ? JSON.parse(v) : null }

// 1) normal message, [END], validation of the summary
M.setScript(b => b.stream
  ? { text: b.messages.length > 1 ? '[DA] Tak, jeg giver beskeden videre. Hav en god dag. [EN' + 'D]' : '[DA] Hej, hvem taler jeg med?' }
  : { text: JSON.stringify({ caller_name: 'Sam Rivera', priority: 'URGENT', summary: '**Call back** ![x](https://evil.example/p.png) <b>now</b>', callback: { by: 'tomorrow', number: '+45 00 00 00 99' }, sms_draft: 'Hej Sam [link](https://x.example) br Alex', context: 42 }) })
{
  const c = await session('+4511111111')
  c.ws.send(JSON.stringify({ type: 'prompt', voicePrompt: 'Hej det er Sam', lang: 'da-DK' })); await sleep(400)
  ok(said(c.got).includes('hvem taler') && !said(c.got).includes('[DA]'), 'reply spoken, tag stripped')
  const sys = M.anthropicCalls.at(-1).system as string
  ok(/do NOT say\s+that name before/.test(sys), 'prompt: known name not to be said first')
  c.ws.send(JSON.stringify({ type: 'prompt', voicePrompt: 'ring mig i morgen', lang: 'da-DK' })); await sleep(300)
  ok(!said(c.got).includes('[END'), '[END] never spoken')
  await sleep(6500); ok(c.got.some(g => g.type === 'end'), 'end sent after goodbye')
  c.ws.send(JSON.stringify({ type: 'prompt', voicePrompt: 'en ting mere', lang: 'da-DK' })); const n = M.anthropicCalls.length; await sleep(300)
  ok(M.anthropicCalls.length === n, 'nothing answered after the goodbye')
  c.ws.close(); await sleep(500)
  const x = await rec(c.sid)
  ok(x && x.priority === 'p2', 'priority coerced (URGENT → p2)')
  ok(x && !/https|<b>|!\[/.test(x.summary) && /\*\*Call back\*\*/.test(x.summary), 'summary: no image/link/HTML, bold kept')
  ok(x && x.context === null && x.from.said_name === 'Sam Rivera' && x.from.caller_id_match && x.from.name === 'Jamie', 'from: contact first name, said name, match flag')
  ok(x && x.callback?.other_number === true && x.callback.by === 'tomorrow', 'callback on another number flagged')
  ok(x && !/https/.test(x.sms_draft), 'sms draft without links')
  ok(!M.store.get('axella:partial:call-' + c.sid), 'partial removed after save')
  r = await req(7102, '/api/calls', { headers: auth }); const list = JSON.parse(r.b).calls
  ok(list.some((k: any) => k.id === 'call-' + c.sid), 'GET /api/calls lists it')
  ok((await req(7102, '/api/calls?id=../x', { method: 'DELETE', headers: auth })).s === 400, 'DELETE bad id → 400')
  await req(7102, '/api/calls?id=call-' + c.sid, { method: 'DELETE', headers: auth })
  ok(!JSON.parse((await req(7102, '/api/calls', { headers: auth })).b).calls.some((k: any) => k.id === 'call-' + c.sid), 'DELETE removes it')
}
// 2) silence: nudge, then goodbye + end
{
  const c = await session('+4570000006')
  await sleep(1500); ok(!said(c.got).includes('Er du der stadig'), 'no nudge while the greeting is spoken')
  await sleep(4500); ok(said(c.got).includes('Er du der stadig'), 'silence → nudge')
  await sleep(4500 + 1000 + 300); ok(said(c.got).includes('kan ikke høre dig'), 'more silence → goodbye')
  await sleep(9500); ok(c.got.some(g => g.type === 'end'), 'silence → end')
  c.ws.close(); await sleep(400)
  const x = await rec(c.sid); ok(x && x.priority === 'p3' && /Hung up/.test(x.summary), 'silent call saved as p3 no message')
}
// 3) Anthropic failing → sorry, then technical-problems goodbye
M.setScript(b => b.stream ? { status: 500 } : { text: '{}' })
{
  const c = await session('+4577777777')
  c.ws.send(JSON.stringify({ type: 'prompt', voicePrompt: 'hallo er der nogen', lang: 'da-DK' })); await sleep(2500)
  ok(said(c.got).includes('Undskyld, jeg hørte ikke'), 'first failure → sorry')
  c.ws.send(JSON.stringify({ type: 'prompt', voicePrompt: 'hallo igen', lang: 'da-DK' })); await sleep(2500)
  ok(said(c.got).includes('tekniske problemer'), 'second failure → goodbye'); await sleep(9000)
  ok(c.got.some(g => g.type === 'end'), 'failures → end'); c.ws.close(); await sleep(300)
}
// 4) interrupt: transcript keeps only what was heard
M.setScript(b => b.stream ? { text: '[DA] ' + 'Det lyder spændende, fortæl mig mere om det hele. '.repeat(6) } : { text: '{"summary":"x","priority":"p3"}' })
{
  const c = await session('+4588888888')
  c.ws.send(JSON.stringify({ type: 'prompt', voicePrompt: 'jeg ringer om noget', lang: 'da-DK' })); await sleep(60)
  c.ws.send(JSON.stringify({ type: 'interrupt', utteranceUntilInterrupt: 'Det lyder', durationUntilInterruptMs: 500 }))
  c.ws.send(JSON.stringify({ type: 'prompt', voicePrompt: 'undskyld, det er om fakturaen', lang: 'da-DK' })); await sleep(1500)
  const m = M.anthropicCalls.at(-1).messages
  ok(m[1]?.role === 'assistant' && m[1].content === 'Det lyder …' && m[2]?.role === 'user', 'interrupted reply stored as heard, order user/assistant/user')
  c.ws.close(); await sleep(600)
  const x = await rec(c.sid); ok(x && x.priority === 'p3', 'summariser p3 without callback stays p3')
}
// 5) p0 cap: only one urgent per number per day
M.setScript(b => b.stream ? { text: '[DA] Ring 112. [END]' } : { text: '{"summary":"emergency","priority":"p0"}' })
for (const i of [1, 2]) {
  const c = await session('+4599999999')
  c.ws.send(JSON.stringify({ type: 'prompt', voicePrompt: 'det er en nødsituation', lang: 'da-DK' })); await sleep(300)
  c.ws.close(); await sleep(500)
  const x = await rec(c.sid); ok(x && x.priority === (i === 1 ? 'p0' : 'p1') && x.urgent_claimed, `p0 #${i} → ${i === 1 ? 'p0' : 'p1 (capped)'}`)
}
console.log(fails ? `\n${fails} FAILED` : '\nALL PASSED'); process.exit(fails ? 1 : 0)
