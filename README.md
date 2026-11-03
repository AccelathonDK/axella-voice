# axella-voice

Axella's call agent. When you don't take a call, your phone forwards it to the Twilio number; Twilio
ConversationRelay turns speech into text, this app lets Claude (Haiku) take a message, and the finished call
(summary, priority, callback wish, SMS draft, transcript) waits in Redis until the Mac mini fetches it into
`axella/calls/` – the dashboard's Calls view and push take it from there. **Message-taking only**: no promises,
no calendar, no personal information given out.

## Endpoints (Vercel Functions, region fra1)

| Path | Who calls it | Auth |
|---|---|---|
| `POST /api/voice` | Twilio, "A call comes in" | `X-Twilio-Signature` (HMAC with the auth token); each CallSid answered once |
| `WS /api/relay` | Twilio ConversationRelay | Twilio signature on the upgrade + a one-time ticket from `/api/voice` bound to the CallSid |
| `GET /api/calls`, `DELETE /api/calls?id=` | Mac mini | `Authorization: Bearer $AXELLA_SYNC_TOKEN` |
| `POST /api/config` | Mac mini (greetings + contacts) | same bearer token |

Treat the deployment URL as public – security rests on the signature, the ticket and the token only.

## Data kept in Redis

- `axella:config` – greetings, owner name, and contacts. The Mac mini sends contacts as
  `HMAC-SHA256(VOICE_LOOKUP_KEY, number)` + first name + tier, so no full names or numbers are stored here.
  Contacts are a separate key, `axella:contacts`, that expires after 7 days (the Mac mini re-sends every 6 h); owner
  name and greetings don't expire. A contact who is **Private for Calls** on the Mac mini comes with `"private": true`:
  such a caller hears a fixed message ("… can't take your call right now and will call you back") and the call ends –
  no AI, no transcription, no summary. Their record holds who and when only.
- `axella:call:<id>` / `axella:partial:<id>` – one key per call with its own 7-day expiry, listed in
  `axella:calls:index` / `axella:partial:index`. Deleted as soon as the Mac mini has saved the call.
- Short-lived counters: `axella:sid:*` (6 h), `axella:nonce:*` (2 min), `axella:rate:*` (1 h), `axella:p0:*` (24 h).

## Abuse limits

- Per caller number: 3 answered calls per hour (VIPs exempt), 30 per hour in total. Over the limit the call is
  rejected as busy before it's answered – no media minutes, no TTS, no Claude.
- Silence: one "are you still there?" after 20 s, goodbye after 45 s. Calls are cut at 4 min (Vercel ends functions at 5);
  the summary is still saved.
- Claude: 8 s timeout, one retry; after two failed replies in a row the agent says goodbye.
- One urgent (p0) push per caller number per day; further "urgent" calls from that number are p1.
- Also set a usage alert in Twilio (Console → Monitor → Usage triggers) and keep auto-recharge off.

## Environment variables (Vercel → Project → Settings → Environment Variables)

| Name | Value |
|---|---|
| `ANTHROPIC_API_KEY` | key from the `axella-voice` workspace (spend limit set there) |
| `ANTHROPIC_MODEL` | optional, default `claude-haiku-4-5` – check the model name in the Anthropic Console |
| `TWILIO_AUTH_TOKEN` | Twilio Console → Account → API keys & tokens → Auth token |
| `AXELLA_SYNC_TOKEN` | a random secret of 32+ characters, shared with the Mac mini (`openssl rand -hex 32`) |
| `VOICE_LOOKUP_KEY` | a second random secret (`openssl rand -hex 32`), the same as `state/voice-lookup-key` on the Mac mini |
| `KV_REST_API_URL`, `KV_REST_API_TOKEN` | set automatically when you add Redis (Upstash) from the Vercel Marketplace |
| `OWNER_NAME` | optional fallback until the Mac mini has pushed its config |
| `VOICE_DA`, `VOICE_EN` | optional ElevenLabs voice per language (`<voiceId>-<model>-<speed>_<stability>_<similarity>`) |
| `RATE_PER_NUMBER_HOUR`, `RATE_ALL_HOUR`, `IDLE_NUDGE_MS`, `IDLE_END_MS` | optional, override the limits above |

Node 22 (`engines` in `package.json`; set the same in Vercel → Settings → General → Node.js Version).

## Setup

1. Vercel → Add New → Project → import your `axella-voice` repo → Deploy.
2. Storage / Marketplace → **Upstash Redis** → create (region Frankfurt) → connect to this project.
3. Add the environment variables above → Redeploy.
4. Twilio → Phone Numbers → your number → Voice → "A call comes in": Webhook `https://<project>.vercel.app/api/voice`, HTTP POST.
   Voice geo permissions: block international outgoing calls.
5. Call the number from another phone.

## Development

`npm ci`, `npm run typecheck`, `npm test` – the test runs the four functions against a fake Redis and a fake
Anthropic API (no network, no secrets) and plays through calls: signatures, replay, rate limits, ticket, silence,
failures, barge-in, `[END]`, record validation and the p0 cap.
