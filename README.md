# follow-api

Small Express service that answers one question for Roblox game servers:
**does `userId` follow `followId`?**

Roblox locked the followings/followers list endpoints behind login auth
(~May 2026, HTTP 401 without a cookie), and proxies like RoProxy strip
cookies — so this service calls `friends.roblox.com` directly with a
throwaway alt's `.ROBLOSECURITY` cookie plus auto-refreshed `x-csrf-token`.

## Endpoints

| Method | Path | Auth | Description |
| ------ | ---- | ---- | ----------- |
| GET | `/follows?userId=123&followId=456` | `x-api-key: <API_SECRET>` | `{ ok: true, follows: bool }`. May include `stale: true` (served from cache while revalidating). |
| GET | `/health` | none | `{ ok: true, upstream: "cookie"/"missing-cookie", breaker }` |
| GET | `/metrics` | `x-api-key` | Counters (hits, misses, stale, coalesced, breaker, `csrf_refreshes`, `cookie_auth_fail`), cache size, queue depth. |

`GET /follows` error shapes (all with `ok: false`, never cached):

| Status | `error` | Meaning |
| ------ | ------- | ------- |
| 400 | — | `userId`/`followId` not numeric |
| 403 | — | wrong/missing `x-api-key` |
| 429 | `rate_limited` | per-IP rate limit, retry after `Retry-After` |
| 502 | `cookie_missing` / `cookie_invalid` | secret not set / alt cookie dead → rotate it |
| 502 | `upstream_rate_limited` | Roblox throttled the alt |
| 503 | `overloaded` / `circuit_open` | load shed or breaker open, retry after 2s |
| 504 | `upstream_timeout` | friends API too slow |

## How it handles load

- **Bounded LRU+TTL cache** (`CACHE_MAX`, 10 min fresh / 30 min hot / +30 min stale) with stale-while-revalidate: stale hits return instantly and refresh in background.
- **Singleflight by `userId`**: concurrent checks for the same player share one upstream fetch.
- **Bounded upstream concurrency** (`UPSTREAM_MAX_CONCURRENT=20`, queue cap 200, then `503`).
- **Circuit breaker** on upstream 429/5xx/timeouts/invalid-cookie.
- **Cluster mode**: one worker per CPU, auto-restart. `CLUSTER=0` to disable.
- **Per-IP rate limit**: 1000/min default (Roblox servers share egress IPs).

## Environment variables

| Var | Required | Default | Notes |
| --- | -------- | ------- | ----- |
| `API_SECRET` | yes | — | Value of the `x-api-key` header. Without it every `/follows` call 403s. |
| `ROBLOX_COOKIE` | yes | — | Throwaway **alt** `.ROBLOSECURITY` value, raw, no quotes. Secret — never commit. |
| `PORT` | no | `3000` | Render injects its own. |
| `FRIENDS_BASE_URL` | no | `https://friends.roblox.com` | Override for testing only. |
| `CACHE_TTL_MS` / `HOT_TTL_MS` / `STALE_MS` / `CACHE_MAX` | no | `600000` / `1800000` / `1800000` / `20000` | Cache sizing. |
| `UPSTREAM_TIMEOUT_MS` / `UPSTREAM_MAX_PAGES` / `UPSTREAM_MAX_CONCURRENT` / `UPSTREAM_MAX_QUEUE` | no | `8000` / `10` / `20` / `200` | Upstream guardrails. |
| `RATE_LIMIT_MAX` / `RATE_LIMIT_WINDOW_MS` | no | `1000` / `60000` | Per-IP, per-worker. |
| `BREAKER_THRESHOLD` / `BREAKER_WINDOW_MS` / `BREAKER_OPEN_MS` | no | `10` / `30000` / `30000` | Circuit breaker. |
| `CLUSTER` | no | on | Set `0` to run single-process (dev). |

## Run locally

```cmd
npm install
set API_SECRET=test123
set ROBLOX_COOKIE=paste-alt-cookie-here
set CLUSTER=0
node node.js
```

Smoke test:

```cmd
curl http://localhost:3000/health
curl -H "x-api-key: test123" "http://localhost:3000/follows?userId=340352163&followId=1255467886"
```

## Deploy (Render)

1. Push this repo, create a Web Service (`npm start` / `node node.js`).
2. Environment → add **secret** `API_SECRET` and **secret** `ROBLOX_COOKIE`.
3. Deploy, then check `/health` (`"upstream":"cookie"`) and `/metrics`.

## Getting the alt cookie

Desktop browser logged in as the **alt** (never your main): `F12` →
Application → Cookies → `https://www.roblox.com` → copy `.ROBLOSECURITY`
(Chrome/Edge/Opera GX; Firefox uses the Storage tab). Paste raw into
`ROBLOX_COOKIE`. Don't log the alt out afterwards — logout, password
changes, and account switches that end the session kill it
(`cookie_invalid` in logs = fetch a fresh one the same way).

## Roblox Studio usage

Server-side only (`HttpService`, `HttpEnabled` on). Recommended pattern is
the shared `FollowService` ModuleScript (10 min cache, coalescing, 3x
retry with backoff, `nil` = unknown so callers show "try again"):

```lua
local FollowService = require(script.Parent:WaitForChild("FollowService"))
local follows = FollowService.Check(player.UserId, FOLLOW_ID)
if follows == nil then
  return false, "Follow check unavailable, try again" -- transient, keep old state
elseif not follows then
  return false, "Follow @name to claim"
end
```

Notes:

- `userId == followId` (testing your own game) auto-passes — you can't follow yourself.
- Studio budget is 500 external req/min per game server; at ~250–1000 checks/hour this service is idle.
- Never call `*.roblox.com` directly from game servers and never put `.ROBLOSECURITY` in a game script.
