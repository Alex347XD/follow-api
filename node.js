import cluster from "node:cluster";
import os from "node:os";
import http from "node:http";
import https from "node:https";
import express from "express";
import fetch from "node-fetch";

// ---------- Config (tune via env) ----------
const PORT = Number(process.env.PORT || 3000);
const SECRET = process.env.API_SECRET;
const USE_CLUSTER = process.env.CLUSTER !== "0"; // set CLUSTER=0 to disable
const CACHE_TTL_MS = Number(process.env.CACHE_TTL_MS || 10 * 60 * 1000); // 10 min fresh
const STALE_MS = Number(process.env.STALE_MS || 30 * 60 * 1000); // serve stale up to 30 min while revalidating
const HOT_HITS = Number(process.env.HOT_HITS || 3); // after N hits, key counts as hot
const HOT_TTL_MS = Number(process.env.HOT_TTL_MS || 30 * 60 * 1000); // hot keys stay fresh longer
const CACHE_MAX = Number(process.env.CACHE_MAX || 20000); // bound memory
const UPSTREAM_TIMEOUT_MS = Number(process.env.UPSTREAM_TIMEOUT_MS || 8000);
const UPSTREAM_MAX_PAGES = Number(process.env.UPSTREAM_MAX_PAGES || 10); // cap: 10 x 100 = 1000 followings max
const UPSTREAM_MAX_CONCURRENT = Number(process.env.UPSTREAM_MAX_CONCURRENT || 20); // semaphore for roproxy
const UPSTREAM_MAX_QUEUE = Number(process.env.UPSTREAM_MAX_QUEUE || 200); // shed load past this
const RATE_LIMIT_WINDOW_MS = Number(process.env.RATE_LIMIT_WINDOW_MS || 60 * 1000);
const RATE_LIMIT_MAX = Number(process.env.RATE_LIMIT_MAX || 1000); // raised: Roblox servers share few egress IPs
const BREAKER_THRESHOLD = Number(process.env.BREAKER_THRESHOLD || 10); // failures to trip
const BREAKER_WINDOW_MS = Number(process.env.BREAKER_WINDOW_MS || 30 * 1000);
const BREAKER_OPEN_MS = Number(process.env.BREAKER_OPEN_MS || 30 * 1000);

// ---------- Cluster: use all CPUs, isolate crashes ----------
if (USE_CLUSTER && cluster.isPrimary) {
  const cpus = Math.max(1, os.availableParallelism?.() ?? os.cpus().length);
  console.log(`Primary ${process.pid} forking ${cpus} workers`);
  for (let i = 0; i < cpus; i++) cluster.fork();
  cluster.on("exit", (worker, code) => {
    console.error(`Worker ${worker.process.pid} died (${code}), restarting`);
    cluster.fork();
  });
} else {
  const app = express();
  app.set("trust proxy", 1);
  app.disable("x-powered-by");

  // Keep-alive agents: reuse sockets to roproxy, avoid socket churn under load
  const httpAgent = new http.Agent({ keepAlive: true, maxSockets: 100, maxFreeSockets: 20 });
  const httpsAgent = new https.Agent({ keepAlive: true, maxSockets: 100, maxFreeSockets: 20 });

  // ---------- Metrics (for /metrics) ----------
  const metrics = {
    total: 0, hit: 0, miss: 0, stale: 0, swr: 0,
    coalesced: 0, overloaded: 0, breaker_rejects: 0,
    upstream_ok: 0, upstream_fail: 0, breaker_opens: 0,
  };

  // ---------- Bounded LRU + TTL cache with hot keys + SWR ----------
  // Map preserves insertion order -> delete+set on hit = LRU. Evict oldest when over CACHE_MAX.
  const CACHE = new Map(); // key -> { time, follows, hits }
  function ttlFor(entry) {
    return entry.hits >= HOT_HITS ? HOT_TTL_MS : CACHE_TTL_MS;
  }
  // Returns { entry, status: 'fresh' | 'stale' } or undefined on full miss
  function cacheGet(key, now) {
    const entry = CACHE.get(key);
    if (!entry) return undefined;
    const ttl = ttlFor(entry);
    if (now - entry.time <= ttl) {
      entry.hits = (entry.hits || 0) + 1;
      CACHE.delete(key);
      CACHE.set(key, entry); // LRU refresh
      return { entry, status: "fresh" };
    }
    if (now - entry.time <= ttl + STALE_MS) {
      return { entry, status: "stale" };
    }
    CACHE.delete(key);
    return undefined;
  }
  function cacheSet(key, follows) {
    const prev = CACHE.get(key);
    const entry = { time: Date.now(), follows, hits: prev?.hits ?? 0 };
    if (CACHE.has(key)) CACHE.delete(key);
    CACHE.set(key, entry);
    if (CACHE.size > CACHE_MAX) {
      const oldest = CACHE.keys().next().value;
      CACHE.delete(oldest);
    }
  }
  // Periodic sweep of fully-expired (fresh + stale) entries so idle keys don't pin memory
  setInterval(() => {
    const now = Date.now();
    let scanned = 0;
    for (const [k, v] of CACHE) {
      if (scanned++ > 2000) break; // bound sweep cost
      if (now - v.time > ttlFor(v) + STALE_MS) CACHE.delete(k);
    }
    const rNow = Date.now();
    for (const [ip, rec] of rateMap) {
      if (rNow > rec.reset) rateMap.delete(ip);
    }
  }, 60 * 1000).unref();

  // ---------- Circuit breaker for RoProxy ----------
  const breaker = { state: "closed", failures: [], openedAt: 0 };
  function breakerRecordSuccess() {
    if (breaker.state === "half-open") {
      breaker.state = "closed";
      breaker.failures = [];
    }
  }
  function breakerRecordFailure() {
    const now = Date.now();
    breaker.failures = breaker.failures.filter((t) => now - t < BREAKER_WINDOW_MS);
    breaker.failures.push(now);
    if (breaker.failures.length >= BREAKER_THRESHOLD && breaker.state === "closed") {
      breaker.state = "open";
      breaker.openedAt = now;
      metrics.breaker_opens++;
      console.error(`Breaker OPEN (${breaker.failures.length} failures in ${BREAKER_WINDOW_MS}ms)`);
    }
  }
  function breakerCheck() {
    if (breaker.state === "open") {
      if (Date.now() - breaker.openedAt >= BREAKER_OPEN_MS) {
        breaker.state = "half-open"; // allow one probe
        return true;
      }
      return false;
    }
    return true;
  }

  // ---------- In-flight coalescing (singleflight) by userId ----------
  // Key insight for your traffic: many pairs share the same userId (one creator followId).
  // Coalescing by userId (not pair) dedupes those too.
  const inflight = new Map(); // userId -> Promise<Set<string>>

  // ---------- Upstream concurrency semaphore with bounded queue ----------
  let upstreamActive = 0;
  const upstreamQueue = [];
  function acquireUpstream() {
    if (upstreamActive < UPSTREAM_MAX_CONCURRENT) {
      upstreamActive++;
      return Promise.resolve(false);
    }
    if (upstreamQueue.length >= UPSTREAM_MAX_QUEUE) {
      return Promise.reject(Object.assign(new Error("overloaded"), { status: 503 }));
    }
    return new Promise((resolve) => upstreamQueue.push(resolve));
  }
  function releaseUpstream() {
    upstreamActive--;
    const next = upstreamQueue.shift();
    if (next) {
      upstreamActive++;
      next(false);
    }
  }

  // ---------- Simple fixed-window rate limiter (per IP, per worker) ----------
  // NOTE: per-worker counts; behind a load balancer use Redis. Good enough to shed load.
  const rateMap = new Map(); // ip -> { count, reset }
  function rateLimit(req, res, next) {
    const ip = req.ip || req.socket.remoteAddress || "unknown";
    const now = Date.now();
    let rec = rateMap.get(ip);
    if (!rec || now > rec.reset) {
      rec = { count: 0, reset: now + RATE_LIMIT_WINDOW_MS };
      rateMap.set(ip, rec);
    }
    rec.count++;
    if (rec.count > RATE_LIMIT_MAX) {
      res.set("Retry-After", String(Math.ceil((rec.reset - now) / 1000)));
      return res.status(429).json({ ok: false, error: "rate_limited" });
    }
    next();
  }

  function isValidId(v) {
    // Roblox user IDs are numeric; reject injection / oversized input early
    return typeof v === "string" && /^\d{1,20}$/.test(v);
  }

  async function fetchPage(url) {
    const ctrl = new AbortController();
    const t = setTimeout(() => ctrl.abort(), UPSTREAM_TIMEOUT_MS);
    try {
      const r = await fetch(url, {
        agent: url.startsWith("https:") ? httpsAgent : httpAgent,
        signal: ctrl.signal,
        headers: { Accept: "application/json" },
      });
      if (r.status === 429) {
        const err = new Error("upstream_rate_limited");
        err.status = 429;
        throw err;
      }
      if (!r.ok) {
        const err = new Error(`upstream_${r.status}`);
        err.status = r.status;
        throw err;
      }
      return await r.json();
    } finally {
      clearTimeout(t);
    }
  }

  async function fetchFollowingsSet(userId) {
    if (!breakerCheck()) {
      metrics.breaker_rejects++;
      throw Object.assign(new Error("circuit_open"), { status: 503 });
    }
    await acquireUpstream();
    try {
      const ids = new Set();
      let nextCursor = null;
      for (let page = 0; page < UPSTREAM_MAX_PAGES; page++) {
        let url = `https://friends.roproxy.com/v1/users/${encodeURIComponent(userId)}/followings?limit=100`;
        if (nextCursor) url += `&cursor=${encodeURIComponent(nextCursor)}`;
        let json;
        try {
          json = await fetchPage(url);
        } catch (e) {
          if (e?.status === 429 || e?.status >= 500 || e?.name === "AbortError") breakerRecordFailure();
          throw e;
        }
        const list = json.data || json.followings || json.users || [];
        if (!Array.isArray(list)) break;
        for (const u of list) ids.add(String(u.id ?? u.userId));
        nextCursor = json.nextPageCursor;
        if (!nextCursor) break;
      }
      breakerRecordSuccess();
      metrics.upstream_ok++;
      return ids;
    } catch (e) {
      metrics.upstream_fail++;
      throw e;
    } finally {
      releaseUpstream();
    }
  }

  function getFollowingsCoalesced(userId) {
    const existing = inflight.get(userId);
    if (existing) {
      metrics.coalesced++;
      return existing;
    }
    const p = fetchFollowingsSet(userId).finally(() => inflight.delete(userId));
    inflight.set(userId, p);
    return p;
  }

  function refreshInBackground(key, userId, followId) {
    // SWR: don't await, don't throw; update pair cache when done
    getFollowingsCoalesced(userId).then(
      (set) => cacheSet(key, set.has(String(followId))),
      () => {} // upstream failed: keep serving stale until STALE_MS expires
    );
  }

  app.get("/health", (_req, res) => res.json({ ok: true }));
  app.get("/metrics", (req, res) => {
    if (!SECRET || req.headers["x-api-key"] !== SECRET) {
      return res.status(403).json({ ok: false });
    }
    res.json({
      ok: true, pid: process.pid, ...metrics,
      cacheSize: CACHE.size, inflight: inflight.size,
      upstreamActive, upstreamQueued: upstreamQueue.length,
      breaker: breaker.state,
    });
  });

  app.get("/follows", rateLimit, async (req, res) => {
    metrics.total++;
    if (!SECRET || req.headers["x-api-key"] !== SECRET) {
      return res.status(403).json({ ok: false });
    }

    const { userId, followId } = req.query;
    if (!isValidId(userId) || !isValidId(followId)) {
      return res.status(400).json({ ok: false });
    }

    const key = `${userId}:${followId}`;
    const now = Date.now();

    const cached = cacheGet(key, now);
    if (cached?.status === "fresh") {
      metrics.hit++;
      res.set("Cache-Control", "private, max-age=60");
      res.set("X-Cache", "HIT");
      return res.json({ ok: true, follows: cached.entry.follows });
    }
    if (cached?.status === "stale") {
      // Serve stale instantly, refresh in background. This is what keeps p99 low at 1000 rps.
      metrics.stale++;
      metrics.swr++;
      refreshInBackground(key, userId, followId);
      res.set("X-Cache", "STALE");
      return res.json({ ok: true, follows: cached.entry.follows, stale: true });
    }

    metrics.miss++;
    try {
      const set = await getFollowingsCoalesced(userId);
      const follows = set.has(String(followId));
      cacheSet(key, follows);
      res.set("Cache-Control", "private, max-age=60");
      res.set("X-Cache", "MISS");
      res.json({ ok: true, follows });
    } catch (e) {
      const stale = CACHE.get(key); // any version, even fully expired, beats an error
      if (stale) {
        res.set("X-Cache", "STALE");
        return res.json({ ok: true, follows: stale.follows, stale: true });
      }
      if (e?.message === "overloaded" || e?.status === 503) {
        metrics.overloaded++;
        res.set("Retry-After", "2");
        return res.status(503).json({ ok: false, error: e.message === "circuit_open" ? "circuit_open" : "overloaded" });
      }
      if (e?.name === "AbortError") {
        return res.status(504).json({ ok: false, error: "upstream_timeout" });
      }
      if (e?.status === 429) {
        return res.status(502).json({ ok: false, error: "upstream_rate_limited" });
      }
      console.error(e);
      res.status(500).json({ ok: false });
    }
  });

  const server = app.listen(PORT, () => {
    console.log(`Worker ${process.pid} listening on ${PORT}`);
  });

  // Don't let slow clients / slow upstream hold sockets forever
  server.keepAliveTimeout = 65000;
  server.headersTimeout = 66000;
  server.requestTimeout = 15000;

  const shutdown = () => {
    console.log("Shutting down...");
    server.close(() => process.exit(0));
    setTimeout(() => process.exit(1), 10000).unref();
  };
  process.on("SIGTERM", shutdown);
  process.on("SIGINT", shutdown);
}