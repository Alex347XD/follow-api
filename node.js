import express from "express";
import fetch from "node-fetch";

const app = express();
const PORT = process.env.PORT || 3000;
const SECRET = process.env.API_SECRET;

const CACHE = new Map();
const CACHE_TIME = 60 * 1000; // 1 minute

app.get("/follows", async (req, res) => {
  if (req.headers["x-api-key"] !== SECRET) {
    return res.status(403).json({ ok: false });
  }

  const { userId, followId } = req.query;
  if (!userId || !followId) {
    return res.status(400).json({ ok: false });
  }

  const key = `${userId}:${followId}`;
  const now = Date.now();

  if (CACHE.has(key)) {
    const cached = CACHE.get(key);
    if (now - cached.time < CACHE_TIME) {
      return res.json({ ok: true, follows: cached.follows });
    }
  }

  try {
    let nextCursor = null;
    let follows = false;

    do {
      let url = `https://friends.roproxy.com/v1/users/${userId}/followings?limit=100`;
      if (nextCursor) url += `&cursor=${nextCursor}`;
s
      const r = await fetch(url);
      const json = await r.json();

      // Check property returned by RoProxy
      const list = json.data || json.followings || json.users || [];
      if (Array.isArray(list)) {
        follows = list.some(u => String(u.id) === String(followId));
        if (follows) break;
      }

      nextCursor = json.nextPageCursor;
    } while (nextCursor);

    CACHE.set(key, { time: now, follows });
    res.json({ ok: true, follows });
  } catch (e) {
    console.error(e);
    res.status(500).json({ ok: false });
  }
});

app.listen(PORT, () => {
  console.log("Server running on port", PORT);
});