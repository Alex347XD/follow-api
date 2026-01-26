import express from "express";
import fetch from "node-fetch";

const app = express();
const PORT = process.env.PORT || 3000;
const SECRET = process.env.API_SECRET;

const CACHE = new Map();
const CACHE_TIME = 60 * 1000;

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
    const url = `https://friends.roproxy.com/v1/users/${userId}/followings`;
    const r = await fetch(url);
    const json = await r.json();

    const follows =
      Array.isArray(json.data) &&
      json.data.some(u => String(u.id) === String(followId));

    CACHE.set(key, { time: now, follows });
    res.json({ ok: true, follows });
  } catch (e) {
    res.status(500).json({ ok: false });
  }
});

app.listen(PORT, () => {
  console.log("Server running on port", PORT);
});
