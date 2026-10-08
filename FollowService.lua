-- FollowService: shared hardened follow-check for follow-api
-- Canonical copy lives in this repo. Deploy: copy into
-- ServerScriptService as a ModuleScript named "FollowService".
--
-- Features vs a raw GetAsync snippet:
--  * 10min cache (600s), stale results cached 60s for fast revalidate
--  * in-flight coalescing by userId:followId (burst joins share 1 HTTP)
--  * RequestAsync + pcall + JSON validate, retries on 429/502/503/timeout with backoff
--  * no retry on 400/403 (config error), returns nil = unknown (caller shows "try again")
--  * accepts API stale:true as valid data
--  * userId == followId auto-passes (you can't follow yourself; covers testing your own game)
local HttpService = game:GetService("HttpService")

local FollowService = {}

-- EDIT THESE per game before deploying:
local FOLLOW_API_URL = ""
local FOLLOW_API_KEY = ""

local CACHE_TTL = 600 -- fresh cache, matches API TTL
local STALE_TTL = 60 -- API returned stale:true -> revalidate sooner
local INFLIGHT_TIMEOUT = 15

local cache = {} -- [key] = { time, follows, ttl }
local inflight = {} -- [key] = true while fetching

local function makeKey(userId, followId)
	return tostring(userId) .. ":" .. tostring(followId)
end

local function fetchOnce(userId, followId)
	local url = string.format("%s?userId=%d&followId=%d", FOLLOW_API_URL, tonumber(userId), tonumber(followId))
	local ok, resp = pcall(function()
		return HttpService:RequestAsync({
			Url = url,
			Method = "GET",
			Headers = { ["x-api-key"] = FOLLOW_API_KEY },
		})
	end)
	if not ok then
		return nil, "http_error:" .. tostring(resp)
	end
	if not resp then
		return nil, "no_response"
	end
	-- Config errors: don't retry
	if resp.StatusCode == 400 or resp.StatusCode == 403 or resp.StatusCode == 404 then
		warn(("[FollowService] config error %d for user %s: %s"):format(resp.StatusCode, tostring(userId), tostring(resp.Body):sub(1, 200)))
		return nil, "config_" .. resp.StatusCode
	end
	if not resp.Success then
		return nil, "status_" .. tostring(resp.StatusCode)
	end
	local data
	local ok2, err = pcall(function()
		data = HttpService:JSONDecode(resp.Body)
	end)
	if not ok2 or type(data) ~= "table" or data.ok == nil then
		warn("[FollowService] bad JSON for user " .. tostring(userId) .. ": " .. tostring(err))
		return nil, "bad_json"
	end
	if data.ok ~= true then
		return nil, "api_not_ok"
	end
	local follows = data.follows == true or data.follows == 1 or data.follows == "true"
	local ttl = data.stale == true and STALE_TTL or CACHE_TTL
	return follows, nil, ttl
end

function FollowService.Check(userId, followId)
	userId = tonumber(userId)
	followId = tonumber(followId)
	if not userId or not followId or userId <= 0 or followId <= 0 then
		return nil
	end
	-- Self-check (you testing your own game): can't follow yourself, auto-pass
	if userId == followId then
		return true
	end
	local key = makeKey(userId, followId)
	local now = os.clock()
	local hit = cache[key]
	if hit and now - hit.time < hit.ttl then
		return hit.follows
	end
	-- Coalesce: if another thread is fetching same key, wait for it (up to timeout)
	if inflight[key] then
		local waited = 0
		while inflight[key] and waited < INFLIGHT_TIMEOUT do
			task.wait(0.05)
			waited += 0.05
		end
		local hit2 = cache[key]
		if hit2 and os.clock() - hit2.time < hit2.ttl then
			return hit2.follows
		end
		-- if still inflight after timeout, fall through and fetch ourselves
		if inflight[key] then
			warn("[FollowService] inflight timeout for " .. key)
			return nil
		end
	end
	inflight[key] = true
	local result = nil
	for attempt = 1, 3 do
		local follows, err, ttl = fetchOnce(userId, followId)
		if follows ~= nil then
			cache[key] = { time = os.clock(), follows = follows, ttl = ttl or CACHE_TTL }
			result = follows
			break
		end
		-- don't retry config errors
		if err and (err:find("config_")) then
			break
		end
		if attempt < 3 then
			task.wait(1 * attempt) -- 1s, 2s backoff (covers API 503 overloaded / circuit_open)
		end
	end
	inflight[key] = nil
	if result == nil then
		warn(("[FollowService] unknown for user %d after retries"):format(userId))
	end
	return result
end

function FollowService.Clear(userId, followId)
	if userId and followId then
		cache[makeKey(userId, followId)] = nil
	else
		table.clear(cache)
	end
end

return FollowService
