local key = KEYS[1]
local limit = tonumber(ARGV[1])
local window = tonumber(ARGV[2])

local time_result = redis.call("TIME")
local now = tonumber(time_result[1]) + (tonumber(time_result[2]) / 1000000)
local window_start = now - window

redis.call("ZREMRANGEBYSCORE", key, "-inf", window_start)
local count = redis.call("ZCARD", key)

local allowed = 0
if count < limit then
  local member = tostring(now) .. "-" .. tostring(math.random())
  redis.call("ZADD", key, now, member)
  count = count + 1
  allowed = 1
end

redis.call("EXPIRE", key, math.ceil(window) + 1)

local remaining = limit - count
if remaining < 0 then
  remaining = 0
end

local reset_after = window
local oldest = redis.call("ZRANGE", key, 0, 0, "WITHSCORES")
if #oldest > 0 then
  local oldest_ts = tonumber(oldest[2])
  reset_after = (oldest_ts + window) - now
  if reset_after < 0 then
    reset_after = 0
  end
end

return {allowed, tostring(remaining), tostring(reset_after)}
