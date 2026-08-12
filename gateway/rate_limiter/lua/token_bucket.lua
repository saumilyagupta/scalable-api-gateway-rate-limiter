local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])
local requested = tonumber(ARGV[3])

local time_result = redis.call("TIME")
local now = tonumber(time_result[1]) + (tonumber(time_result[2]) / 1000000)

local bucket = redis.call("HMGET", key, "tokens", "ts")
local tokens = tonumber(bucket[1])
local last_ts = tonumber(bucket[2])

if tokens == nil then
  tokens = capacity
  last_ts = now
end

local elapsed = math.max(0, now - last_ts)
tokens = math.min(capacity, tokens + elapsed * refill_rate)

local allowed = 0
if tokens >= requested then
  tokens = tokens - requested
  allowed = 1
end

redis.call("HMSET", key, "tokens", tostring(tokens), "ts", tostring(now))
local ttl = 60
if refill_rate > 0 then
  ttl = math.max(60, math.ceil(capacity / refill_rate) + 1)
end
redis.call("EXPIRE", key, ttl)

local reset_after = 0
if allowed == 0 and refill_rate > 0 then
  reset_after = (requested - tokens) / refill_rate
end

return {allowed, tostring(tokens), tostring(reset_after)}
