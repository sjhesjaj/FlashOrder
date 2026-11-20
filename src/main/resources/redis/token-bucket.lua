local rate = tonumber(ARGV[1])
local capacity = tonumber(ARGV[2])
local time = redis.call('TIME')
local now = tonumber(time[1]) * 1000000 + tonumber(time[2])
local stored = redis.call('HMGET', KEYS[1], 'tokens', 'updated_at')
local tokens = tonumber(stored[1]) or capacity
local updated_at = tonumber(stored[2]) or now
local effective_now = math.max(now, updated_at)

tokens = math.min(capacity, tokens + (effective_now - updated_at) * rate / 1000000)
local allowed = 0
if tokens >= 1 then
    tokens = tokens - 1
    allowed = 1
end

redis.call('HSET', KEYS[1], 'tokens', string.format('%.17g', tokens),
    'updated_at', string.format('%.0f', effective_now))

-- Only expire a bucket once it would otherwise have refilled, including after a clock regression.
local refill_ms = math.max(1000, math.ceil(capacity / rate * 1000) + 1)
local clock_lag_ms = math.ceil((effective_now - now) / 1000)
redis.call('PEXPIRE', KEYS[1], string.format('%.0f', refill_ms + clock_lag_ms))
return allowed
