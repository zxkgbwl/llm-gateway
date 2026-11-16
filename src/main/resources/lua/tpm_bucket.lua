-- TPM 令牌桶：容量 = tpm，每毫秒补充 tpm / 60000
-- KEYS[1] 桶的 key
-- ARGV[1] 容量（tpm）
-- ARGV[2] 本次请求token 数
-- ARGV[3] reserve | settle， 先预留，后结算
-- 返回 {是否成功(1/0), 当前余额, 还需等待的毫秒数}
local cap = tonumber(ARGV[1])
local n = tonumber(ARGV[2])
local mode = ARGV[3]
 
local t = redis.call('TIME')

--转化为毫秒
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
--获取当前桶的状态
local v = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
--初次访问，桶不存在，按照容量初始化
local tokens = tonumber(v[1]) or cap
local ts = tonumber(v[2]) or now
--每毫秒补充rate
local rate = cap / 60000
 
-- 先按流逝时间补充，最多补到容量
tokens = math.min(cap, tokens + math.max(0, now - ts) * rate)

local ok, wait = 1, 0
if mode == 'reserve' then
  if tokens >= n then
    tokens = tokens - n
  else
    ok = 0
    wait = math.ceil((n - tokens) / rate)
  end
else
  -- 结算：n > 0 退回，n < 0 补扣。补扣不检查余额，允许欠账（余额为负）
  tokens = math.min(cap, tokens + n)
end
 
redis.call('HSET', KEYS[1], 'tokens', tostring(tokens), 'ts', tostring(now))
-- 空闲 2 分钟后桶必然已满，key 过期等价于满桶
redis.call('PEXPIRE', KEYS[1], 120000)
return {ok, math.floor(tokens), wait}

