Week 3 Notes：TPM 预扣、结算与对账
本周结果
新增 tenant（虚拟 Key 和租户）、quota（令牌桶预扣和结算）两个子包，以及 resources/lua/tpm_bucket.lua
链路：ApiKeyFilter → Controller（客户端没传 max_tokens 时填默认值、估算输入）→ QuotaSettler.meter（预扣 → 转发 → 结算）→ 编码器
测试：QuotaSettlerTest（正常结束、取消、首字节前出错、预扣被拒）、RedisTpmLimiterTest（Testcontainers 起真 Redis）全部通过
依赖：spring-boot-starter-data-redis-reactive、testcontainers junit-jupiter（版本由 BOM 管理）
第 1 步：虚拟 Key 和租户（速过）
配置里只存 Key 的 SHA-256：理由和密码只存哈希一样，配置或内存泄露时拿不到可用的 Key。
租户放在 exchange attributes 里，没有放 Reactor Context（和手册不同）：Controller 方法里同步组装请求时就要用到租户（默认 max_tokens），用 exchange 属性最直接。它和请求绑定，不依赖线程，所以同样能避开 ThreadLocal 在换线程时丢值的问题。
ApiKeyFilter 只拦截 /v1/，假上游（/v1beta/）不受影响。
配置绑定：@ConfigurationProperties 的 record 用构造器绑定；YAML 列表展开成 gateway.tenants[i].*，元素类型从构造参数的泛型里拿到；key-sha256 按宽松绑定对应 keySha256。
第 2 步：预扣（重点）
预扣量 = 输入估算 × 1.3 + max_tokens
客户端没传 max_tokens 时，网关填入租户默认值并转发给上游：网关修改了请求，换来的是输出有上限，预扣才有界。
单个请求的预扣量超过 tpm 时直接返回 400，不返回 429：令牌桶的余额最多补到容量，这种请求重试多少次都不会成功；返回 429 会让带自动重试的 SDK 一直重试下去。没有选择把 max_tokens 截短后再转发，因为客户端会拿到被截断的回答，却不知道原因。
用令牌桶，不用按分钟的固定窗口：结算可能跨过分钟边界，固定窗口下退款该退到哪个窗口说不清；令牌桶只有一个余额，直接加回去就行。
Lua 脚本保证原子性：一个脚本处理 reserve 和 adjust 两种模式；时间用 Redis 的 TIME，多个网关实例不受各自时钟偏差影响；key 空闲 2 分钟后过期，过期等价于满桶。key 用 tpm:{tenantId}，以后上 Redis Cluster 时同一租户落在同一个槽。
结算允许欠账（余额为负）：实际用量超过预扣时照样补扣，后面的请求会多等一会儿。
网关在预扣和结算之间崩溃：这部分额度只是多扣了，桶会随时间补满，系统自己恢复，而且出错的方向是偏保守的。对比第 5 周的钱的预算：预算不会自动补回来，需要“预留记录 + TTL + 定期清理”。
网关自己的 429：type=gateway_rate_limit，带 Retry-After 和 x-ratelimit-remaining-tokens，和上游 429 的 upstream_error 区分开。
第 3 步：结算（本周核心）
用 Flux.usingWhen，不用 doFinally：完成、出错、取消三条路径分开写；结算是异步的 Redis 调用，usingWhen 会订阅它，doFinally 里只能自己 subscribe，失败了也没人知道。
只结算一次：取消和完成可能同时到达，用 AtomicBoolean 保护。
结算失败只记日志：不能让 Redis 的问题把已经发完的流变成错误。
直接复用已有的 UsageUpdate 事件（和手册不同，不需要新加事件类型）；Meter.observe 用 switch 模式匹配，由编译器检查是否覆盖了所有子类型。
场景	实际用量	source
首字节前出错	0，全额退回	REFUNDED
正常结束（收到 Finished）且有 usage	usage.input + usage.output	USAGE
中途出错、客户端取消、结束但没有 usage	输入：有 usage 用 usage，没有就用估算；输出：max(usage.output, 已发出的量)	ESTIMATED
流断了不全额退回：上游已经生成的部分是计费的；全额退回会被人利用来白嫖额度（发请求、拿到大半回答后断开）。
已发出的量按每个 TextDelta 分别计数，片段边界处会略多估，方向是保守的。
不同厂商取消时的估算精度不同：Gemini 每个 chunk 都带累计 usage，取消时一般已经有上游的数字；Claude 的输出 token 只在流快结束时的 message_delta 里，中途取消只能按已发出的文字估算。
第 4 步：对账日志和实测（Gemini，gemini-3.1-flash-lite）

对账日志单独用 settlement logger：requestId tenant model outcome source reserved actual delta ratio estInput usage。第 5 周改成发到 Kafka。

实验	结果
max_tokens=500，约 300 词的回答	ratio ≈ 1.4
max_tokens=2000	ratio ≈ 1（输出基本用满）
输入估算 vs 实际	估算 20，实际 15，约 1.33 倍
429：tpm=1200，3 个并发请求	两个 200，一个 429；剩余 160 = 1200 − 2×520；Retry-After 18 = (520 − 160) / 20 每秒
取消	只有一条 CANCEL/ESTIMATED，actual=158（usage 15 + 143），退回 362

结论

ratio ≈ (输入估算 + max_tokens) / (实际输入 + 实际输出)。输入通常远小于 max_tokens，多估多少主要由 max_tokens 决定，1.3 这个系数暂时不用调。
多估的代价是：请求在途期间占着额度，同一个租户能同时进行的请求变少；请求结束后多扣的会退回，不会造成长期的额度损失。客户端不传 max_tokens 时（默认 1024，短回答）多估最严重。
Retry-After 偏保守：只按补充速度算，没有考虑在途请求结算时会退回的额度。在途请求什么时候结束，在请求开始时没法知道，所以保持保守。
以后可以改进的方向：不按 max_tokens 预扣，改按这个租户和模型历史输出量的 P95 预扣，超出的部分靠允许欠账的结算补回来。好处是预扣更接近实际，代价是会短时间超过 TPM。