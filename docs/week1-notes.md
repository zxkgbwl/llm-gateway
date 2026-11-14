#
# Week 1 Notes

## 第 2 步：curl 看原始 SSE 流

模型：claude-haiku-5-5 / gemini-3.1-flash-lite
抓包：captures/*.txt、captures/*.headers

### 实验记录

| 实验 | Claude | Gemini |
|---|---|---|
| 1 正常流式 | 事件序列：message_start → content_block_start → ping → content_block_delta ×N → content_block_stop → message_delta → message_stop | 短回复只有 2 个 chunk：文本全在第 1 块；最后一块 text 为空，只带 thoughtSignature 和 finishReason |
| 2 去掉 ?alt=sse | — | 返回 JSON 数组 `[{...},{...}]`，多行格式化，不是 SSE |
| 3 输出上限 = 5 | 输出 "I'm Cla"，在词中间截断；stop_reason=max_tokens，output_tokens=5 | 只输出 "I"；finishReason=MAX_TOKENS，candidatesTokenCount=1，没有 thoughtsTokenCount。另外 4 个 token 去向未知，现象可稳定复现 |
| 4 错误 Key | 401，Content-Type: application/json，带 x-should-retry: false | 400（不是 401）；Content-Type 写的是 text/event-stream，但 body 是普通 JSON；details[].reason=API_KEY_INVALID |
| 5 去掉 -N | 终端里看不出差异。curl 输出到终端时本来就逐行刷新，缓冲只在输出接管道或写文件时才生效 | 同左 |

### 检查点

**Q1 流怎么算结束？**
- Gemini：没有专门的结束事件。某个 chunk 出现 finishReason（STOP / MAX_TOKENS / ...），随后连接关闭。
- Claude：以 `event: message_stop` 结束；结束原因在之前的 message_delta.stop_reason 里。
- 如果连接断了但一直没收到结束标志，说明流被截断，应该产出 StreamError，不能当成正常结束。

**Q2 usage 在哪里，增量还是累计？**
- Gemini：每个 chunk 都带 usageMetadata。
- Claude：message_start 里有 input_tokens 和一个初始 output_tokens（8）；message_delta 里是最终值（46）。
- 两家都是累计值。证据：Claude 如果按增量累加，会得到 8 + 46 = 54，比实际多；Gemini 的 total = 7 + 11 = 18，和累计值吻合。
- 所以取最后一次快照，不能累加。这就是 UsageUpdate 设计成累计快照的原因。

**Q3 出错时还是 SSE 吗？**
- 流开始前出错：返回的是普通 JSON。Gemini 的 Content-Type 仍然写着 text/event-stream，所以要以状态码为准：非 2xx 抛 UpstreamHttpException，这时网关还可以重试或切换厂商。
- 流开始后出错：HTTP 200 已经发出，只能用流内事件报错（Claude 有 `event: error`），对应 StreamError。已经发给客户端的内容收不回来。

### 对网关设计的启示
- 先看状态码，再决定用什么解析器。不要相信 Content-Type。
- usage 取最后一次快照，不累加。
- 翻译器要跳过 text 为空的 part（例如 Gemini 只带 thoughtSignature 的那一块）。
- 是否截断只能从 finish reason 判断，看文本内容看不出来。
- 每一层都可能缓冲：curl、Nginx（要设 `X-Accel-Buffering: no`）、网关自己。第 2 周写 Flux 转发时，每个事件都要立即刷出。
- 错误分类：Gemini 的 400 要看 details.reason 才知道具体原因；Claude 有 x-should-retry 可以参考。

- 
## 第 3 步：代码审查与真实数据测试

### 调用链
StreamingClient.stream(adapter, request)
→ adapter.buildRequest()：ChatRequest → UpstreamRequest（各家的 URL、请求头、请求体）
→ 发请求：状态码非 2xx 抛 UpstreamHttpException
→ new ChatStream(new SseReader(body), adapter.newTranslator())
ChatStream.hasNext() → pullFromUpstream() → SseReader.next() → translator.translate() → 放入 pending 队列

### 设计决定
- **请求与响应对称**：请求方向 ProtocolAdapter 把 ChatRequest 翻译成 UpstreamRequest，响应方向 EventTranslator 把 SSE 事件翻译成 StreamEvent。各家差异都关在这一对类里，接入新厂商只需要写这一对。
- **翻译器实例**：每个流新建一个（adapter.newTranslator()），所以有状态也安全。
    - Claude：有状态。stop_reason 在 message_delta 里收到并暂存，到 message_stop 时才产出 Finished。
    - Gemini：无状态。同一块数据里按 [TextDelta, UsageUpdate, Finished] 的顺序产出。
- **事件顺序**：ChatStream 遇到终止事件后，同一批里后面的事件都会被丢弃。所以 UsageUpdate 必须排在 Finished 前面。
- **出错判断以状态码为准**：Content-Type 不可信，Gemini 出错时仍然返回 text/event-stream。
- **message_stop 之前没收到 message_delta**：按正常结束处理，因为文本已经完整送达。rawReason 为 null，表示 usage 可能不完整，第 3 周结算时要用本地估算补足。
- **分派依据**：按 JSON 里的 type 字段分派，不依赖 SSE 的分帧方式，两家处理方式也一致。
- **不认识的事件**：直接忽略，以保持向前兼容。非文本类型的 delta 应该打一条 debug 日志，logger 在构造时注入。

### 测试：CaptureReplayTest（真实抓包回放，全部通过）
- 两家各两份抓包：正常结束、因输出上限被截断
- 两家的结束原因映射到同一套统一枚举
- Gemini 没收到 finishReason 就 EOF → StreamError（截断）
- Claude 没收到 message_delta 就收到 message_stop → Finished，rawReason 为 null

### 思考题
- **拉模式下 1 万个并发流**：每个流从头到尾独占一个阻塞的线程，就需要 1 万个线程，栈空间约 10GB 量级，再加上大量的上下文切换。如果用有上限的线程池（比如 200），每个流持续 30 秒，网关每秒最多只能接纳大约 7 个新请求。推模式下等待不占用线程，少量事件循环线程就能撑住。
    - 追问：为什么不用虚拟线程？虚拟线程解决了线程数量问题，但背压和取消传播仍然要自己处理；另外项目用的是 Java 17。
- **半行 SSE、半个汉字**：
    - 拉模式：BufferedReader.readLine() 读不到完整的一行会一直阻塞，InputStreamReader 的解码器是有状态的，会暂存半个字符。半包问题被阻塞 I/O 隐式处理了。
    - 推模式（第 2 周）：要自己处理。先在字节层面缓冲，再按 0x0A 切出完整的行，最后逐行做 UTF-8 解码。0x0A 不会出现在多字节字符的中间，所以半个汉字的问题自然消失。流结束时缓冲区里还有残留数据，就产出 StreamError。

### 留到后面处理
- 流开始后没有读超时：上游卡住时 read() 会永远阻塞 → 第 4 周做超时分层（首 token 超时、空闲超时）
- 读取错误响应体时没有大小上限
- cache_creation 和 cache_read 这两类 token 没有计入 → 第 5 周做成本统计时处理

## 第 4 步：延迟与断开连接

- **TTFT**：短 prompt 和几千 token 的长 prompt 差距不大。在常见的 prompt 长度下，TTFT 主要由网络和排队决定；prefill 要到几万 token 的超长上下文才会成为主要因素。
- **断开后的计费**：客户端断开后，厂商会停止计费。
    - 所以第 2 周的取消传播不是为了省钱，而是为了及时释放资源：上游连接、网关线程，以及厂商那边的并发名额和 RPM 额度。
    - 被取消的流拿不到最终 usage，结算时要用已收到的文本做估算。

## 第 5 步：token 估算偏差（Gemini countTokens，只计数不生成）

| 类型 | 本地估算 | Gemini | 比值 |
|---|---|---|---|
| 中文 | 92 | 76 | 0.83 |
| 英文 | 60 | 59 | 0.98 |
| 代码 | 72 | 89 | 1.24 |

- **决定**：Gemini 的预扣系数统一取 **1.3**，偏向多估。估多了只是在额度接近用完时误拒一些请求，多扣的部分会在结算时退回；估少了会让租户实际超出额度，这部分收不回来。
- **代价**：中文请求会被多扣约 1.6 倍。按文本类型分别设系数虽然更准，但复杂度更高，暂时不做。
- **关键发现**：预扣量 = 输入估算值 × 系数 + max_tokens。输出部分通常远大于输入部分，所以预扣准不准，主要取决于 max_tokens 怎么处理，而不是分词器准不准 → 第 3 周解决。
- **局限**：样本只有 60 到 90 个 token，每类只跑了一次，比值只能说明量级。系数要按厂商分别设定，Claude 的分词器切得更碎，系数大概率会更高。

## 第 6 步：LiteLLM 对照

| 问题 | LiteLLM 的做法 | 我的取舍 |
|---|---|---|
| 冷却（router_utils/cooldown_handlers.py） | 429（模型组里有多个部署时）、401、404、最近一分钟失败率过高时触发冷却；可配置 allowed_fails，并按错误类型区分；冷却时长依次取部署配置、Retry-After、全局默认 5 秒；状态存在 Redis 里，全集群共享 | 照搬这个思路。另外要先把 Gemini 的 400 加 API_KEY_INVALID 归类为鉴权错误 |
| TPM（proxy/hooks/parallel_request_limiter.py） | 请求前只做检查，请求成功后才累加 TPM。并发时，同一时刻进来的请求看到的都是旧值，可能一起冲过上限 | 改进：请求前按估算值预扣，请求后按实际用量结算 → 第 3 周的面试论据 |

- 流式请求：一旦有内容已经发给调用方，就不再重试，和第 2 步“流开始后不能重试”的结论一致。
- 注：以上代码来自旧版本，新版本的限流器可能已经重写过，面试前要用主仓库的最新代码再核对一遍。

## 概念备忘
- **RPM**：每分钟请求数，每次请求计 1，请求前就知道该扣多少，可以直接复用我已有的限流器。
- **TPM**：每分钟 token 数，一次请求消耗多少要到请求结束才知道，所以需要预扣加结算。
- **token 没有行业标准**：各家用各自的分词器计数。计费只认上游返回的 usage，本地估算只用于请求前的决策：限流准入、上下文长度检查、按成本路由，以及拿不到 usage 时的兜底。