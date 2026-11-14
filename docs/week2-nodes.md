# Week 2 Notes

## 环境
- **统一用 Java 21**：pom 的 release、IDEA 的 Project SDK、命令行 JDK、IDEA 里的 Maven Runner JRE 全部是 21。
    - 约束：编译和运行用的 JDK 都不能低于 release；release 也决定了能用哪些语法和 API。
    - 改用 21 的原因：sealed 接口配合 switch 模式匹配，新增事件类型时编译器会报错。
- **Spring Boot 3.5.x**，用 BOM 统一管理依赖版本，不选 4.x，因为 4.x 改用 Jackson 3，包名变了。
- Jackson 和 JUnit 的版本交给 BOM 管理，pom 里不写死版本号，避免同一个库的不同模块混用不同版本。
- 两个 API Key 都从环境变量读取，网关启动时两个都必须存在。PowerShell 里用 `$env:` 设置的变量 IDEA 读不到，要配置在 IDEA 的运行配置里，或者设置成用户级别的环境变量。

## 第 1 步：Reactor 背压与取消

### 核心模型
- 数据从上游流向下游：onNext、onComplete、onError
- 控制信号从下游流向上游：request(n)、cancel
- 上游累计发出的元素数量，不能超过下游累计请求的总数；`request(unbounded)` 就是 `Long.MAX_VALUE`

### 实验结论
| 实验 | 结论 |
|---|---|
| 1 | 惰性：订阅之后才开始执行；用 lambda 订阅时默认是 request(unbounded) |
| 2 | 背压：request(1) 和

## 第 4 步：并发冒烟测试（假上游：每 100ms 一个 chunk，持续 30 秒）

- **结果**：1000 个并发流全部成功；线程数峰值 34，基本保持不变；堆内存约 160MB（包含假上游）。
  对比：拉模式下，1000 个流需要 1000 个阻塞线程。
- **发现 1：上游连接池才是流式网关的并发上限。** Reactor Netty 默认的连接池大约 500 条连接，超出的请求要排队，表现为“分两批跑完”，排队太久的请求报 500。每个流从开始到结束都占着一条连接，所以连接池的大小决定了网关能同时承载多少个流。上游 WebClient 的连接池改成 maxConnections 2000。
- **发现 2：获取连接失败也是“流开始前的错误”，但目前被漏掉了**，返回的是 500 → 第 4 周做错误分类时，归入可以切换厂商或重试的那一类，返回 503。
- **发现 3：区分系统的极限和测试方法造成的假象。** 1000 个连接请求在同一瞬间发出，超过了 Windows 监听端口的等待队列，被直接拒绝。客户端改成每 5ms 发起一个请求后解决。
- 打开了编译器的 `-parameters` 选项，使用 BOM 而不是 parent 时，Spring Boot 不会自动打开它。