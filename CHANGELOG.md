# 更新日志

## v1.0.0 (2026-06-18)

TGLog C# SDK 首次发布，从 Java TGLog SDK 移植的高性能游戏服务器远程日志 SDK。

### 功能特性

- **异步非阻塞发送** — 无锁 MPSC 队列，应用线程入队无需阻塞
- **多 Worker 线程** — 默认 8 个后台 Worker，每个运行 Socket.Select() 事件循环
- **TCP / UDP 双协议** — 支持两种传输协议，自动连接管理
- **消息批量发送** — 可按消息条数、总大小、时间延迟三种策略触发 flush
- **连接池管理** — 按 endpoint 分配连接，自动替换和故障恢复
- **指数退避重试** — 可配置重试次数，带抖动的退避策略应对瞬时故障
- **DNS 动态发现** — 周期性域名解析，endpoint 变更时通知 Worker
- **Prometheus 指标** — 完整的可观测性：计数器（request/enqueued/succeeded/failed/dropped/retry/timeout）和 gauge（队列大小、活跃连接数、endpoint 数量）
- **可插拔日志** — 注入任意 `ILoggerFactory` 实现（Serilog、NLog 等）
- **Fluent 配置 API** — `TglogConfig.CreateTcp(host, port).SetWorkerNum(4).SetBatchingMaxMessages(100)`
- **Callback 与 async/await** — 支持 fire-and-forget `Send()`、回调模式 `Send(payload, callback)`、异步 `SendAsync()` 返回 `Task`
- **连接生命周期抖动** — 避免多连接同时过期导致惊群
- **高水位反压** — 单连接输出缓冲区上限，防止内存耗尽
- **优雅关闭** — `Dispose()` 先 flush 待发消息，再关闭连接
- **连接替换异步生效** — 替换过期连接时，旧连接仅在新连接真正进入 Connected 状态后才退役（延迟关闭），避免握手窗口内可用连接池缩水；新连接握手失败时旧连接继续服务并在下一轮重试
- **TCP 消息级重发** — TCP 聚集写在部分字节提交后失败时，按完整消息边界裁剪并重发剩余消息，避免将已完整提交的批量前缀全部重复发送

### 架构

- 目标框架：.NET 8.0
- 命名空间：`Tencent.Tglog`
- 无锁环形缓冲区，128 字节 cache-line padding 避免 false sharing
- 非阻塞 Socket I/O，`Socket.Select()` 多路复用
- `Send()` / `SendAsync()` 并发调用线程安全

### 依赖

- `prometheus-net` v8.x — Prometheus 指标
- `Microsoft.Extensions.Logging.Abstractions` v8.x — 日志抽象

### 配置项

所有配置参数均有合理默认值，在 client 构造时校验。可选配置的非法值静默回退默认值；必填字段（Host/Port）非法时抛出 `TglogException`。

| 参数 | 默认值 | 说明 |
|------|--------|------|
| WorkerNum | 8 | 后台 Worker 线程数 |
| BatchingMaxMessages | 50 | 每批最大消息数 |
| BatchingMaxSize | 32KB | 每批最大字节数 |
| BatchingMaxDelayMs | 10 | 批量最大延迟（毫秒） |
| MaxPendingMessages | 409600 | 每个 Worker 的队列容量 |
| ConnectionTimeoutMs | 3000 | TCP 连接超时 |
| MaxConnectionLifetimeSec | 300 | 连接最大生命周期（0 = 持久连接） |
| DnsUpdateIntervalSec | 60 | DNS 重新解析间隔 |
| SendTimeoutMs | 30000 | 发送超时 |
| MaxRetries | 10 | 每批最大重试次数 |
| StopTimeoutMs | 3000 | 关闭超时 |

### 示例

- **TglogSdk.Examples** — 简单用法演示，包括 fire-and-forget、回调、async/await 和 metrics
- **TglogSdk.IntegrationTest** — 性能压测工具，集成 Serilog 演示外部日志库注入
