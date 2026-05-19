# TGLog C# SDK 使用教程

本教程演示如何在 .NET 8 应用中集成 TGLog C# SDK，将游戏服务器的遥测日志高吞吐量地投递到 TGLog 基础设施。

> 适用人群：希望在 .NET 8 项目中接入 TGLog 的后端 / 游戏服务端工程师。

---

## 1. SDK 能做什么

- `TglogClient`：进程级、线程安全的发送器，支持 TCP / UDP。
- 多 Worker 线程轮询分发，每个 Worker 拥有独立的无锁 MPSC 队列和连接池。
- 三重批量触发条件：消息数、字节数、最大延迟（任一满足即发送）。
- 三种发送方式：fire-and-forget、回调、`async`/`await`。
- 内置 Prometheus 指标（prometheus-net），日志框架可插拔（Serilog / NLog / MEL 任选）。
- 自动 DNS 刷新、连接生命周期回收、带退避的重试。

发送语义：TGLog 协议是**单向请求**，没有服务端 ACK。SDK 把消息写入 OS socket 缓冲区即视为发送成功。

---

## 2. 准备工作

- .NET 8 SDK
- 可达的 TGLog 接入地址（host + port）
- 在工程中引用 SDK：

```xml
<ProjectReference Include="..\..\src\TglogSdk\TglogSdk.csproj" />
```

---

## 3. 60 秒上手

```csharp
using Tencent.Tglog;

var config = TglogConfig.CreateTcp("dev.tglog.com", 20001);

using var client = new TglogClient(config);

// payload 必须非空且以 '\n' 结尾
client.Send("login|1001|player_abc|success\n");
```

`using` 离开作用域时会调用 `Dispose`，自动 flush 并关闭连接。

---

## 4. 配置客户端

`TglogConfig` 采用流式 API。从 `CreateTcp` 或 `CreateUdp` 起手，按需链式配置：

```csharp
var config = TglogConfig.CreateTcp("dev.tglog.com", 20001)
    .SetWorkerNum(8)                     // 8 个后台 Worker 线程
    .SetBatchingMaxMessages(50)          // 攒到 50 条就发
    .SetBatchingMaxSize(32 * 1024)       // 或攒到 32 KB
    .SetBatchingMaxDelayMs(10)           // 或闲置 10 ms
    .SetMaxRetries(5)                    // 失败批最多重试 5 次
    .SetSendTimeoutMs(10_000)            // 单批最长 10s
    .SetMaxConnectionLifetimeSec(300)    // 连接 5 分钟回收一次
    .SetMaxPendingMessages(409_600)      // 每个 Worker 队列容量
    .SetDebugEnabled(true);              // 输出更详细的内部日志
```

需要记住的默认值与上限：

| 配置项 | 默认值 | 说明 |
|---|---|---|
| Worker 数 | 8 | 越多并发连接越多 |
| 批量消息数 | 50 | |
| 批量字节数 | 32 KB | 自动收敛到协议上限 |
| 批量延迟 | 10 ms | |
| 单 Worker 队列容量 | 409 600 | 反压边界 |
| TCP 单请求上限 | 512 KB | `TglogConfig.MaxTcpReqSize` |
| UDP 单请求上限 | 65 507 B | `TglogConfig.MaxUdpReqSize` |
| 连接超时 | 3 000 ms | |
| 连接生命周期 | 300 s | 0 = 不回收 |
| DNS 刷新间隔 | 60 s | |

构造时如果 `Host` / `Port` 非法，会抛 `TglogException(BadFlag)`；其他数值类配置非法时会被 `Validate()` 静默重置为默认。

---

## 5. 三种发送方式

无论哪种方式，payload 都必须满足：

1. 非 null 且非空。
2. 以 `\n` 结尾。
3. 长度不超过当前协议的 `MaxRequestSize`。

### 5.1 Fire-and-forget（即发即忘）

最适合高吞吐热路径，不关心单条消息的发送结果。

```csharp
client.Send("tick|frame=42\n");
```

如果 Worker 队列已满，会抛 `TglogException(SendQueueIsFull)`，请按需在调用处捕获。

### 5.2 回调

回调在 Worker 线程上执行：消息所属批次写入发送缓冲区时回调成功；重试用尽仍失败时回调异常。

```csharp
client.Send("purchase|1001|sword|100\n", (payload, ex) =>
{
    if (ex != null)
        Console.Error.WriteLine($"发送失败 [{ex.ErrorCode}]: {ex.Message}");
});
```

回调要保持轻量，回调内的异常会被 SDK 捕获并打日志，不影响后续投递。

### 5.3 `async` / `await`

只在确实需要等待单条结果时使用（例如测试、关键启动事件）：

```csharp
try
{
    await client.SendAsync("game_start|1001|map_01|team_a\n");
}
catch (TglogException ex)
{
    // 队列满 / 客户端已关闭 / payload 校验失败
}
```

不要在热循环里逐条 `await`，会把吞吐拖到批延迟级别。热路径用 fire-and-forget，必要时在收尾处 `Flush` 即可（或交给 `Dispose`）。

---

## 6. UDP 模式

把 `CreateTcp` 换成 `CreateUdp` 即可。批量数据会拼成单个 UDP datagram，因此单批上限降到 65 507 字节：

```csharp
var config = TglogConfig.CreateUdp("dev.tglog.com", 20001)
    .SetWorkerNum(2)
    .SetBatchingMaxMessages(20)
    .SetBatchingMaxDelayMs(5);
```

可以接受偶发丢包、追求更低开销时选择 UDP。其余场景推荐使用 TCP（也是默认值）。

---

## 7. 接入日志框架

默认使用 `NullLoggerFactory`，SDK 自身不输出任何日志。注入任意 `ILoggerFactory` 即可看到 SDK 的运行情况：

```csharp
using Microsoft.Extensions.Logging;
using Serilog;

var serilogLogger = new LoggerConfiguration()
    .MinimumLevel.Information()
    .WriteTo.Console()
    .CreateLogger();

using var loggerFactory = new LoggerFactory().AddSerilog(serilogLogger);

var config = TglogConfig.CreateTcp("dev.tglog.com", 20001)
    .SetLoggerFactory(loggerFactory)
    .SetDebugEnabled(true); // 包含内部 DBG 级别日志
```

任意 `ILoggerFactory` 实现都可以（MEL 内置 provider、Serilog、NLog 等）。

---

## 8. Prometheus 指标

`TglogClient.Metrics` 暴露的指标名称与 Java SDK 完全一致，方便共用监控面板。常用指标：

| 指标 | 含义 |
|---|---|
| `tglog_request_messages_total` | `Send` / `SendAsync` 接受的调用数 |
| `tglog_enqueued_messages_total` | 入队成功数 |
| `tglog_succeeded_messages_total` | 已写入发送缓冲区数 |
| `tglog_failed_messages_total` | 重试用尽失败数 |
| `tglog_dropped_messages_total` | 队列满被丢弃数 |
| `tglog_msg_queue_size{worker_id}` | 每个 Worker 当前队列深度 |
| `tglog_active_conn_count{worker_id}` | 每个 Worker 活跃连接数 |
| `tglog_endpoint_count` | 当前 DNS 解析出的 endpoint 数 |

开发期快速查看：

```csharp
Console.WriteLine(client.GetMetricsString());
Console.WriteLine(client.GetMetricsJson());
```

如果同一进程内**同时**运行多个 `TglogClient`（例如配置热加载），请给每个客户端配独立 registry，避免重复注册：

```csharp
var config = TglogConfig.CreateTcp("dev.tglog.com", 20001)
    .SetPrometheusRegistry(Prometheus.Metrics.NewCustomRegistry());
```

要通过 HTTP 暴露指标，可在 registry 上挂 prometheus-net 的 `MetricServer`（或其它抓取方式）。

---

## 9. 关闭与 `Flush`

`Dispose` 已经做了正确的收尾：停止接受新发送、按 `StopTimeoutMs`（默认 3s）排空队列、再多等一段时间让 socket flush，最后注销指标。

`Flush(timeoutMs)` 一般**不需要**显式调用——只在需要在某个时点保证排空时使用（例如测试或关键 checkpoint）。返回 `true` 表示所有 Worker 队列、批量缓冲、socket 缓冲都在超时内清空：

```csharp
if (!client.Flush(5_000))
{
    logger.LogWarning("TGLog 5 秒内未排空，仍有消息在途");
}
```

务必使用 `using` 包裹 `TglogClient`（或显式 `Dispose`），保证指标 registry 被正确清理。

---

## 10. 错误码速查

`TglogException.ErrorCode` 用 switch 即可处理：

| 错误码 | 触发场景 | 处理建议 |
|---|---|---|
| `BadLog` | payload 为空或缺少 `\n` | 修正调用方 |
| `LogTooLong` | payload 超过 `MaxRequestSize` | 拆分日志或改用 TCP |
| `SendQueueIsFull` | Worker 队列已满 | 反压、丢弃，或调大 `MaxPendingMessages` |
| `ProducerClosed` | `Dispose` 后仍在 `Send` | 不要在关闭后继续发送 |
| `BadFlag` | 构造时 host/port 非法 | 修正配置 |
| `NewConnFailed` | 启动时无可用 endpoint | 检查 DNS / 防火墙 |
| `SendTimeout`、`SendFailed`、`ConnWriteFailed` | 仅通过回调反馈 | 已经重试 `MaxRetries` 次 |

校验类错误（`BadLog`、`LogTooLong`）从 `Send` / `SendAsync` 同步抛出；网络类错误只通过回调或被 `await` 的 `Task` 暴露。

---

## 11. 整合示例 —— 生产环境模板

```csharp
using Microsoft.Extensions.Logging;
using Tencent.Tglog;

public sealed class TelemetryService : IAsyncDisposable
{
    private readonly TglogClient _client;
    private readonly ILogger<TelemetryService> _log;

    public TelemetryService(ILoggerFactory loggerFactory, ILogger<TelemetryService> log)
    {
        _log = log;

        var config = TglogConfig.CreateTcp("tglog.internal", 20001)
            .SetWorkerNum(Math.Max(4, Environment.ProcessorCount))
            .SetBatchingMaxMessages(100)
            .SetBatchingMaxSize(64 * 1024)
            .SetBatchingMaxDelayMs(20)
            .SetMaxRetries(5)
            .SetMaxPendingMessages(1_000_000)
            .SetLoggerFactory(loggerFactory)
            .SetPrometheusRegistry(Prometheus.Metrics.NewCustomRegistry());

        _client = new TglogClient(config);
    }

    public void Track(string line)
    {
        try
        {
            _client.Send(line.EndsWith('\n') ? line : line + "\n");
        }
        catch (TglogException ex) when (ex.ErrorCode == TglogErrorCode.SendQueueIsFull)
        {
            _log.LogWarning("TGLog 队列已满，丢弃事件");
        }
    }

    public ValueTask DisposeAsync()
    {
        _client.Flush(5_000);
        _client.Dispose();
        return ValueTask.CompletedTask;
    }
}
```

---

## 12. 联调与压测

仓库自带的集成/压测命令行可用于联调和性能验证：

```bash
# 单条 TCP 烟雾测试
dotnet run --project integration-test/TglogSdk.IntegrationTest -- \
    --host 127.0.0.1 --port 20001 --messages 1 --tcp

# 10 万条消息，8 个 Worker，开启回调和 debug 日志
dotnet run --project integration-test/TglogSdk.IntegrationTest -- \
    --host dev.tglog.com --port 20001 \
    --messages 100000 --workers 8 --batch-size 50 --callback --debug
```

常用参数：`--tcp` / `--udp`、`--rate-limit <每秒条数>`（0 表示不限速）、`--callback`、`--debug`。完整参数请看 `--help`。

---

## 13. 常见问题

**Q：退出前一定要 `Flush` 吗？**
不需要——`Dispose` 已经包含 flush。只在你需要在某个时点强制排空时才显式调用。

**Q：成功回调在数据真正出网卡前就触发了，是 Bug 吗？**
不是。TGLog 协议无 ACK，SDK 把"写入 OS socket 缓冲"作为发送成功的判定，是有意为之。

**Q：整个进程能否只用一个 `TglogClient`？**
推荐这样做。`TglogClient` 线程安全，可以在所有 Worker 线程间共享，从而摊薄连接和指标开销。

**Q：能创建多个 `TglogClient` 吗？**
可以，但请通过 `SetPrometheusRegistry` 给每个客户端独立 registry，避免重复注册指标报错。

**Q：为什么 payload 必须以 `\n` 结尾？**
TGLog 协议按行分帧。SDK 选择校验换行符而不是替你修改 payload，把字节流的最终形式留给业务方控制。
