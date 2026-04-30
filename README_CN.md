# TGLog SDK for C#

高性能游戏服务器远程日志 SDK，基于 .NET 实现，提供异步、批量的日志投递能力，对接 TGLog 基础设施。

本项目是 Java TGLog SDK 的 C# 移植版本，提供一致的 API 和行为。

## 特性

- **高性能** - 无锁 MPSC 队列、非阻塞 I/O、消息批量发送
- **TCP / UDP** - 支持两种传输协议
- **异步发送** - 支持 fire-and-forget、回调、async/await 三种发送模式
- **连接池** - 自动连接管理，支持重试与退避策略
- **Prometheus 指标** - 内置可观测性（请求计数、队列深度、连接数等）
- **可插拔日志** - 支持任意 `ILoggerFactory` 实现（Serilog、NLog、MEL 等）

## 环境要求

- .NET 8.0+

## 快速开始

### 编译

```bash
dotnet build TglogSdk.sln
```

### 基本用法

```csharp
using Tencent.Tglog;

// 创建配置
var config = TglogConfig.CreateTcp("server-host", 20001)
    .SetWorkerNum(4)
    .SetBatchingMaxMessages(50)
    .SetBatchingMaxDelayMs(10);

// 创建客户端并发送
using var client = new TglogClient(config);

// Fire-and-forget（发后即忘）
client.Send("login|1001|player_abc|success\n");

// 带回调
client.Send("purchase|1001|sword|100\n", (payload, ex) =>
{
    if (ex != null) Console.WriteLine($"发送失败: {ex.Message}");
});

// Async/Await
await client.SendAsync("game_start|1001|map_01|team_a\n");

// 关闭前刷新
client.Flush(5000);
```

### UDP 模式

```csharp
var config = TglogConfig.CreateUdp("server-host", 20001)
    .SetWorkerNum(2);
```

### 注入日志框架

```csharp
var config = TglogConfig.CreateTcp("server-host", 20001)
    .SetLoggerFactory(myLoggerFactory); // Serilog、NLog 等
```

### 自定义 Prometheus Registry

```csharp
var config = TglogConfig.CreateTcp("server-host", 20001)
    .SetPrometheusRegistry(Prometheus.Metrics.NewCustomRegistry());
```

## 配置项

| 方法 | 默认值 | 说明 |
|------|--------|------|
| `SetWorkerNum` | 8 | 后台工作线程数 |
| `SetBatchingMaxMessages` | 50 | 每批最大消息数 |
| `SetBatchingMaxSize` | 32KB | 每批最大字节数 |
| `SetBatchingMaxDelayMs` | 10 | 批量等待最大延迟（ms） |
| `SetMaxRetries` | 3 | 每批最大重试次数 |
| `SetSendTimeoutMs` | 30000 | 发送超时时间（ms） |
| `SetMaxConnectionLifetimeSec` | 300 | 连接最大存活时间（s） |
| `SetConnectionTimeoutMs` | 3000 | TCP 连接超时时间（ms） |
| `SetMaxPendingMessages` | 409600 | 每个 Worker 最大排队消息数 |
| `SetDnsUpdateIntervalSec` | 60 | DNS 重新解析间隔（s） |
| `SetDebugEnabled` | false | 启用调试日志 |

## 项目结构

```
TglogSdk.sln
  src/TglogSdk/                  - SDK 核心库
  examples/TglogSdk.Examples/    - 使用示例
  integration-test/              - 性能/集成测试
```

## 运行示例

```bash
# 默认连接 127.0.0.1:20001
dotnet run --project examples/TglogSdk.Examples

# 指定地址和端口
dotnet run --project examples/TglogSdk.Examples -- 10.0.0.1 9090
```

## 许可证

Proprietary - 腾讯内部使用
