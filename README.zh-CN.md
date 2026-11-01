[English](README.md) | 简体中文

# gcache

Go 的两级缓存（本地 + Redis/Olric）与分布式锁工具库。

> 状态：**已实现并有测试覆盖**（v0.0.0-dev：尚未 1.0，API 仍可能变化）。下表中
> 每一项都有测试覆盖，除非该行另有说明；仍然存在的缺口列在
> [未完成或部分完成](#未完成或部分完成)一节。

## 形态

```
L1   本地 LRU + TTL，按条目数与字节数设上限          （进程内，始终启用）
L2   RemoteStore 端口                                （redis/、olric/、memory/）
     └─ 与部署中其他进程共用的一个 Redis，或内嵌的 Olric 集群
INV  Invalidator 端口                                （Redis pub/sub、进程内总线）
LOCK Locker 端口                                     （Redis：SET NX PX + 比对 token）
SCAN Scanner 端口（可选）                            （Redis：SCAN MATCH <prefix>* COUNT）
```

两层同时生效：L1 在本地作答，未命中时 L2 是事实来源。把 L2 从 Redis 换成内嵌集群只是
换一个端口。key 布局与锁脚本刻意保持常规写法：共用同一个 Redis 的另一个进程应该看不出
任何私有约定。

## 状态

| 能力 | 状态 |
|---|---|
| `Cache` 的 `Get`/`Set`/`Delete`/`GetOrLoad`、有界 L1、值复制 | 完成 |
| TTL 收敛、jitter、负缓存、超大值拒绝 | 完成 |
| `SetPersistent`（零 TTL） | memory 与 Redis 完成；Olric 改用其文档化的默认 TTL |
| 同 key 加载合并，每个等待者可独立取消 | 完成 |
| 每次加载的超时（`LoaderTimeout`） | 完成 |
| 后台过期清扫（`SweepInterval`） | 完成 |
| `stale-if-error`（有界窗口，返回条目标记 `Stale`） | 完成 |
| 失效端口 + `InvalidationError`（已生效但未传播） | 完成 |
| `Lock`/`Locker`/`WaitPolicy` 端口 | 完成 |
| `memory/` 存储与总线（测试、单进程） | 完成 |
| Redis `RemoteStore`/`Invalidator` | 完成（miniredis 测试） |
| Redis `Locker`：脚本路径 + 原生 `DELEX`/`SET IFEQ` 路径 | 完成；脚本路径有测试，8.4+ 原生路径已实现但测试套件未覆盖（miniredis 没有 `DELEX`） |
| Olric `RemoteStore`（内嵌集群作为 L2） | 完成，已用真实节点验证（`-tags=integration`） |
| 可选 key 枚举（`Scanner`） | Redis 完成；Olric 刻意不实现（见下文） |
| 健康检查（`Health`、存储 `Ping`） | 仅 Redis 存储；`memory` 与 `olric` 未实现 `Ping`，对它们 `Health` 是空操作 |
| 指标计数器 | 除 `Metrics.LoadWaiters`（声明了但从不递增）外完成 |

### 未完成或部分完成

- **带版本的条目。** `Entry.Version` 已贯通本地层与失效路径，但随包发布的存储都不产生
  version，因此当前的一致性只有 TTL + 失效广播，没有"版本不旧才回填"。
- **`Config.InvalidationBuffer`。** 该字段会被规范化，但没有传给任何随包发布的失效实现：
  订阅队列属于实现本身（`memory.NewInvalidator` 用自己的缓冲参数，Redis pub/sub 用客户端的
  channel）。
- **Redis 8.4+ 锁路径。** 已实现，通过探测 `COMMAND INFO delex` 选择，但没有针对具备
  `DELEX` 的服务器的测试。
- **Olric 只是存储。** Olric 层没有 `Invalidator`、`Locker`、`Scanner`，也没有 `Ping`。
- **基准测试与 v1.0 API 稳定化**仍未完成，见 [ROADMAP.zh-CN.md](ROADMAP.zh-CN.md)。

## 扫描共享层

`RemoteStore` 是按 key 寻址的。枚举是一个独立、可选的端口，因此存储可以保持按 key 寻址，
核心也不被迫依赖某种扫描能力：

```go
type Scanner interface {
	Scan(ctx context.Context, prefix string, fn func(key string) error) error
}
```

存储不必实现它——缓存本身从不枚举——消费者用类型断言请求该能力并处理拒绝：

```go
scanner, ok := store.(gcache.Scanner)
if !ok {
	return errors.New("this store cannot enumerate its keys")
}
```

`redis.Store` 用 `SCAN` + `MATCH <prefix>*` + `COUNT`（每轮 512 个 key）实现。回调按实现
未承诺的顺序对每个匹配 key 调用一次；回调的错误会停止扫描并原样返回；Redis 故障报告为
`ErrUnavailable`，绝不伪装成空 key 列表。`olric.Store` 刻意不实现：Olric 的遍历会走遍集群
每个分区并用正则匹配每个 key，不是廉价的前缀查询，伪装成廉价查询等于隐藏这份开销。见
[scanner.go](scanner.go)。

## 不用 Lua 的加锁

`SET key token NX PX ttl` 在所有 Redis 版本上都是原子的，所以获取锁从来不需要脚本。释放与
续期必须**先比对 token**：

| Redis | 释放 | 续期 |
|---|---|---|
| < 8.4 | `EVAL if get==token then del` | `EVAL if get==token then pexpire` |
| **8.4+** | `DELEX key IFEQ <token>` | `SET key token IFEQ <token> PX <ttl>` |

`Locker` 探测一次 `COMMAND INFO delex` 后选择路径，因此同一个二进制在两类服务器上都能工作。
注意 Redis 7 的 *Functions*（`FUNCTION LOAD`）仍然是 Lua——它只改变脚本的管理方式，不改变
是否执行 Lua。Valkey 的 `DELIFEQ` 与 Tair 的 `CAS`/`CAD` 不是 `DELEX`，仍走脚本路径。

实现遵守的规则：

- 不存在的条目是 `(entry, false, nil)`，不是错误；存储故障是错误，绝不伪装成"不存在"；
- loader 失败不进缓存；只有显式的 `ErrNotFound` 会被短暂缓存；
- `Set`/`Delete` 先写 L2，再写 L1，最后广播；广播失败返回
  `InvalidationError{Applied: true}`——写入已经生效，其他节点可能在其 TTL 到期前仍是旧值；
- 值在进出两侧都会复制，调用方无法修改缓存中的缓冲区；
- 失效消息只携带 key 与 version，绝不携带值；
- 扫描是分页的，扫描失败是错误，绝不是空 key 列表。

## 安装

```bash
go get github.com/maxid/gcache
```

## 许可证

MIT。见 [LICENSE](LICENSE)。
