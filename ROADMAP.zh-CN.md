[English](ROADMAP.md) | 简体中文

# 路线图

- [x] 本地缓存层（有界 LRU + TTL、stale-if-error 窗口）
- [x] Redis 层（`RemoteStore`、`Invalidator`、`Locker`）
- [x] 失效广播（Redis pub/sub 与进程内总线）
- [x] 分布式锁（`Locker`/`Lock`，比对 token 的释放与续期）
- [x] 前缀扫描端口（`Scanner`）及其 Redis 实现
- [ ] 基准测试
- [ ] v1.0 API 稳定化
