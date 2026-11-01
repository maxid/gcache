[English](CHANGELOG.md) | 简体中文

# 变更日志

本文件记录本项目的重要变更。

## [Unreleased]

- `Scanner`：可选的 key 枚举端口（`Scan(ctx, prefix, fn)`）及其 Redis 实现：
  `SCAN` + `MATCH <prefix>*` + `COUNT`，回调返回错误即停止扫描，存储故障包装
  `ErrUnavailable`。Olric 刻意不实现（DMap 遍历是全分区扫描，不是前缀查询）。
- 更正状态文档：本库已实现并有测试覆盖，不是脚手架。仍存在的缺口列在 README.zh-CN.md。

## [0.0.0-dev]

- 初始实现：两级缓存、memory/Redis/Olric 共享层、失效广播、分布式锁、负缓存、
  stale-if-error、后台清扫与健康检查。
- `SetPersistent` 写入不带默认 TTL 的值；内存与 Redis remote 把 0 视为不过期。
