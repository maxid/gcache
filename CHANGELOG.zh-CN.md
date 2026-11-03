[English](CHANGELOG.md) | 简体中文

# 变更日志

本文件记录本项目的重要变更。

## [Unreleased]

- `Scanner`：可选的 key 枚举端口（`Scan(ctx, prefix, fn)`），随包提供的两个实现都有：
  Redis 用 `SCAN` + `MATCH <prefix>*` + `COUNT`，Olric 用分页的 DMap 遍历（`Match` + `Count`，
  每轮 512 个 key）。回调的错误会停止扫描并原样返回；存储故障包装 `ErrUnavailable`，绝不
  伪装成空 key 列表。**破坏性变更**：`olric.DMap` 新增 `Scan`，本仓库之外对该导出端口的实现
  在此之后不再满足它；`olric.Store` 现在也实现 `gcache.Scanner`。
- 更正状态文档：本库已实现并有测试覆盖，不是脚手架。仍存在的缺口列在 README.zh-CN.md。

## [0.0.0-dev]

- 初始实现：两级缓存、memory/Redis/Olric 共享层、失效广播、分布式锁、负缓存、
  stale-if-error、后台清扫与健康检查。
- `SetPersistent` 写入不带默认 TTL 的值；内存与 Redis remote 把 0 视为不过期。
