[English](README.md) | 简体中文

# gcache

Go 的两级缓存（本地 + Redis）与分布式锁工具库。

> 状态：**实验性脚手架**（v0.0.0-dev）。

## 计划中的特性

- 本地缓存层（LRU/TTL）
- Redis 二级缓存
- 跨节点失效（pub/sub）
- 分布式锁
- 缓存统计与健康检查

## 安装

```bash
go get github.com/maxid/gcache
```

## 许可证

MIT。见 [LICENSE](LICENSE)。
