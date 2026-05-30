# LoomQ 文档

本文档库对应当前主线版本的 LoomQ。这里优先放开发、部署和运维时最常用的入口。

## 优先阅读

1. [核心模型](architecture/core-model.md) - Intent、状态机、语义边界
2. [配置说明](operations/CONFIGURATION.md) - 配置项与环境变量
3. [架构说明](development/ARCHITECTURE.md) - 模块分层、调度器和 Raft 结构
4. [API 文档](development/API.md) - REST API 与错误返回

---

## 快速导航

| 文档 | 说明 |
|------|------|
| [核心模型](architecture/core-model.md) | Intent、状态机、语义边界 |
| [投递模式选择](architecture/delivery-modes.md) | HTTP 单请求/批量/gRPC 流三种模式对比 |
| [架构说明](development/ARCHITECTURE.md) | 系统分层、调度器、Raft、快照与恢复 |
| [API 文档](development/API.md) | REST API 接口与错误响应 |
| [OpenAPI 规范](development/openapi.yaml) | OpenAPI 3.0 规范 |
| [配置说明](operations/CONFIGURATION.md) | 完整配置项与精度档位参数 |
| [指标文档](operations/metrics.md) | 指标、RTT、借用统计、Prometheus 导出 |
| [排障指南](operations/troubleshooting.md) | 常见问题与处理 |
| [部署指南](operations/DEPLOYMENT.md) | 生产部署、Docker、Kubernetes |

---

## 工程文档

| 文档 | 说明 |
|------|------|
| [发布清单](engineering/release-checklist.md) | 发版前检查清单 |
| [Benchmark 清单](engineering/benchmark-checklist.md) | 基准测试流程与数据采集 |
| [技术债清理](development/TECH_DEBT_CLEANUP.md) | 技术债务清理记录 |
| [OpenAPI 规范](development/openapi.yaml) | API 契约定义 |

---

## 项目规范

| 文档 | 说明 |
|------|------|
| [文档命名规范](NAMING_CONVENTION.md) | 文件命名约定 |

---

## 历史版本

| 版本 | 主要特性 |
|------|----------|
| [v0.6.1](archive/v0.6.1/) | WAL 优化、性能提升 |
| [v0.6](archive/v0.6/) | 精度档位调度 |
| [v0.5](archive/v0.5/) | Intent API、高可用 |
| [v0.4.8](archive/v0.4.8/) | 主从复制基础 |

---

## 相关链接

- [项目 README](../README.md) (English) / [中文](../README.zh.md)
- [更新日志](../CHANGELOG.md)
- [贡献指南](../CONTRIBUTING.md)
- [安全策略](../SECURITY.md)
- [License](../LICENSE)
