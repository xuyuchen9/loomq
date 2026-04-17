# LoomQ 文档命名规范

## 基本原则

1. **小写字母**：文件名全部使用小写
2. **连字符连接**：单词间使用连字符 `-`
3. **无版本前缀**：版本号体现在文件夹路径，不在文件名中
4. **清晰简洁**：文件名能直观表达内容

## 标准文件类型

每个版本文件夹应包含以下标准文件（如适用）：

| 文件名 | 用途 | 必需 |
|--------|------|------|
| `roadmap.md` | 版本路线图/规划 | 是 |
| `design.md` | 核心设计文档 | 是 |
| `architecture.md` | 架构详细说明 | 可选 |
| `implementation.md` | 实现报告/总结 | 是 |
| `api.md` | API 接口文档 | 可选 |
| `benchmark.md` | 性能基准测试报告 | 是 |
| `benchmark-stress.md` | 压力/极限测试报告 | 可选 |
| `review.md` | 技术评审报告 | 可选 |
| `changelog.md` | 变更日志 | 可选 |

## 命名示例

```
docs/
├── v0.3/
│   ├── roadmap.md
│   ├── design.md
│   ├── architecture.md
│   ├── implementation.md
│   ├── benchmark.md
│   ├── benchmark-stress.md
│   └── review.md
├── v0.4/
│   ├── roadmap.md
│   ├── design.md
│   ├── implementation.md
│   ├── benchmark.md
│   └── review.md
└── v0.5/
    ├── roadmap.md
    ├── design.md
    ├── implementation.md
    └── benchmark.md
```

## 特殊文件

- `README.md` - 文档索引（仅存在于 docs 根目录）
- `NAMING_CONVENTION.md` - 本命名规范
- 通用文档（如 `ARCHITECTURE.md`）保留在根目录，不涉及版本
