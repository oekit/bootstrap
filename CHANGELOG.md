# Changelog — oekit/bootstrap

所有 notable 变更记录如下。格式参考 [Keep a Changelog](https://keepachangelog.com/)。

## [obs/v0.2.3] — 2026-10-17

- fix(deps): core 转直接依赖（obs.go 迁移 core/lifecycle 后的 tidy 归位，CI tidy 门禁）；bootstrap 地板 v0.5.2

## [nacos/v0.2.3] — 2026-10-17

- fix(deps): bootstrap 地板 v0.5.1 → v0.5.2（lifecycle 迁移版）

## [obs/v0.2.2] — 2026-10-17

- fix(lifecycle): obs.go 迁移 logx/shutdown → core/lifecycle

## [nacos/v0.2.2] — 2026-10-17

- fix(lifecycle): nns.go 迁移 logx/shutdown → core/lifecycle

## [v0.5.2] — 2026-10-17

- fix(lifecycle): 迁移 logx/shutdown（v0.5.0 起废弃）→ core/lifecycle.Manager——类型别名等价，仅 import 与构造符号替换；嵌套 nacos/obs 同步迁移；修复 CI staticcheck SA1019；go.mod 新增 core 直接依赖

## [obs/v0.2.1] — 2026-10-17

- fix(deps): 嵌套模块地板全量对齐最新——bootstrap v0.5.1、logx v0.5.0、metrics v0.1.6、tracing v0.2.1；go 维持 1.26.0（tracing/OTel 生态传导）

## [nacos/v0.2.1] — 2026-10-17

- fix(deps): 嵌套模块地板全量对齐最新——bootstrap v0.5.1、core v0.6.0、logx v0.5.0、nns v0.3.4、registry v0.3.1；go 维持 1.25.0（nacos-sdk 图要求）

## [v0.5.1] — 2026-10-17

- fix(deps): 内部依赖地板 concurrency v0.1.5 → v0.5.2、logx v0.1.9 → v0.5.0；go 最低版本降至 1.24.0（自然落点）

## [v0.5.0] — 2026-10-17

仓库结构重置后的首个版本（历史与旧版本号 v0.1.0–v0.4.2 已清空重做，从 v0.5.0 起算）。

### 目录结构

```
github.com/oekit/bootstrap                # package bootstrap（模块根）
├── healthz/                              # package healthz — 健康检查
├── nacos/                                # 嵌套 module github.com/oekit/bootstrap/nacos
└── obs/                                  # 嵌套 module github.com/oekit/bootstrap/obs
```

- **核心包提升到模块根**：import 从 `github.com/oekit/bootstrap/bootstrap`
  改为 `github.com/oekit/bootstrap`（包名仍为 `bootstrap`）
- **healthz 路径同步**：`github.com/oekit/bootstrap/bootstrap/healthz`
  → `github.com/oekit/bootstrap/healthz`
- 嵌套 module `nacos` / `obs` 依赖改为 `require github.com/oekit/bootstrap v0.5.0`
  + `replace => ../`（仓内联编；对外发布忽略 replace）

### 能力面（结构重置保留）

- `bootstrap.New(opts...)` / `Run(userFn)` — 应用骨架（App ctx + AddResource + GoBackground）
- `NewApp(mgr)` — attach 模式（不接管信号/server，与宿主协作）
- `ServiceNameAware` / `AddComponent` — 组件动态注入
- `healthz` — 多 check 并发探测、总超时、panic 隔离
- `nacos.WithNNS` / `obs.WithMetrics` — 嵌套 module 函数选项

### 迁移（从旧路径）

| 旧 | 新 |
|---|---|
| `github.com/oekit/bootstrap/bootstrap` | `github.com/oekit/bootstrap` |
| `github.com/oekit/bootstrap/bootstrap/healthz` | `github.com/oekit/bootstrap/healthz` |
