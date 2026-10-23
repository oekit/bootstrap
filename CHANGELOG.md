# Changelog — oekit/bootstrap

所有 notable 变更记录如下。格式参考 [Keep a Changelog](https://keepachangelog.com/)。

## [nacos/v0.3.6] — 2026-10-24

- feat(nacos): 就绪门漏接的可观测性——看门狗超时打 ERROR 点明「服务不会注册到注册中心」（WithReadyTimeout 可调）+ NNSComponent.RegistrationState() 暴露注册阶段（#33）

## [v0.6.5] — 2026-10-24

- feat(nacos): 就绪门漏接的可观测性——看门狗超时打 ERROR 点明「服务不会注册到注册中心」（WithReadyTimeout 可调）+ NNSComponent.RegistrationState() 暴露注册阶段（#33）

## [nacos/v0.3.5] — 2026-10-20

- build: 父模块依赖地板 v0.6.3 → v0.6.4（跟随 bootstrap v0.6.4；父模块地板不得落后最新 tag）

## [obs/v0.2.9] — 2026-10-20

- build: 父模块依赖地板 v0.6.3 → v0.6.4（跟随 bootstrap v0.6.4；父模块地板不得落后最新 tag）

## [nacos/v0.3.4] — 2026-10-20

- Go 最低版本地板 1.25.0 → 1.24.0（#33）：12 个 indirect pin 同步降级到 nns/registry 已在用的 ≤1.24 组合（otel v1.41.0、grpc v1.80.0、client_golang v1.23.2、x/crypto v0.48.0 等）；实测 tidy 保持 1.24.0（未被依赖顶回），真实 go1.24.0 工具链下 build+vet+test 全绿

## [v0.6.4] — 2026-10-20

- Go 最低版本地板 1.25.0 → 1.24.0（#33）：12 个 indirect pin 同步降级到 nns/registry 已在用的 ≤1.24 组合（otel v1.41.0、grpc v1.80.0、client_golang v1.23.2、x/crypto v0.48.0 等）；实测 tidy 保持 1.24.0（未被依赖顶回），真实 go1.24.0 工具链下 build+vet+test 全绿

## [obs/v0.2.8] — 2026-10-19

- build: 跟随 bootstrap v0.6.3 发版（父模块地板 v0.6.2 → v0.6.3）

## [nacos/v0.3.3] — 2026-10-19

- build: 跟随 bootstrap v0.6.3 发版（父模块地板 v0.6.2 → v0.6.3）

## [v0.6.3] — 2026-10-19

- build: 矩阵内依赖地板对齐（align_floors 不动点）：concurrency v0.5.4 → v0.17.0、logx v0.6.0 → v0.7.0

## [v0.6.2] — 2026-10-18

- docs(healthz): 包文档示例签名修正——Handle(ctx) 传参 + c.Data 直写（[#4](https://github.com/oekit/bootstrap/issues/4)：RequestContext 恰好实现 context.Context 传 c 能编译但超时语义错；c.JSON 会双重编码 body）

## [v0.6.1] — 2026-10-18

- fix(deps): logx 地板 v0.5.0 → v0.6.0（零代码改动）；嵌套 nacos/obs 同步

## [nacos/v0.3.2] — 2026-10-18

- chore(deps): 父模块地板 bootstrap v0.6.1 → v0.6.2（audit 嵌套地板门禁首发抓漏，matrix-tools#6）

## [obs/v0.2.7] — 2026-10-18

- chore(deps): 父模块地板 bootstrap v0.6.1 → v0.6.2（audit 嵌套地板门禁首发抓漏，matrix-tools#6）

## [nacos/v0.3.1] — 2026-10-18

- chore(deps): 全部 oekit 依赖地板对齐最新

## [obs/v0.2.6] — 2026-10-18

- chore(deps): 全部 oekit 依赖地板对齐最新

## [nacos/v0.3.0] — 2026-10-17

- **feat!**：`WithNNS` 注册延迟到 `App.Ready()` 之后——修复「注册先于 server 监听、反注册后于 server 关闭」双序错误，关停序 nns-deregister → server.Shutdown 对齐 kratos Stop

## [v0.6.0] — 2026-10-17

### Added
- `App.Ready()` / `App.ReadyChan()` / `App.IsReady()` 就绪门：配合 bootstrap/nacos v0.3.0 延迟注册，
  服务须在 server 就绪后调用 `app.Ready()` 才会进入注册中心（推荐经 hertz/kitex `RunServer`
  的 `WithOnReady` 触发）。幂等、并发安全

## [v0.5.4] — 2026-10-17

- **feat(bootstrapper)**：`WithBeforeServe(fn func() error)`——组件初始化后、
  userFn 前的启动钩子（失败中止启动）。覆盖零断点重启的 listener fd 收养 /
  pid 文件交接等「必须在引擎 serve 之前」的场景
  （[#3](https://github.com/oekit/bootstrap/issues/3)）

## [obs/v0.2.5] — 2026-10-17

- bootstrap v0.5.3 地板跟随

## [nacos/v0.2.5] — 2026-10-17

- bootstrap v0.5.3、nns v0.3.5 地板跟随

## [obs/v0.2.4] — 2026-10-17

- concurrency 地板 v0.5.4

## [nacos/v0.2.4] — 2026-10-17

- concurrency 地板 v0.5.4

## [v0.5.3] — 2026-10-17

- concurrency 地板 v0.5.2 → v0.5.4；嵌套 nacos/obs 同步

## [obs/v0.2.3] — 2026-10-17

- fix(deps): core 转直接依赖（obs.go 迁移 core/lifecycle 后的 tidy 归位，CI tidy 门禁）；bootstrap 地板 v0.5.2

## [nacos/v0.2.3] — 2026-10-17

- fix(deps): bootstrap 地板 v0.5.1 → v0.5.2（lifecycle 迁移版）

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
