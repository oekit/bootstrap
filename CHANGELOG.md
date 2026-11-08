# Changelog — oekit/bootstrap

所有 notable 变更记录如下。格式参考 [Keep a Changelog](https://keepachangelog.com/)。

## [v0.7.0] — 2026-11-09

- fix(app): closer panic 隔离——坏 Close 不再中断逆序停机链（#132）
- fix(bootstrapper): AddComponent 就地回填 ServiceNameAware（#131）
- feat(app): App.Fail 显式失败出口——ReadyFatal/Required 注册失败真正中止 Run（#133）
- fix(app): SetWaitTimeout(0)/AddResourceTimeout 与 shutdown 预算解耦（#134 #135）
- feat(nacos): Register 取消 best-effort 反注册 + PhaseAbandoned 阶段（#136）

关联问题单：bootstrap#131、bootstrap#132、bootstrap#133、bootstrap#134、bootstrap#135、bootstrap#136。

## [nacos/v0.3.20] — 2026-11-08

- build: 依赖地板对齐——本版把 oekit 依赖地板抬到各依赖当前最新已发布版本（matrix-tools/scripts/align_floors.py 维护轮）

## [obs/v0.2.22] — 2026-11-08

- build: 依赖地板对齐——本版把 oekit 依赖地板抬到各依赖当前最新已发布版本（matrix-tools/scripts/align_floors.py 维护轮）

## [v0.6.16] — 2026-11-08

- build: 依赖地板对齐——本版把 oekit 依赖地板抬到各依赖当前最新已发布版本（matrix-tools/scripts/align_floors.py 维护轮）

## [obs/v0.2.21] — 2026-11-07

- build: 父模块地板对齐（嵌套 go.mod 的父仓 require 跟随最新，audit 门禁项）

## [nacos/v0.3.19] — 2026-11-07

- build: 父模块地板对齐（嵌套 go.mod 的父仓 require 跟随最新，audit 门禁项）

## [obs/v0.2.20] — 2026-11-07

- fix(worker): AddTicker 改完成驱动节拍 + CloseTimeout——慢任务不再背靠背补跑，Close 可超时放弃等待（#98）
- fix(worker): AddTicker 入口校验 interval>0（ErrInvalidInterval）——零/负值不再使周期任务 panic 后静默死亡（#101）
- fix(worker): AddTicker/AddOnce 在 Close 后返回错误（ErrManagerClosed）——静默吞注册关闭（#103）
- fix(app): closeAll 改「锁内拷贝、锁外执行」——closer 内回调 AddResource/SetWaitTimeout 不再死锁（#102）
- fix(app): close 阶段收敛循环——关停竞态窗口注册的 closer 也被执行，closeAll 期间 AddResource 反注册不再漏调（#99）
- fix(app): GoBackground 的 wg.Add 与 shutdown 期 wg.Wait 同锁全序化——WaitGroup 规则违例窗口关闭（#104）
- feat(app): AddResourceTimeout——逐资源关停预算（预算隔离 + 父 ctx 取消传播），Manager 用户不再手写适配层（#75）
- test(nacos): NNSComponent.ReadyTimeout 字段优先级回归——组件字段压过进程级全局（#100）
- feat(healthz): Degraded(err) 三态语义对齐 hertz/health——软依赖降级不触发 503、Result.Degraded 字段；与 hertz/health 的规范与差异文档化（oekit#140）
- fix(nacos): Required=true 注册失败 RequestShutdown——强依赖 fail-fast 贯穿注册面（nns#77）
- fix(obs): 空 Endpoint 也装本地 provider+全局传播器——obs.WithTracing 与直接 tracing.Init 语义对齐（tracing#60）

关联问题单：bootstrap#75、bootstrap#98、bootstrap#99、bootstrap#100、bootstrap#101、bootstrap#102、bootstrap#103、bootstrap#104。

## [nacos/v0.3.18] — 2026-11-07

- fix(worker): AddTicker 改完成驱动节拍 + CloseTimeout——慢任务不再背靠背补跑，Close 可超时放弃等待（#98）
- fix(worker): AddTicker 入口校验 interval>0（ErrInvalidInterval）——零/负值不再使周期任务 panic 后静默死亡（#101）
- fix(worker): AddTicker/AddOnce 在 Close 后返回错误（ErrManagerClosed）——静默吞注册关闭（#103）
- fix(app): closeAll 改「锁内拷贝、锁外执行」——closer 内回调 AddResource/SetWaitTimeout 不再死锁（#102）
- fix(app): close 阶段收敛循环——关停竞态窗口注册的 closer 也被执行，closeAll 期间 AddResource 反注册不再漏调（#99）
- fix(app): GoBackground 的 wg.Add 与 shutdown 期 wg.Wait 同锁全序化——WaitGroup 规则违例窗口关闭（#104）
- feat(app): AddResourceTimeout——逐资源关停预算（预算隔离 + 父 ctx 取消传播），Manager 用户不再手写适配层（#75）
- test(nacos): NNSComponent.ReadyTimeout 字段优先级回归——组件字段压过进程级全局（#100）
- feat(healthz): Degraded(err) 三态语义对齐 hertz/health——软依赖降级不触发 503、Result.Degraded 字段；与 hertz/health 的规范与差异文档化（oekit#140）
- fix(nacos): Required=true 注册失败 RequestShutdown——强依赖 fail-fast 贯穿注册面（nns#77）
- fix(obs): 空 Endpoint 也装本地 provider+全局传播器——obs.WithTracing 与直接 tracing.Init 语义对齐（tracing#60）

关联问题单：bootstrap#75、bootstrap#98、bootstrap#99、bootstrap#100、bootstrap#101、bootstrap#102、bootstrap#103、bootstrap#104。

## [v0.6.15] — 2026-11-07

- fix(worker): AddTicker 改完成驱动节拍 + CloseTimeout——慢任务不再背靠背补跑，Close 可超时放弃等待（#98）
- fix(worker): AddTicker 入口校验 interval>0（ErrInvalidInterval）——零/负值不再使周期任务 panic 后静默死亡（#101）
- fix(worker): AddTicker/AddOnce 在 Close 后返回错误（ErrManagerClosed）——静默吞注册关闭（#103）
- fix(app): closeAll 改「锁内拷贝、锁外执行」——closer 内回调 AddResource/SetWaitTimeout 不再死锁（#102）
- fix(app): close 阶段收敛循环——关停竞态窗口注册的 closer 也被执行，closeAll 期间 AddResource 反注册不再漏调（#99）
- fix(app): GoBackground 的 wg.Add 与 shutdown 期 wg.Wait 同锁全序化——WaitGroup 规则违例窗口关闭（#104）
- feat(app): AddResourceTimeout——逐资源关停预算（预算隔离 + 父 ctx 取消传播），Manager 用户不再手写适配层（#75）
- test(nacos): NNSComponent.ReadyTimeout 字段优先级回归——组件字段压过进程级全局（#100）
- feat(healthz): Degraded(err) 三态语义对齐 hertz/health——软依赖降级不触发 503、Result.Degraded 字段；与 hertz/health 的规范与差异文档化（oekit#140）
- fix(nacos): Required=true 注册失败 RequestShutdown——强依赖 fail-fast 贯穿注册面（nns#77）
- fix(obs): 空 Endpoint 也装本地 provider+全局传播器——obs.WithTracing 与直接 tracing.Init 语义对齐（tracing#60）

关联问题单：bootstrap#75、bootstrap#98、bootstrap#99、bootstrap#100、bootstrap#101、bootstrap#102、bootstrap#103、bootstrap#104。

## [obs/v0.2.19] — 2026-11-06

- build: 矩阵依赖地板对齐（第二轮发版后置·终圈）

## [nacos/v0.3.17] — 2026-11-06

- build: 矩阵依赖地板对齐（第二轮发版后置·终圈）

## [v0.6.14] — 2026-11-06

- build: 矩阵依赖地板对齐（第二轮发版后置·终圈）

## [obs/v0.2.18] — 2026-11-06

- build: 矩阵依赖地板对齐（第二轮发版后置·收敛圈）

## [nacos/v0.3.16] — 2026-11-06

- build: 矩阵依赖地板对齐（第二轮发版后置·收敛圈）

## [v0.6.13] — 2026-11-06

- build: 矩阵依赖地板对齐（第二轮发版后置·收敛圈）

## [obs/v0.2.17] — 2026-11-06

- build: 矩阵依赖地板对齐（第二轮发版后置）

## [nacos/v0.3.15] — 2026-11-06

- build: 矩阵依赖地板对齐（第二轮发版后置）

## [v0.6.12] — 2026-11-06

- build: 矩阵依赖地板对齐（第二轮发版后置）

## [v0.6.11] — 2026-11-06

- build: 矩阵依赖地板对齐（第二轮发版后置）

## [obs/v0.2.16] — 2026-11-06

- build: 矩阵依赖地板对齐（维护轮 #233）

## [nacos/v0.3.14] — 2026-11-06

- build: 矩阵依赖地板对齐（维护轮 #233）

## [obs/v0.2.15] — 2026-11-06

- build: 矩阵依赖地板对齐（维护轮 #233）

## [nacos/v0.3.13] — 2026-11-06

- build: 矩阵依赖地板对齐（维护轮 #233）

## [v0.6.10] — 2026-11-06

- **BREAKING** fix(app): 关停错误上抛——资源关闭失败时退出码不再为 0（[#80](https://github.com/oekit/bootstrap/issues/80)）——`closeAll` 对 closer 错误只 `logger.Error` 后继续，错误进不了 lifecycle.Manager 的 shutdownErrs；`Run` 只返回 runErr——资源关闭失败（DB flush 未落盘、连接未优雅断开）时**进程退出码仍为 0**，滚动发版门禁 / K8s preStop 对账把「带着未落盘数据退出」判成成功，数据丢失在部署链路上不可见。修法：closeAll 聚合错误（带资源名，errors.Is 可辨）→ app-lifecycle:close job 返回→ shutdownErrs → Run 末尾上抛（runErr 优先，关停错误 join 附加）。**升级可见**：依赖『rr==nil 即干净关停」的消费方（如欠面把 AddResource 错误当告警的场景）退出码会变——这正是本修复的目的；需旧行为请在 closer 内部自行容错
- docs(nacos): `WithReadyTimeout` 作用域按实现修正为进程级全局（[#76](https://github.com/oekit/bootstrap/issues/76)）——doc 曾宣称「须在 WithNNS 之前应用、仅作用于同一 Bootstrapper」，实现是包级 atomic（顺序无关、后写覆盖先写），同文件两处注释互相矛盾。按真实行为修正（进程全局、任何 Bootstrapper 的 NNS 组件 Init 时读取）；多 Bootstrapper 场景需 per-作用域时用组件字段 NNSComponent.ReadyTimeout。行为零改变，只修文档（与 #74 的 WithReadyFatal 全局选项同一款式）

关联问题单：bootstrap#76、bootstrap#80。

## [v0.6.9] — 2026-11-06

- build: 矩阵依赖地板对齐（维护轮 #233）

## [obs/v0.2.14] — 2026-11-03

- build: 把父仓 bootstrap v0.6.8 与 logx v0.8.2、registry v0.5.3、registry/nacos v0.1.3、registry/nacosimpl v0.1.4 的地板带进已发布产物（bootstrap/nacos、bootstrap/obs 原快照嵌的是更旧的父仓与跨仓地板，消费方拿不到最新内容）（bootstrap#70）

关联问题单：bootstrap#70。

## [nacos/v0.3.10] — 2026-11-03

- build: 把父仓 bootstrap v0.6.8 与 logx v0.8.2、registry v0.5.3、registry/nacos v0.1.3、registry/nacosimpl v0.1.4 的地板带进已发布产物（bootstrap/nacos、bootstrap/obs 原快照嵌的是更旧的父仓与跨仓地板，消费方拿不到最新内容）（bootstrap#70）

关联问题单：bootstrap#70。

## [obs/v0.2.13] — 2026-11-02

- 抬嵌套模块地板 github.com/oekit/registry/nacosimpl v0.1.0 → v0.1.1（oekit/oekit#104）

## [nacos/v0.3.9] — 2026-11-02

- 抬嵌套模块地板 github.com/oekit/registry/nacosimpl v0.1.0 → v0.1.1（oekit/oekit#104）

## [v0.6.8] — 2026-11-02

- 抬嵌套模块地板 github.com/oekit/registry/nacosimpl v0.1.0 → v0.1.1（oekit/oekit#104）

## [obs/v0.2.12] — 2026-11-01

- build: 矩阵内依赖地板 `nns` v0.5.4 → v0.5.5（嵌套模块 nacos 跟随；本版不含 API 变更）

## [nacos/v0.3.8] — 2026-11-01

- build: 矩阵内依赖地板 `nns` v0.5.4 → v0.5.5（嵌套模块 nacos 跟随；本版不含 API 变更）

## [v0.6.7] — 2026-11-01

- build: 矩阵内依赖地板 `nns` v0.5.4 → v0.5.5（嵌套模块 nacos 跟随；本版不含 API 变更）

## [obs/v0.2.11] — 2026-10-30

- build: 矩阵内依赖地板 `tracing` v0.3.1 → v0.3.2（嵌套模块 obs 跟随；本版不含 API 变更）

## [v0.6.6] — 2026-10-30

- build: 矩阵内依赖地板 `tracing` v0.3.1 → v0.3.2（嵌套模块 obs 跟随；本版不含 API 变更）

## [nacos/v0.3.7] — 2026-10-24

- build: 父模块依赖地板 v0.6.4 → v0.6.5（跟随 bootstrap v0.6.5；父模块地板不得落后最新 tag）

## [obs/v0.2.10] — 2026-10-24

- build: 父模块依赖地板 v0.6.4 → v0.6.5（跟随 bootstrap v0.6.5；父模块地板不得落后最新 tag）

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
- `NewApp(mgr)` — attach 模式（不接管信号/server，与宿主协作）（#1）
- `ServiceNameAware` / `AddComponent` — 组件动态注入
- `healthz` — 多 check 并发探测、总超时、panic 隔离
- `nacos.WithNNS` / `obs.WithMetrics` — 嵌套 module 函数选项

### 迁移（从旧路径）

| 旧 | 新 |
|---|---|
| `github.com/oekit/bootstrap/bootstrap` | `github.com/oekit/bootstrap` |
| `github.com/oekit/bootstrap/bootstrap/healthz` | `github.com/oekit/bootstrap/healthz` |
