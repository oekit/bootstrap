# Changelog — oekit/bootstrap/nacos

由 oekit/bootstrap v0.3.0 拆分而来。

## [v0.3.15] — 2026-11-06

- 跟随 bootstrap v0.6.12 发版（release_train 嵌套 train）

## [v0.3.14] — 2026-11-06

- 随本次 train 一并发布（只发嵌套模块，父模块本次不发版）

## [v0.3.13] — 2026-11-06

- 跟随 bootstrap v0.6.9 发版（release_train 嵌套 train）

## [v0.3.12] — 2026-11-06

- 跟随补记发版（本版无代码变更）：v0.3.11 的 CHANGELOG 段因发版流程事故未随 tag 快照落盘（tag 快照缺本版段，matrix-tools#234 新门禁当场判出）；v0.3.12 快照起恢复「tag 必须落在 CHANGELOG 提交之后」的纪律，内容见下一条

关联问题单：bootstrap#74、matrix-tools#234。

## [v0.3.11] — 2026-11-06

- feat(nacos): 新增 `WithReadyFatal()` 就绪门看门狗 fail-fast（[bootstrap#74](https://github.com/oekit/bootstrap/issues/74)）——`WithNNS` 的服务注册延迟到 `app.Ready()` 之后；裸 `h.Spin()` 起服务（不经 hertz/kitex RunServer）的消费方**永远不会触发 Ready**——服务静默永不注册，唯一信号是 30s 看门狗一条 ERROR（bootstrap#33），之后服务照常运行——「注册缺失」是**带日志的静默降级**，排查时不会先想到去看启动日志里一条 30 秒前的 ERROR（下游活案例：bianque main.go 用 WithNNS 但裸 Spin，全仓非测试代码零 app.Ready() 调用）。修法：`WithReadyFatal()`（全局选项，与 WithReadyTimeout 同款「选项写、Init 读」包级 atomic）或组件字段 `NNSComponent.ReadyFatal`——看门狗超时后 RegistrationState 落 PhaseFailed + `errReadyFatal` 哨兵（errors.Is 可辨，错误文案直接点名裸 Spin 消费方的两条出路）并 `app.RequestShutdown()` 中止启动——注册中心缺失在集群部署下等于服务不存在，早死优于静默。缺省 false：告警后继续等就绪、慢启动不误杀（#33 语义零破坏）——本地开发/注册为软增强的服务不受影响

关联问题单：bootstrap#74。

## [v0.3.10] — 2026-11-03

- 随本次 train 一并发布（只发嵌套模块，父模块本次不发版）（bootstrap#70）

关联问题单：bootstrap#70。

## [v0.3.9] — 2026-11-02

- 跟随 bootstrap v0.6.8 发版（release_train 嵌套 train）

## [v0.3.8] — 2026-11-01

- 跟随 bootstrap v0.6.7 发版（release_train 嵌套 train）

## [v0.3.7] — 2026-10-24

- 跟随 bootstrap  发版（release_train 嵌套 train）
- 补记：nacos 就绪门漏接的可观测性（看门狗超时告警 + RegistrationState）已在 nacos/v0.3.6 节首发，本版未再改动（#33）

## [v0.3.6] — 2026-10-24

- 跟随 bootstrap v0.6.5 发版（release_train 嵌套 train）
- feat(nacos): 就绪门漏接的可观测性（#33）——看门狗超时打 ERROR 点明「服务不会注册到注册中心」（WithReadyTimeout 可调）+ NNSComponent.RegistrationState() 暴露注册阶段

## [v0.3.5] — 2026-10-20

- build: 父模块依赖地板 v0.6.3 → v0.6.4（跟随 bootstrap v0.6.4）

## [v0.3.4] — 2026-10-20

- 跟随 bootstrap v0.6.4 发版（release_train 嵌套 train）

## [v0.3.3] — 2026-10-19

- build: 跟随 bootstrap v0.6.3 发版（矩阵依赖地板对齐轮；父模块地板 v0.6.2 → v0.6.3）

## [nacos/v0.3.0] — 2026-10-17

### Breaking
- **注册时机变更**：`WithNNS`/`WithRequiredNNS` 的服务注册由组件 Init 阶段延迟到
  `app.Ready()` 之后（bootstrap v0.6.0 就绪门）。调用方必须在 server 就绪后触发
  `app.Ready()`（推荐经 hertz/kitex `RunServer` 的 `WithOnReady`），否则服务不会
  进入注册中心；Ready 永不触发时不产生任何注册
- **停机序修正**：反注册改在注册成功后经 `app.AddResource` 挂载——close 阶段按注册
  逆序释放，关停序为 nns-deregister → server.Shutdown（对齐 go-kratos Stop 语义；
  旧实现 server 已关闭后才反注册）
- 依赖升级：nns v0.4.0 / registry v0.4.0（双仓包提升至模块根）

## [v0.1.0] — 2026-10-15

- 首版：自 bootstrap 根包迁入（行为不变）
