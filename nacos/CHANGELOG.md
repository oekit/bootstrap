# Changelog — oekit/bootstrap/nacos

由 oekit/bootstrap v0.3.0 拆分而来。

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
