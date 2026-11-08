# Changelog — oekit/bootstrap/obs

由 oekit/bootstrap v0.3.0 拆分而来。

## [v0.2.22] — 2026-11-08

- 随本次 train 一并发布（只发嵌套模块，父模块本次不发版）

## [v0.2.21] — 2026-11-07

- 随本次 train 一并发布（只发嵌套模块，父模块本次不发版）

## [v0.2.20] — 2026-11-07

- 跟随 bootstrap v0.6.15 发版（release_train 嵌套 train）（bootstrap#75、bootstrap#98、bootstrap#99、bootstrap#100、bootstrap#101、bootstrap#102、bootstrap#103、bootstrap#104）

关联问题单：bootstrap#75、bootstrap#98、bootstrap#99、bootstrap#100、bootstrap#101、bootstrap#102、bootstrap#103、bootstrap#104。

## [v0.2.19] — 2026-11-06

- 跟随 bootstrap v0.6.14 发版（release_train 嵌套 train）

## [v0.2.18] — 2026-11-06

- 跟随 bootstrap v0.6.13 发版（release_train 嵌套 train）

## [v0.2.17] — 2026-11-06

- 跟随 bootstrap v0.6.12 发版（release_train 嵌套 train）

## [v0.2.16] — 2026-11-06

- 随本次 train 一并发布（只发嵌套模块，父模块本次不发版）

## [v0.2.15] — 2026-11-06

- 跟随 bootstrap v0.6.9 发版（release_train 嵌套 train）

## [v0.2.14] — 2026-11-03

- 随本次 train 一并发布（只发嵌套模块，父模块本次不发版）（bootstrap#70）

关联问题单：bootstrap#70。

## [v0.2.13] — 2026-11-02

- 跟随 bootstrap v0.6.8 发版（release_train 嵌套 train）

## [v0.2.12] — 2026-11-01

- 跟随 bootstrap v0.6.7 发版（release_train 嵌套 train）

## [v0.2.11] — 2026-10-30

- 跟随 bootstrap v0.6.6 发版（release_train 嵌套 train）

## [v0.2.10] — 2026-10-24

- 跟随 bootstrap  发版（release_train 嵌套 train）

## [v0.2.9] — 2026-10-20

- build: 父模块依赖地板 v0.6.3 → v0.6.4（跟随 bootstrap v0.6.4）

## [v0.2.8] — 2026-10-19

- build: 跟随 bootstrap v0.6.3 发版（矩阵依赖地板对齐轮；父模块地板 v0.6.2 → v0.6.3）

## [v0.2.7] — 2026-10-18

- build: 嵌套地板 bootstrap v0.6.1 → v0.6.2（audit 嵌套地板门禁首发抓漏，matrix-tools#6）

关联问题单：matrix-tools#6。

## [v0.2.6] — 2026-10-18

- build: 全部 oekit 依赖地板对齐最新——bootstrap v0.6.1、core v0.7.0、logx v0.6.0、metrics v0.1.7、tracing v0.2.2

## [v0.2.5] — 2026-10-17

- build: bootstrap v0.5.3 地板跟随

## [v0.2.4] — 2026-10-17

- build: concurrency 地板 v0.5.4

## [v0.2.3] — 2026-10-17

- build: core 转直接依赖（obs.go 迁移 core/lifecycle 后的 tidy 归位，CI tidy 门禁）；bootstrap 地板 v0.5.2

## [v0.2.1] — 2026-10-17

- build: 嵌套地板全量对齐最新——bootstrap v0.5.1、logx v0.5.0、metrics v0.1.6、tracing v0.2.1；go 1.26.0（tracing/OTel 生态传导）

## [v0.2.0] — 2026-10-17

- 首版嵌套模块：随仓库结构重置（bootstrap 包提升至模块根）自根包拆出——module github.com/oekit/bootstrap/obs，行为与拆分前一致

## [v0.1.0] — 2026-10-15

- 首版：自 bootstrap 根包迁入（行为不变）
