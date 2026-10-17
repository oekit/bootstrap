// Package bootstrap 提供 oekit 微服务的启动编排——纯编排壳，零重依赖。
//
// 职责：App 生命周期（ctx/资源注册/后台任务）、组件化 Bootstrapper、
// 信号监听与优雅关闭、WorkerManager。
//
// # 组件化接入
//
// 重量级能力以嵌套 module 形式提供，经 BootstrapperOption 函数注入：
//
//	import (
//	    "github.com/oekit/bootstrap"
//	    "github.com/oekit/bootstrap/nacos" // Nacos/NNS 注册（按需）
//	    "github.com/oekit/bootstrap/obs"   // Prometheus/OTel（按需）
//	)
//
//	b, _ := bootstrap.New(
//	    bootstrap.WithServiceName("my-service"),
//	    nacos.WithNNS(cfg.Nacos, cfg.Port, cfg.DepServices),
//	    obs.WithMetrics(),
//	    obs.WithTracing(tracing.Config{Endpoint: "otel:4317"}),
//	)
//	_ = b.Run(userFn)
//
// 第三方集成实现 Component 接口后经 WithComponent 注入即可。
//
// # 多 Server 场景
//
// 启动多个 server，通过 AddResource 注册关闭函数（逆序执行）：
//
//	app.AddResource("hertz", func(ctx context.Context) error { return h.Shutdown(ctx) })
//
// # BeforeServe（引擎启动前）
//
// WithBeforeServe(fn) 在组件初始化后、userFn 之前执行；失败中止启动。
// 用于 listener fd 收养、pid 交接等「必须在 serve 前」的动作。bootstrap
// 不接管 listener——自备时用 server.WithListener(ln) 即可。
//
// # attach 模式（NewApp）
//
// bootstrap.NewApp(mgr *shutdown.Manager) 是 **attach 模式**入口：只把
// app-lifecycle 四阶段挂到调用方的 shutdown.Manager 上，**不接管信号与
// server**——不调用 signal.Notify、不调用 h.Spin，SIGINT/SIGTERM 与服务
// 启停由宿主负责。适合宿主已有 Spin/OnShutdown 生命周期（如 hertz），
// 只想补上「ctx 取消 + 资源关闭 + 后台任务 drain」的场景：
//
//	teardown := shutdown.New(30 * time.Second)
//	app := bootstrap.NewApp(teardown)
//	app.GoBackground("outbox", outbox.Run)
//	app.AddResource("redis", func(ctx context.Context) error { return rdb.Close() })
//	h.OnShutdown = append(h.OnShutdown, func(ctx context.Context) {
//	    teardown.Shutdown(ctx)
//	})
//	h.Spin()
//
// 四阶段 job 名为 app-lifecycle:{cancel,drain,close,wait}，按注册顺序在
// mgr.Shutdown 时执行；caller 自己 mgr.Add 的任务同样按注册顺序插入：
// NewApp 之前 Add 的排在四阶段前，之后 Add 的排在四阶段后。AddResource
// 的 closer 在 close 阶段逆序释放；GoBackground 的 goroutine 在 wait 阶段
// 被等待退出。
package bootstrap
