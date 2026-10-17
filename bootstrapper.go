package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/oekit/concurrency/async"
	"github.com/oekit/core/lifecycle"
	"github.com/oekit/logx"
)

var logger = logx.Delegating("bootstrap")

// Bootstrapper 是组件化应用编排器。
//
// 通过函数选项配置启动组件；重量级集成（Nacos/NNS、Prometheus/OTel）
// 位于嵌套 module，以 Option 函数形式注入，根包保持零重依赖：
//
//	b, err := bootstrap.New(
//	    bootstrap.WithServiceName("my-service"),
//	    nacos.WithNNS(nacosCfg, 8080, deps),   // github.com/oekit/bootstrap/nacos
//	    obs.WithMetrics(),                     // github.com/oekit/bootstrap/obs
//	    obs.WithTracing(tracing.Config{...}),
//	)
//	if err != nil { log.Fatal(err) }
//	b.Run(func(app *bootstrap.App) error {
//	    h := server.New(server.WithHostPorts(":8080"))
//	    app.AddResource("hertz", h.Shutdown)
//	    return h.Run()
//	})
//
// 本地开发和测试场景可不添加任何外部依赖组件。
type Bootstrapper struct {
	serviceName     string
	shutdownTimeout time.Duration
	components      []Component
}

// BootstrapperOption 配置 Bootstrapper。
type BootstrapperOption func(*Bootstrapper)

// WithServiceName 设置服务名称（必填）。
func WithServiceName(name string) BootstrapperOption {
	return func(b *Bootstrapper) { b.serviceName = name }
}

// WithShutdownTimeout 设置优雅关闭超时，默认 10 秒。
func WithShutdownTimeout(d time.Duration) BootstrapperOption {
	return func(b *Bootstrapper) { b.shutdownTimeout = d }
}

// WithComponent 添加自定义组件——第三方集成注入的标准入口。
//
// 实现 Component 接口的任意类型都可通过本方法接入启动编排；
// 嵌套 module（bootstrap/nacos、bootstrap/obs）的 WithXxx 选项
// 内部即委托此方法。
func WithComponent(c Component) BootstrapperOption {
	return func(b *Bootstrapper) { b.AddComponent(c) }
}

// AddComponent 追加启动组件（动态注入）。
// 导出以支持嵌套 module 与第三方实现注入，与 WithComponent 等价。
func (b *Bootstrapper) AddComponent(c Component) {
	b.components = append(b.components, c)
}

// WithShutdownHook 添加自定义关闭钩子。
func WithShutdownHook(name string, hook func(ctx context.Context) error) BootstrapperOption {
	return WithComponent(&ShutdownHookComponent{ComponentName: name, Hook: hook})
}

// ServiceNameAware 需要感知服务名的组件实现此接口；
// New() 在应用完全部选项后统一回填（与 WithServiceName 的调用顺序无关）。
// 嵌套 module 中的组件实现它即可参与回填，根包无需感知具体组件类型。
type ServiceNameAware interface {
	SetServiceName(name string)
}

// New 创建 Bootstrapper 实例。
func New(opts ...BootstrapperOption) (*Bootstrapper, error) {
	b := &Bootstrapper{
		shutdownTimeout: 10 * time.Second,
	}
	for _, opt := range opts {
		opt(b)
	}
	if b.serviceName == "" {
		return nil, errors.New("bootstrap: WithServiceName is required")
	}
	// 选项全部应用后统一回填组件配置（消除组件对 Bootstrapper 内部
	// 字段的指针依赖，也与调用顺序解耦）
	for _, c := range b.components {
		if s, ok := c.(ServiceNameAware); ok {
			s.SetServiceName(b.serviceName)
		}
	}
	return b, nil
}

// Run 启动服务，阻塞直到收到关闭信号或 userFn 返回。
//
// 执行顺序：
//  1. 创建 lifecycle.Manager 和 App
//  2. 按注册顺序初始化所有 Component
//  3. 调用 userFn(app) 启动用户服务
//  4. 监听 SIGINT/SIGTERM
//  5. 优雅关闭
func (b *Bootstrapper) Run(userFn func(*App) error) error {
	mgr := lifecycle.New(b.shutdownTimeout)
	ctx := context.Background()

	// 1. 创建 App（注册到 mgr）
	app := NewApp(mgr)

	// 2. 初始化组件：任一失败时回滚已初始化组件（执行已注册的关闭钩子），
	//    否则 tracing exporter 等资源会泄漏到进程结束
	for _, c := range b.components {
		if err := safeInit(c, ctx, app, mgr); err != nil {
			mgr.Shutdown(context.Background())
			return fmt.Errorf("bootstrap: component %s init failed: %w", c.Name(), err)
		}
	}

	// 3. 启动用户服务
	// panic 转为 error 发入 errCh：async.GoSafe 的 recover 在外层，
	// 若不在此处转换，panic 会被静默吞掉且 select 永远等不到 errCh，
	// 进程无服务地挂起（假活）。
	errCh := make(chan error, 1)
	async.GoSafe(func() {
		defer func() {
			if r := recover(); r != nil {
				errCh <- fmt.Errorf("bootstrap: userFn panicked: %v", r)
			}
		}()
		errCh <- userFn(app)
	})

	// 4. 监听系统信号
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// 5. 等待：userFn 返回 或 收到信号
	var runErr error
	select {
	case sig := <-sigCh:
		logger.Info("received signal, shutting down...", "signal", sig)
		signal.Stop(sigCh)
	case err := <-errCh:
		runErr = err
		if err != nil {
			logger.Error("userFn returned error", "err", err)
		} else {
			logger.Info("userFn returned, shutting down...")
		}
		signal.Stop(sigCh)
	}

	// 6. 执行清理：每个任务使用自己的独立超时预算（WithShutdownTimeout
	//    是单个任务的默认超时），此处传 Background 不做全局截断，
	//    保证尾部任务（如 nns-deregister）拿到完整预算，不留幽灵实例
	mgr.Shutdown(context.Background())
	return runErr
}

// safeInit 执行组件 Init 并隔离 panic：组件初始化 panic 转为 error，
// 保证失败回滚路径得以执行（panic 会直接跳出 Run 的初始化循环）。
func safeInit(c Component, ctx context.Context, app *App, mgr *lifecycle.Manager) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("component init panicked: %v", r)
		}
	}()
	return c.Init(ctx, app, mgr)
}
