package bootstrap

import (
	"context"
	"sync"
	"time"

	"github.com/oekit/concurrency/async"
	"github.com/oekit/core/lifecycle"
)

// App 是应用生命周期管理器，统一管理资源注册、后台任务和优雅关闭。
//
// 用法：
//
//	app := bootstrap.NewApp(shutdownMgr)
//	app.AddResource("db", db.Close)
//	app.GoBackground("poller", func(ctx context.Context) { ... })
//	// shutdown 时自动按注册逆序关闭所有资源和等待后台任务退出
type App struct {
	mu          sync.Mutex
	mgr         *lifecycle.Manager
	closers     []namedCloser
	wg          sync.WaitGroup
	cancel      context.CancelFunc
	ctx         context.Context
	waitTimeout time.Duration // GoBackground goroutine drain timeout
}

type namedCloser struct {
	name   string
	closer func(context.Context) error
}

// defaultWaitTimeout is the default drain timeout for GoBackground goroutines.
const defaultWaitTimeout = 30 * time.Second

// NewApp 创建应用生命周期管理器，挂到调用方传入的 lifecycle.Manager 上（attach 模式）。
//
// 契约：NewApp 只做生命周期编排，**不接管信号与 server**——不调用
// signal.Notify、不调用 h.Spin/RunServer；SIGINT/SIGTERM 与服务启停
// 一律由宿主（如 hertz.Server + OnShutdown）负责，宿主停机回调里再调
// mgr.Shutdown 即可。
//
// 四阶段顺序语义：NewApp 立即向 mgr 注册
// app-lifecycle:{cancel,drain,close,wait} 四个 job；lifecycle.Manager 按
// **注册顺序**执行，因此调用方在 NewApp **之前** mgr.Add 的任务先于四阶段执行，
// 在 NewApp **之后** mgr.Add 的任务排在四阶段之后。应用自身资源请用
// AddResource/GoBackground（分别落在 close / wait 阶段），不要在 NewApp 之后
// 直接 mgr.Add（会排到 wait 之后，错过 close 窗口）。
func NewApp(mgr *lifecycle.Manager) *App {
	ctx, cancel := context.WithCancel(context.Background())
	a := &App{
		mgr:         mgr,
		cancel:      cancel,
		ctx:         ctx,
		waitTimeout: defaultWaitTimeout,
	}

	// 四个阶段独立注册，按注册顺序依次执行。
	// 拆开后每个 phase 独立超时、独立日志，用户也可在中间插入自定义 job。
	mgr.Add("app-lifecycle:cancel", func(_ context.Context) error {
		a.cancel()
		return nil
	}, 0)
	mgr.Add("app-lifecycle:drain", func(ctx context.Context) error {
		mgr.Drain(ctx)
		return nil
	}, 0)
	mgr.Add("app-lifecycle:close", func(ctx context.Context) error {
		a.closeAll(ctx)
		return nil
	}, 0)
	mgr.Add("app-lifecycle:wait", func(ctx context.Context) error {
		a.timedWait(ctx)
		return nil
	}, 0)

	return a
}

// Context 返回应用的根 context，可传递给后台任务。
func (a *App) Context() context.Context {
	return a.ctx
}

// AddResource 注册需要在关闭时释放的资源。
// name 用于日志标识，closer 是关闭函数。
func (a *App) AddResource(name string, closer func(context.Context) error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.closers = append(a.closers, namedCloser{name: name, closer: closer})
}

// GoBackground 启动一个带 panic recovery 的后台 goroutine。
// 关闭时通过 ctx 取消信号通知退出，并等待所有后台任务完成。
func (a *App) GoBackground(name string, fn func(ctx context.Context)) {
	a.wg.Add(1)
	async.GoSafe(func() {
		defer a.wg.Done()
		fn(a.ctx)
	})
}

// RequestShutdown 请求应用进入关闭流程：取消根 context，解除 userFn 中
// `<-app.Context().Done()` 的阻塞，使其返回并触发 lifecycle.Manager 的优雅
// 关闭序列。幂等；在关闭序列中再次调用是安全的（cancel 本身幂等）。
// 典型用途见 lifecycle.App.RequestShutdown。
func (a *App) RequestShutdown() {
	a.cancel()
}

// SetWaitTimeout 设置 GoBackground goroutine 的 drain 超时。
// 超时后 wg.Wait 被跳过，日志警告仍在运行的 goroutine 数量。
// 默认 30s；传 0 禁用超时（无限等待，仅在确信所有 goroutine 会自行退出时使用）。
func (a *App) SetWaitTimeout(d time.Duration) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.waitTimeout = d
}

// timedWait 等待所有 GoBackground goroutine 退出，超时或 context 取消则跳过并警告。
//
// 超时触发后，内部 wg.Wait goroutine 被有意泄漏（sync.WaitGroup 无法取消等待），
// 进程退出时自动回收。这是可接受的，因为仍在运行的 goroutine 本身就是拒绝退出的。
func (a *App) timedWait(ctx context.Context) {
	a.mu.Lock()
	timeout := a.waitTimeout
	a.mu.Unlock()

	done := make(chan struct{})
	async.GoSafe(func() {
		a.wg.Wait()
		close(done)
	})

	if timeout <= 0 {
		select {
		case <-done:
		case <-ctx.Done():
			logger.Warn("shutdown context cancelled, skipping goroutine drain",
				"err", ctx.Err())
		}
		return
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-done:
	case <-timer.C:
		logger.Warn("GoBackground goroutine drain timeout, proceeding with shutdown",
			"timeout", timeout.String())
	case <-ctx.Done():
		logger.Warn("shutdown context cancelled, skipping goroutine drain",
			"err", ctx.Err())
	}
}

func (a *App) closeAll(ctx context.Context) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for i := len(a.closers) - 1; i >= 0; i-- {
		c := a.closers[i]
		if err := c.closer(ctx); err != nil {
			logger.Error("close resource failed", "resource", c.name, "err", err)
		} else {
			logger.Info("resource closed", "resource", c.name)
		}
	}
}
