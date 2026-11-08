package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
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

	// closing 在 app-lifecycle:cancel 阶段（先于 a.cancel）置位（#104）：
	// 此后 GoBackground 拒绝新注册。由此保证 wg.Add（正 delta）要么发生在
	// wait 阶段 timedWait 的 wg.Wait 启动之前、要么根本不发生——消除
	// 「counter 为零时 Wait 运行中发起正 delta Add」这一 sync.WaitGroup
	// 规则未定义行为的窗口。
	closing bool
	// closeDone 在 closeAll 收敛（无剩余 closer 且不再有新增）后置位（#99）：
	// 此后 AddResource 的 closer 永远不会被调用，注册时打 ERROR 留痕。
	closeDone bool

	readyOnce sync.Once
	ready     chan struct{} // NewApp 初始化；Ready 时 close 广播
	readyFlag atomic.Bool

	// fail-fast 错误面（#133）：Fail 记录首个致命错误并触发关停；错误经
	// Bootstrapper.Run 的返回值上抛（退出码非 0）。fatalCh 容量 1，Fail
	// 永不阻塞（userFn 不监听 ctx 的裸 Spin 形态也能解除 Run 的 select）。
	fatalMu  sync.Mutex
	fatalErr error      // 首次 Fail 的错误；nil = 未记录（Fail(nil) 仅关停）
	fatalCh  chan error // Run 的唤醒出口；容量 1，首次非 nil 错误投递
}

type namedCloser struct {
	name    string
	closer  func(context.Context) error
	timeout time.Duration // 独立关闭超时预算（#75）；0 表示不单独限时
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
		ready:       make(chan struct{}),
		fatalCh:     make(chan error, 1),
	}

	// 四个阶段独立注册，按注册顺序依次执行。
	// 拆开后每个 phase 独立超时、独立日志，用户也可在中间插入自定义 job。
	mgr.Add("app-lifecycle:cancel", func(_ context.Context) error {
		// 先关闸（closing）再 cancel（#104）：观察到 ctx.Done 的任意代码
		// 路径里，GoBackground 的注册闸必已关闭。
		a.mu.Lock()
		a.closing = true
		a.mu.Unlock()
		a.cancel()
		return nil
	}, 0)
	mgr.Add("app-lifecycle:drain", func(ctx context.Context) error {
		mgr.Drain(ctx)
		return nil
	}, 0)
	mgr.Add("app-lifecycle:close", func(ctx context.Context) error {
		return a.closeAll(ctx) // #80：错误进 shutdownErrs，经 Run 上抛
	}, 0)
	// wait job 不设预算（timeout < 0，Manager 的「显式无超时」语义）：
	// drain 时长只由 SetWaitTimeout 管辖（#134）——若用 Manager 默认预算
	// （timeout=0 ⇒ WithShutdownTimeout，默认 10s），SetWaitTimeout(0) 的
	// 「无限等待」乃至默认 30s drain 都会被静默砍到 10s，且无任何报错。
	// ctx 仍作为显式中止信号传播（调用方 Shutdown(ctx) 取消时放弃等待）。
	mgr.Add("app-lifecycle:wait", func(ctx context.Context) error {
		a.timedWait(ctx)
		return nil
	}, -1)

	return a
}

// Context 返回应用的根 context，可传递给后台任务。
func (a *App) Context() context.Context {
	return a.ctx
}

// Ready 标记应用就绪（server 已监听、健康检查通过）。幂等，并发安全。
// 延迟注册型组件（如 bootstrap/nacos 的 NNS 组件）在收到就绪信号后
// 才执行注册——服务须在 server 就绪后调用本方法，否则不会进入注册中心。
func (a *App) Ready() {
	a.readyOnce.Do(func() {
		a.readyFlag.Store(true)
		close(a.ready)
	})
}

// ReadyChan 返回就绪信号；未 Ready 时阻塞，已 Ready 立即返回。
func (a *App) ReadyChan() <-chan struct{} { return a.ready }

// IsReady 报告当前就绪状态。
func (a *App) IsReady() bool { return a.readyFlag.Load() }

// AddResource 注册需要在关闭时释放的资源。
// name 用于日志标识，closer 是关闭函数。
//
// closer 在 close 阶段按注册逆序执行，无独立超时预算（受 Manager 的
// close job 超时约束）。需要逐资源预算隔离（防止单个资源卡死吃光后续
// 清理预算）时用 AddResourceTimeout（#75）。
//
// 关停边界（#99）：关闭序列启动后（close 阶段收敛循环结束前）注册的
// closer 仍会被执行；close 阶段已结束后注册的 closer 永远不会执行
// （打 ERROR 留痕）。
func (a *App) AddResource(name string, closer func(context.Context) error) {
	a.AddResourceTimeout(name, 0, closer)
}

// AddResourceTimeout 注册需要在关闭时释放的资源，并为其设置独立的关闭
// 超时预算 timeout（#75）——App 用户直接获得 core/lifecycle.Manager 的
// per-job 预算隔离语义，不必再手写 Manager↔App 适配层。
//
// 预算语义（对齐 Manager 的 per-job 超时）：
//   - 预算完全独立（#135）：从 Background 派生，不与 close 阶段共享预算，
//     也不被 close job 的预算（WithShutdownTimeout，默认 10s）封顶——
//     timeout 大于 WithShutdownTimeout 时完整生效（文档示例「outbox 排空
//     20s」在默认配置下成立）。代价是 close 阶段总时长可能超过
//     WithShutdownTimeout（逐资源预算隔离 #75 的既定语义：总时长上界 =
//     各资源预算之和）。调用方 Shutdown ctx 的取消同样不截断预算化
//     closer；无独立预算的 closer（AddResource）仍受 close job 预算约束。
//   - 前序资源的耗时不会挤占本资源的预算。典型场景（octopus 停机编排）：
//     outbox 排空 20s / otel 摘除 10s / nns 注销 10s 三阶段独立预算——
//     共享预算时 outbox 卡住会吃光 nns 注销的预算，产生「已注册但永不
//     反注册」的幽灵实例。
//   - 超时是协作式的：到点时 closer 收到已取消的 ctx，错误经 close 阶段
//     聚合上抛（#80）；不感知 ctx 的 closer 无法被强杀，仍会阻塞 close 阶段。
//   - timeout <= 0 表示不单独限时（等同 AddResource）。
//
// 关停边界（#99）：关闭序列启动后（close 阶段收敛循环结束前）注册的
// closer 仍会被执行；close 阶段已结束后注册的 closer 永远不会执行
// （打 ERROR 留痕）。
func (a *App) AddResourceTimeout(name string, timeout time.Duration, closer func(context.Context) error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closeDone {
		logger.Error("resource added after close phase completed; its closer will never run",
			"resource", name)
	}
	a.closers = append(a.closers, namedCloser{name: name, closer: closer, timeout: timeout})
}

// GoBackground 启动一个带 panic recovery 的后台 goroutine。
// 关闭时通过 ctx 取消信号通知退出，并等待所有后台任务完成。
//
// 关停边界（#104）：shutdown 序列启动（app-lifecycle:cancel 阶段）之后再
// 调用会被拒绝并打 Warn 日志——fn 不执行。此前该窗口内的 wg.Add（正
// delta）与 wait 阶段 timedWait 的 wg.Wait 并发，属于 sync.WaitGroup 规则
// 的未定义行为（counter 为零时 Wait 运行中发起 Add，可能 panic 或漏等）。
func (a *App) GoBackground(name string, fn func(ctx context.Context)) {
	a.mu.Lock()
	if a.closing {
		a.mu.Unlock()
		logger.Warn("app shutting down, background task rejected", "task", name)
		return
	}
	// wg.Add 在锁内、closing 检查之后：与 timedWait 的 wg.Wait 之间由
	// closing 置位（同锁、且发生在 cancel 阶段）全序化——Wait 启动后
	// 不再可能出现正 delta Add（#104）。
	a.wg.Add(1)
	a.mu.Unlock()
	async.GoSafe(func() {
		defer a.wg.Done()
		fn(a.ctx)
	})
}

// RequestShutdown 请求应用进入关闭流程：取消根 context，解除 userFn 中
// `<-app.Context().Done()` 的阻塞，使其返回并触发 lifecycle.Manager 的优雅
// 关闭序列。幂等；在关闭序列中再次调用是安全的（cancel 本身幂等）。
// 典型用途见 lifecycle.App.RequestShutdown。
//
// 注意：纯取消语义不携带失败原因——userFn 不监听 ctx（裸 h.Spin()）时 Run
// 永不返回，userFn 监听 ctx 且返回 nil 时 Run 返回 nil（退出码 0）。需要
// 「中止且退出码非 0」的 fail-fast 语义用 Fail（#133）。
func (a *App) RequestShutdown() {
	a.cancel()
}

// Fail 报告致命错误并触发关停（#133）：fail-fast 原语——等价
// RequestShutdown 的关停触发，但错误经 Bootstrapper.Run 的返回值上抛
// （进程退出码非 0），且 Run 阻塞在等待信号/userFn 返回时被立即解除，
// userFn 无需监听 ctx（裸 h.Spin() 形态）即可被中止。
//
// 适用：启动期发现「服务不可用」必须早死（如延迟注册失败且 Required、
// 就绪门超时且 ReadyFatal）——仅 RequestShutdown 的取消语义表达不了失败，
// 部署门禁 / systemd Restart=on-failure / 对账链路会把启动失败当「成功」。
//
// 语义：
//   - err == nil：仅触发关停（等同 RequestShutdown），不上抛错误；
//   - 首个非 nil 错误生效（幂等，后续调用不覆盖；并发安全）；
//   - userFn 已返回后调用同样生效：错误并入 Run 的返回值（runErr 优先，
//     fatal 以「; fatal: 」附加，与 #80 的关停错误同级）。
//
// Run 返回后 Fail 不再有任何效果（进程退出在即，错误已并入返回值）。
func (a *App) Fail(err error) {
	a.fatalMu.Lock()
	if a.fatalErr == nil {
		a.fatalErr = err
	}
	deliver := a.fatalErr
	a.fatalMu.Unlock()

	if deliver != nil {
		select {
		case a.fatalCh <- deliver:
		default: // 已投递过（幂等）：错误由 Run 的 fatalError 读回
		}
	}
	a.cancel()
}

// fatalError 返回已记录的 fatal 错误（无则 nil）。供 Run 在任意关停出口
// 合并检查——userFn 因 ctx 取消返回 nil 的形态也要带上 fatal 错误。
func (a *App) fatalError() error {
	a.fatalMu.Lock()
	defer a.fatalMu.Unlock()
	return a.fatalErr
}

// SetWaitTimeout 设置 GoBackground goroutine 的 drain 超时。
// 超时后 wg.Wait 被跳过，日志警告仍在运行的 goroutine 数量。
// 默认 30s；传 0 禁用超时（无限等待，仅在确信所有 goroutine 会自行退出时使用）。
//
// drain 时长独立于 WithShutdownTimeout（#134）：wait job 在 lifecycle.Manager
// 上不设预算，「0 = 无限等待」与默认 30s 都不会被 Manager 的默认 job 预算
// （Bootstrapper 下即 WithShutdownTimeout）静默截断。中止源只有两个：本
// 超时到点，与调用方显式取消 Shutdown 的 ctx。
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
		// 无限等待（SetWaitTimeout(0)，#134）：wait job 无预算，本 ctx 只会
		// 因调用方显式取消 Shutdown 的 ctx 而 Done（Bootstrapper.Run 传
		// Background，永不取消）——此时按「显式放弃等待」处理。
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

// closeAll 逆序执行资源关闭并聚合错误（#80）：错误经返回值进 lifecycle.Manager
// 的 shutdownErrs（app-lifecycle:close job 返回它）⇒ Bootstrapper.Run 上抛 ⇒
// 进程退出码反映关停质量——此前只 logger.Error 后继续，资源关闭失败（DB flush
// 未落盘、连接未优雅断开）时退出码仍为 0，「带着未落盘数据退出」在滚动发版
// 门禁 / K8s preStop 对账的部署链路上不可见。逐条日志保留（定位哪个资源）。
//
// 锁纪律（#102/#99，对齐 core/lifecycle.Manager.Shutdown 的「锁内取、锁外执行」
// 范式）：closeAll 全程**不持锁执行 closer**——closer 内回调
// AddResource/AddResourceTimeout/SetWaitTimeout（同锁；常见写法：某资源的
// Close 里注册后续清理、更新 drain 预算）不再死锁挂死整个关停序列。
// 收敛循环（#99）：每轮「加锁取队尾 closer→解锁执行→再检查是否有新增」，
// 直到无剩余 closer——关闭序列启动后注册的 closer（如 nns-deregister 恰在
// 关停竞态窗口完成 nnsc.Register 后才 AddResource）也会被执行，不再产生
// 「已注册但永不反注册」的注册中心幽灵实例（此前只能靠 nacos ephemeral
// 心跳超时兜底，数十秒窗口内流量打向已停止的 server）。
func (a *App) closeAll(ctx context.Context) error {
	var errs []error
	for {
		a.mu.Lock()
		if len(a.closers) == 0 {
			a.closeDone = true
			a.mu.Unlock()
			return errors.Join(errs...)
		}
		// 取队尾（注册逆序）后立即放锁，执行段不持锁（#102）。
		c := a.closers[len(a.closers)-1]
		a.closers = a.closers[:len(a.closers)-1]
		a.mu.Unlock()

		if err := runCloserWithBudget(ctx, c); err != nil {
			logger.Error("close resource failed", "resource", c.name, "err", err)
			errs = append(errs, fmt.Errorf("%s: %w", c.name, err))
		} else {
			logger.Info("resource closed", "resource", c.name)
		}
	}
}

// runCloserWithBudget 执行单个 closer：独立超时预算 + panic 隔离 + 预算
// 切片（#75）。语义对齐 core/lifecycle.Manager.runCloser 的隔离粒度。
//
// timeout <= 0 时不设独立预算，直接透传 close 阶段的 ctx（受 close job
// 预算约束）。timeout > 0 时预算完全自持（#135）：仅从 Background 派生，
// 不挂 parent 的取消传播——close job 的预算（WithShutdownTimeout，默认
// 10s）与调用方 Shutdown ctx 的取消都不得截断本资源，否则「独立预算」被
// close job 预算静默封顶（文档的「outbox 排空 20s」示例在默认配置下不
// 成立）。预算化 closer 的唯一中止源是自身 timeout 到点与 closer 的协作
// 退出；前序资源的耗时同样不得挤占本资源的预算（Background 派生保证）。
//
// panic 隔离（#132，对齐 Manager 的 per-closer recover）：某资源 Close 在
// 停机竞态下 panic 时转为错误（closeAll 会再包上资源名）并继续逆序链上
// 的后续 closer——panic 冲出循环会让其后全部反注册/flush 被跳过，留下
// closeAll 要消灭的注册中心幽灵实例。
func runCloserWithBudget(parent context.Context, c namedCloser) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panicked: %v", r)
		}
	}()
	if c.timeout <= 0 {
		return c.closer(parent)
	}
	jobCtx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()
	return c.closer(jobCtx)
}
