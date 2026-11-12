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
	pkgerrors "github.com/oekit/pkg/errors"
)

var logger = logx.Delegating("bootstrap")

// lateUserFnErrGrace 是 #141 补收晚到 userFn 错误的等待窗口：fatal/信号出口
// 赢得首个 select 时 userFn 可能稍后才返回（慢 flush、慢回滚等形态，issue
// 实测 400ms）——关停完成后再等这个固定小窗口补收其错误；窗口耗尽仍未
// 返回则如实放弃（更晚的错误无法再并入返回值，goroutine 对 buffered chan
// 的投递不阻塞、不泄漏）。取值覆盖常见的「ctx 取消 → userFn 收尾返回」
// 延迟，又不给裸 Spin 强制中止形态的退出增加可感知停顿（治理类 ≤60s 上限内）。
const lateUserFnErrGrace = 2 * time.Second

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
	beforeServe     []func() error
}

// BootstrapperOption 配置 Bootstrapper。
type BootstrapperOption func(*Bootstrapper)

// WithServiceName 设置服务名称（必填）。
func WithServiceName(name string) BootstrapperOption {
	return func(b *Bootstrapper) { b.serviceName = name }
}

// WithShutdownTimeout 设置优雅关闭超时，默认 10 秒。
//
// 零值语义（#206，与 nacos.WithReadyTimeout(0) 同口径）：**0 = 未配置**，
// 回填默认 10s——配置结构体零值透传（WithShutdownTimeout(cfg.ShutdownTimeout)
// 在配置未填时）不再把「没配置超时」静默变成「关停永不超时」：closer 挂死时
// Run 永不返回，只能靠 systemd/K8s kill -9 兜底且全程无日志点明。
// **负值 = 关闭关停超时**（closer 不设兜底预算，仅在确信所有 closer 都会
// 自行返回时使用）。
func WithShutdownTimeout(d time.Duration) BootstrapperOption {
	return func(b *Bootstrapper) {
		if d == 0 {
			d = defaultShutdownTimeout // 0 = 未配置 → 回填默认（#206）
		}
		b.shutdownTimeout = d
	}
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
//
// ServiceNameAware 回填（#131）：New 之后调用时就地补做——New 强制
// serviceName 非空，此后挂载的组件（嵌套 module 动态注入、第三方实现）
// 与经 WithComponent 挂载的组件拿到服务名的时机一致，不再静默拿到空
// 服务名。New 之前（选项应用阶段）调用时 serviceName 尚未定，回填留给
// New 的统一回填；两条路径可能对同一组件各执行一次回填（同值重复），
// ServiceNameAware 实现须幂等。
func (b *Bootstrapper) AddComponent(c Component) {
	b.components = append(b.components, c)
	if b.serviceName != "" {
		if s, ok := c.(ServiceNameAware); ok {
			s.SetServiceName(b.serviceName)
		}
	}
}

// WithShutdownHook 添加自定义关闭钩子。
//
// 执行窗口（#168）：drain 之后、AddResource 资源关闭**之前**——flush 指标/
// 写 WAL 摘要/释放许可证类钩子的依赖资源（DB/连接）此刻仍在位。需要
// 「全部资源关闭与后台排空之后」的晚窗时用
// WithComponent(&ShutdownHookComponent{..., Late: true})。
func WithShutdownHook(name string, hook func(ctx context.Context) error) BootstrapperOption {
	return WithComponent(&ShutdownHookComponent{ComponentName: name, Hook: hook})
}

// WithBeforeServe 注册「引擎启动前」钩子：在组件初始化之后、userFn 之前
// 按注册顺序执行；任一返回 error 则中止启动（已初始化组件走优雅回滚）。
//
// 典型用途：零断点重启的 listener fd 收养（SCM_RIGHTS → restart.Adopt）、
// pid 文件交接、启动前配置校验。这些动作必须发生在「进程已知自己角色」
// 之后、「HTTP 引擎开始 serve」之前——userFn 是引擎跑起来之后的逻辑，
// 装不下这段。
//
// 调用方自备 listener 时也可不经过本钩子，直接：
//
//	h := server.New(server.WithListener(ln), ...)
//
// bootstrap 不接管 listener 的创建/收养，只提供时机保证。
func WithBeforeServe(fn func() error) BootstrapperOption {
	return func(b *Bootstrapper) {
		if fn != nil {
			b.beforeServe = append(b.beforeServe, fn)
		}
	}
}

// ServiceNameAware 需要感知服务名的组件实现此接口；
// New() 在应用完全部选项后统一回填（与 WithServiceName 的调用顺序无关），
// New 之后经 AddComponent 挂载的组件就地补做（#131）。实现须幂等——
// 两条路径可能对同一组件各回填一次（同值）。
// 嵌套 module 中的组件实现它即可参与回填，根包无需感知具体组件类型。
type ServiceNameAware interface {
	SetServiceName(name string)
}

// defaultShutdownTimeout 是优雅关停的默认预算（#206：WithShutdownTimeout(0)
// 零值归一的回填目标，与 New 缺省同一常量，两处不再各写一个 10s）。
const defaultShutdownTimeout = 10 * time.Second

// New 创建 Bootstrapper 实例。
func New(opts ...BootstrapperOption) (*Bootstrapper, error) {
	b := &Bootstrapper{
		shutdownTimeout: defaultShutdownTimeout,
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
//
//  1. 创建 lifecycle.Manager 和 App
//
//  2. 按注册顺序初始化所有 Component
//     2.5 BeforeServe 钩子（引擎启动前的定制准备）
//
//  3. 调用 userFn(app) 启动用户服务——server 就绪后应调用 app.Ready()
//     触发就绪门：延迟注册型组件（如 bootstrap/nacos 的 NNS 组件，
//     nacos/v0.3.0 起）在收到就绪信号后才注册，且关停时反注册先于
//     server 关闭执行（对齐 go-kratos Stop 语义）
//
//     **就绪门漏接的可观测性**（bootstrap#33）：漏调 app.Ready() 时服务
//     不会注册到注册中心——推荐经 hertz/kitex RunServer 的 WithOnReady
//     胶水触发；自建启动骨架（直接 h.Spin()）必须自己调。漏接时
//     bootstrap/nacos 的就绪门看门狗会在超时后打 ERROR 日志点明此事，
//     并可用 nacos 组件的 RegistrationState() 查询注册状态。
//
//  4. 监听 SIGINT/SIGTERM
//
//  5. 等待：userFn 返回、收到信号、或组件经 App.Fail 触发 fail-fast
//     （#133：致命错误经返回值上抛，退出码非 0；userFn 不监听 ctx 时
//     Run 也能被解除阻塞）
//
//  6. 优雅关闭；关停完成后再等一个固定小窗口（lateUserFnErrGrace）补收
//     晚于关停完成才返回的 userFn 错误——界内不丢、界外如实声明丢弃
//     （#141，此前非阻塞读连关停期间返回的错误都接不住）
func (b *Bootstrapper) Run(userFn func(*App) error) error {
	mgr := lifecycle.New(b.shutdownTimeout)
	ctx := context.Background()

	// 1. 创建 App（注册到 mgr）
	app := NewApp(mgr)

	// 2. 初始化组件：任一失败时回滚已初始化组件（执行已注册的关闭钩子），
	//    否则 tracing exporter 等资源会泄漏到进程结束
	for _, c := range b.components {
		// 组件名先取（#201）：c.Name() 移进 panic 隔离面——nil 组件的裸
		// Name() 调用若发生在错误格式化时，已被 safeInit recover 的 init
		// 错误会二次爆炸并冲出 Run（跳过 rollback）。
		name := componentName(c)
		if err := safeInit(c, ctx, app, mgr); err != nil {
			return b.rollback(mgr, fmt.Errorf("bootstrap: component %s init failed: %w", name, err))
		}
	}

	// 2.5 BeforeServe 钩子：引擎启动前的定制准备（listener 收养/pid 交接等）。
	// panic 经 safeBeforeServe 转 error 走同一回滚分支（#201）：此前裸调用
	// 让 panic 直接冲出 Run ⇒ rollback 不执行，已初始化组件的关闭链
	// （tracing exporter flush、nacos 断连、AddResource closer）被整体跳过。
	for i, fn := range b.beforeServe {
		if err := safeBeforeServe(fn); err != nil {
			return b.rollback(mgr, fmt.Errorf("bootstrap: beforeServe[%d] failed: %w", i, err))
		}
	}

	// 3. 启动用户服务
	// panic 转为 error 发入 errCh：async.GoSafe 的 recover 在外层，
	// 若不在此处转换，panic 会被静默吞掉且 select 永远等不到 errCh，
	// 进程无服务地挂起（假活）。panic 值经 pkg/errors.Recovered 收编
	// （#182 单源）：userFn 以 panic(err) 失败时错误身份不丢
	// （errors.Is/As 可穿透 PanicError），栈与类型信息在场。
	errCh := make(chan error, 1)
	async.GoSafe(func() {
		defer func() {
			if perr := pkgerrors.Recovered(recover()); perr != nil {
				errCh <- fmt.Errorf("bootstrap: userFn: %w", perr)
			}
		}()
		errCh <- userFn(app)
	})

	// 4. 监听系统信号
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// 5. 等待：userFn 返回、收到信号 或 fatal fail-fast（#133）
	// fatal 出口解除 Run 的阻塞——userFn 不监听 ctx（裸 h.Spin()）时
	// RequestShutdown 的纯取消语义无法让 Run 返回（进程带 ERROR 日志
	// 继续对外服务，假活）；Fail 的错误投递让 Run 立即进入关停。
	var runErr error
	errChDrained := false // userFn 出口下 errCh 已被消费——此后不可能再有投递
	select {
	case sig := <-sigCh:
		logger.Info("received signal, shutting down...", "signal", sig)
		signal.Stop(sigCh)
	case err := <-errCh:
		errChDrained = true
		runErr = err
		if err != nil {
			logger.Error("userFn returned error", logx.ErrKey, err)
		} else {
			logger.Info("userFn returned, shutting down...")
		}
		signal.Stop(sigCh)
	case err := <-app.fatalCh:
		logger.Error("fatal error triggered shutdown", logx.ErrKey, err)
		signal.Stop(sigCh)
	}

	// 6. 执行清理：每个任务使用自己的独立超时预算（WithShutdownTimeout
	//    是单个任务的默认超时），此处传 Background 不做全局截断，
	//    保证尾部任务（如 nns-deregister）拿到完整预算，不留幽灵实例
	mgr.Shutdown(context.Background())

	// #133：补收 userFn 错误 + fatal 错误上抛（退出码必须反映启动失败）。
	//   - 补收：fatal 出口/信号出口赢得 select 时 userFn 可能稍后才返回
	//     （Fail 先投递再 cancel，userFn 因 ctx 取消而返回的时序晚于 Run
	//     醒来；慢 flush/慢回滚形态甚至晚于关停完成，#141）。仅当首个
	//     select 未消费 errCh 时补收（userFn 出口下 errCh 已被消费，此后
	//     不可能再有投递），且改为**带界等待**：关停完成后再等一个固定
	//     小窗口（lateUserFnErrGrace），界内返回的错误不丢。此前的非阻塞
	//     读连「关停期间返回」都接不住——注释声称「两条错误都不丢」实测
	//     不成立，退出码只反映 fatal。窗口耗尽仍未返回时**如实声明丢弃**：
	//     更晚的错误无法再并入返回值，goroutine 对 buffered chan 的投递
	//     不阻塞、不泄漏；userFn 永不返回（被 fatal 强制中止的裸 Spin
	//     形态）时窗口到点即返回，不拖住关停。
	//   - fatal 上抛：无论 Run 从哪个出口进入关停（userFn 因 ctx 取消返回
	//     nil 的形态也必须带上 fatal 错误）。runErr 优先（业务错误是主因），
	//     fatal 以「; fatal: 」附加。
	if runErr == nil && !errChDrained {
		select {
		case err := <-errCh:
			if err != nil {
				logger.Error("userFn returned error during shutdown", logx.ErrKey, err)
				runErr = err
			}
		case <-time.After(lateUserFnErrGrace):
		}
	}
	if ferr := app.fatalError(); ferr != nil {
		logger.Error("fatal error during run", logx.ErrKey, ferr)
		if runErr != nil {
			runErr = fmt.Errorf("%w; fatal: %w", runErr, ferr)
		} else {
			runErr = ferr
		}
	}

	// #80：关停错误上抛——资源关闭失败（DB flush 未落盘、连接未优雅断开）
	// 时退出码必须反映关停质量，此前 runErr 只承载 userFn 错误，「带着未落盘
	// 数据退出、退出码 0」在滚动发版门禁 / K8s preStop 对账的部署链路上
	// 不可见。runErr 优先（业务错误是主因），关停错误以 join 附加。
	// 模板与 rollback 单源于 mergeTeardownErrs（#181）。
	return mergeTeardownErrs(mgr, runErr, "shutdown")
}

// mergeTeardownErrs 把关停/回滚期 teardown 错误并入主因返回值（#181 单源）：
// Run 尾部（label=shutdown，#80）与 rollback（label=rollback，#169）此前各写
// 一份「mgr.Errors() 非空 → logger.Error 计数留痕 → cause 优先、errors.Join
// 以 "; <label>: " 附加」模板。cause 是主因（业务/启动错误，可为 nil）；
// teardown 错误 errors.Join 后附加，保持 errors.Is 可分流；无 teardown 错误
// 时原样返回 cause。#80/#169 的两处口径（runErr 优先、Join 保 Is）从此单源。
func mergeTeardownErrs(mgr *lifecycle.Manager, cause error, label string) error {
	teardownErrs := mgr.Errors()
	if len(teardownErrs) == 0 {
		return cause
	}
	logger.Error(label+" errors during resource teardown", "count", len(teardownErrs), "errs", teardownErrs)
	joined := errors.Join(teardownErrs...)
	if cause != nil {
		return fmt.Errorf("%w; %s: %w", cause, label, joined)
	}
	return joined
}

// rollback 执行启动失败回滚并把回滚期关停错误并入返回值（#169）。
//
// 与 #80 的正常关停面同口径：回滚期 closer 失败（exporter flush、临时
// 目录/许可证释放）不得只进日志——systemd/K8s 的失败通知只带 return err，
// 多组件启动失败的根因与回滚失败互相掩盖，回滚泄漏（fd/goroutine/注册
// 残留）对部署门禁不可见。cause 是主因（init/beforeServe 错误），
// 回滚期关停错误以「; rollback: 」附加，errors.Join 保持可 errors.Is 分流。
// 合并模板与 Run 尾部单源于 mergeTeardownErrs（#181）。
func (b *Bootstrapper) rollback(mgr *lifecycle.Manager, cause error) error {
	mgr.Shutdown(context.Background())
	return mergeTeardownErrs(mgr, cause, "rollback")
}

// safeInit 执行组件 Init 并隔离 panic：组件初始化 panic 转为 error，
// 保证失败回滚路径得以执行（panic 会直接跳出 Run 的初始化循环）。
// panic 值经 pkg/errors.Recovered 收编（#182 单源）：组件以 panic(err)
// 失败时错误身份不丢；上下文（组件名、init 阶段）由 rollback 的
// 「component %s init failed」包装提供，此处不再叠加前缀。
func safeInit(c Component, ctx context.Context, app *App, mgr *lifecycle.Manager) (err error) {
	defer func() {
		if perr := pkgerrors.Recovered(recover()); perr != nil {
			err = perr
		}
	}()
	return c.Init(ctx, app, mgr)
}

// safeBeforeServe 执行 BeforeServe 钩子并隔离 panic（#201）：钩子 panic 转为
// error 走既有的「beforeServe[%d] failed」回滚分支——与 safeInit 同款隔离
// 粒度、同一 #182 panic→error 单源出口（pkg/errors.Recovered 收编，panic(err)
// 的错误身份不丢，errors.Is/As 可穿透）。此前该循环是裸调用：panic 直接冲出
// Run，rollback 不执行且调用方拿不到 bootstrap: 前缀错误——与 safeInit
// godoc 自己声明的「保证失败回滚路径得以执行」纪律相矛盾。
func safeBeforeServe(fn func() error) (err error) {
	defer func() {
		if perr := pkgerrors.Recovered(recover()); perr != nil {
			err = perr
		}
	}()
	return fn()
}

// componentName 返回组件名，隔离 Name() 的 panic（#201，#182 出口清单成员）：
// nil 组件（WithComponent(nil)）的 Name() 是裸 nil 接口调用——若在 safeInit
// 之后的错误格式化里才炸，已被 recover 的 init 错误会二次爆炸并跳过 rollback。
// 捕获先于 Init、与 safeInit 同一调用点：任何形态（nil 接口 / Name() 自身
// panic）都拿到 "<nil>" 占位名而不是让 Run 冲出；错误文案形态不变。
func componentName(c Component) (name string) {
	defer func() {
		if recover() != nil {
			name = "<nil>"
		}
	}()
	if c == nil {
		return "<nil>"
	}
	return c.Name()
}
