// Package nacos 提供 bootstrap 的 Nacos/NNS 服务治理接入组件。
//
// 本包是独立嵌套 module（github.com/oekit/bootstrap/nacos），以
// BootstrapperOption 函数的形式向 bootstrap 注入 NNS 能力，使根包
// github.com/oekit/bootstrap 保持零 Nacos 依赖——不需要 Nacos 的服务
// 不会在 go.mod 中引入 nacos-sdk-go 及其传递依赖（阿里云 SDK 等 50+ 模块）。
//
// # 用法
//
//	b, _ := bootstrap.New(
//	    bootstrap.WithServiceName("my-service"),
//	    nacos.WithNNS(cfg.Nacos, cfg.Port, cfg.DepServices),
//	)
//	err := b.Run(userFn)
//
// 强依赖场景（初始化失败即启动失败）使用 WithRequiredNNS。
//
// 就绪门 fail-fast（#74）：裸 h.Spin() 起服务（不经 RunServer WithOnReady）
// 的消费方永远不会触发 app.Ready() ⇒ 服务静默永不注册。集群部署下用
// WithReadyFatal 把「30s 只告警」升级为「超时中止启动」。
package nacos

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/oekit/bootstrap"
	"github.com/oekit/core/lifecycle"
	"github.com/oekit/core/version"
	"github.com/oekit/logx"
	nns "github.com/oekit/nns"
	"github.com/oekit/registry"
	nacospkg "github.com/oekit/registry/nacos"
)

var logger = logx.Delegating("bootstrap-nacos")

// defaultReadyTimeout 是就绪门看门狗的默认超时：app.Ready() 迟迟不被调用时，
// 等待该时长后打 ERROR 日志，点明「服务不会注册到注册中心」。
//
// 取 30s 的依据：正常服务从 server 起来到调用 Ready() 是毫秒级的，30s 远大于
// 正常抖动；超过 30s 仍未就绪基本可判定为「漏接就绪门」而非慢启动。
const defaultReadyTimeout = 30 * time.Second

// abandonedDeregisterTimeout 是注册被取消后 best-effort 反注册的独立短超时
// （#136）：不能用已取消的根 ctx（Deregister 会立即失败），也不允许反注册
// 拖住关停——超时/失败仅留痕，不重试。
const abandonedDeregisterTimeout = 2 * time.Second

// readyTimeoutNanos 存放当前默认的就绪门看门狗超时（纳秒）。
//
// 包级可变状态用 atomic 保护（AGENTS.md 硬规则）：WithReadyTimeout 在
// 选项应用阶段写入，组件在 Init 阶段读取——二者都在 bootstrap.New/Run
// 之前/之中完成，故选项书写顺序不影响最终取值。
var readyTimeoutNanos atomic.Int64

// readyFatal 存放 ReadyFatal 全局缺省（WithReadyFatal 写入，组件 Init 读取）。
// WithNNS 组件未显式设置 ReadyFatal 字段时以此为准——与 readyTimeoutNanos
// 同一款「选项写、Init 读」的时序保证。
var readyFatal atomic.Bool

func init() {
	readyTimeoutNanos.Store(int64(defaultReadyTimeout))
}

// ErrReadyFatal 是 ReadyFatal 看门狗超时的哨兵错误（errors.Is 可辨）：
// 中止启动的启动失败原因「就绪门漏接（app.Ready() 未被调用）」。
// 经 App.Fail 进 Bootstrapper.Run 的返回值（#133）——进程退出码非 0，
// 部署门禁/重启策略可按此判定启动失败。
var ErrReadyFatal = errors.New("nns: app.Ready() not called within ReadyTimeout (fail-fast enabled; " +
	"裸 h.Spin() 起服务的消费方必须显式调用 app.Ready()，或改用 hertz/kitex RunServer 的 WithOnReady)")

// NNSComponent NNS 服务治理组件，负责 NNS 客户端初始化和服务注册。
// （自 bootstrap 根包迁移，行为不变。）
type NNSComponent struct {
	NacosCfg    nacospkg.Bootstrap
	Port        int
	DepServices []string
	NSNOpts     []nns.NNSOption
	// Required 为 true 时，NNS 初始化失败将返回错误（fail-fast），
	// 触发 Bootstrapper 的回滚与启动失败；为 false（默认）时仅告警并
	// 继续启动（服务对 NNS 为软依赖）。
	//
	// fail-fast 贯穿到注册失败（nns#77）：注册延迟到 app.Ready() 之后
	// 执行，若此时 Register 失败且 Required=true，组件会 App.Fail 上抛
	// 错误并中止启动（#133：经 Bootstrapper.Run 返回值上抛，退出码非 0，
	// userFn 不监听 ctx 的裸 Spin 形态下 Run 也被解除阻塞）——「强依赖」
	// 不再只覆盖 Init 阶段，「未注册但还在跑」的假活窗口被关掉。
	Required bool
	// ReadyTimeout 是就绪门看门狗超时：等待 app.Ready() 超过该时长仍未
	// 就绪则打 ERROR 日志（点明漏接就绪门 ⇒ 服务不会注册到注册中心）。
	// 零值取默认 30s；负值表示关闭看门狗（恢复旧行为：静默等待）。
	ReadyTimeout time.Duration
	// ReadyFatal 为 true 时，就绪门看门狗超时**中止启动**而非只告警（#74）：
	// 裸 h.Spin() 消费方（不经 hertz.RunServer）永远不会触发 app.Ready()，
	// 服务静默永不注册——注册中心缺失在集群部署下等于服务不存在，早死优于
	// 带日志的静默降级。缺省 false（告警后继续等就绪、慢启动不误杀，#33 语义
	// 不变）；经 WithReadyFatal 显式开启。
	ReadyFatal bool

	// registerState 记录注册流程状态，供 RegistrationState 查询。
	registerState atomic.Pointer[RegisterState]
}

// RegisterState 是服务注册流程的状态快照（bootstrap#33）。
//
// 用途：让「进程活着但服务没进注册中心」这件事可被巡检/健康检查发现——
// 此前就绪门未触发时是**完全静默**的：注册 goroutine 阻塞在
// <-app.ReadyChan()，既不注册也不打任何日志，「注册失败」与
// 「根本没尝试注册」在日志上无法区分。
type RegisterState struct {
	// Phase 是当前阶段。
	Phase RegisterPhase
	// Service 是注册的服务名。
	Service string
	// Port 是注册端口。
	Port int
	// Err 是注册失败时的错误（Phase == PhaseFailed/PhaseAbandoned 时非空；
	// PhaseAbandoned 下为中断注册的 ctx.Err）。
	Err error
	// DeregisterErr 记录 PhaseAbandoned 场景下 best-effort 反注册的结果
	// （#136）：非 nil 表示反注册失败/超时——幽灵实例可能存活到 nacos
	// ephemeral 心跳超时，巡检需核查；nil 表示反注册成功。
	DeregisterErr error
	// Since 是进入当前阶段的时间。
	Since time.Time
	// ReadyWaitExceeded 记录就绪门等待是否已超时（看门狗已告警）。
	ReadyWaitExceeded bool
}

// RegisterPhase 是服务注册流程的阶段。
type RegisterPhase string

const (
	// PhasePending 表示尚未收到就绪信号（等待 app.Ready()）。
	PhasePending RegisterPhase = "pending"
	// PhaseRegistered 表示注册成功。
	PhaseRegistered RegisterPhase = "registered"
	// PhaseFailed 表示注册失败（Err 非空）：客户端明确拿到失败响应，
	// 未产生注册。
	PhaseFailed RegisterPhase = "failed"
	// PhaseAbandoned 表示注册请求因 ctx 取消而中断（#136：关停窗口内
	// in-flight 被取消）——请求可能已到达服务端并生效（gRPC 取消不回滚
	// 服务端状态），组件已用独立短超时 best-effort 反注册一次（结果见
	// DeregisterErr）。与 PhaseFailed（明确失败、未产生注册）和「从未
	// 注册」（PhasePending/PhaseShutdown）可区分，巡检可据此核查幽灵
	// 实例。
	PhaseAbandoned RegisterPhase = "abandoned"
	// PhaseSkipped 表示未启用注册（Port <= 0 或 Nacos 地址为空）。
	PhaseSkipped RegisterPhase = "skipped"
	// PhaseShutdown 表示进程关停前从未收到就绪信号（就绪门漏接）。
	PhaseShutdown RegisterPhase = "shutdown"
)

// String 返回阶段的字符串表示。
func (p RegisterPhase) String() string { return string(p) }

// RegistrationState 返回当前注册状态快照；尚未进入注册流程时返回 nil。
//
// 供健康检查/巡检查询：「进程活着但没进注册中心」可由此判定——
// Phase 为 PhasePending（仍在等就绪信号）、PhaseShutdown（等不到就绪信号
// 就被关停）、PhaseFailed（注册明确失败）或 PhaseAbandoned（注册被关停
// 取消，可能已在服务端生效且已 best-effort 反注册，见 DeregisterErr）
// 都表示**未确认注册成功**。
func (c *NNSComponent) RegistrationState() *RegisterState {
	return c.registerState.Load()
}

// setRegisterState 记录注册状态快照。
func (c *NNSComponent) setRegisterState(st *RegisterState) {
	c.registerState.Store(st)
}

// Name 返回组件名称。
func (c *NNSComponent) Name() string { return "nns" }

// nnsClient 是组件对 NNS 门面的最小依赖面（*nns.NNSClient 隐式满足），
// 供测试替换（newNNSClient 缝）。
type nnsClient interface {
	Init(ctx context.Context, serviceName string, depServices []string) error
	Register(ctx context.Context, port int, opts ...registry.RegisterOptionFunc) error
	Deregister(ctx context.Context, port int, opts ...registry.RegisterOptionFunc) error
	Close()
}

// newNNSClient 是 nns.New 的测试缝。
var newNNSClient = func(cfg nacospkg.Bootstrap, opts ...nns.NNSOption) nnsClient {
	return nns.New(cfg, opts...)
}

// Init 初始化 NNS 客户端；服务注册延迟到 app.Ready() 之后（nacos/v0.3.0 起，
// 对齐 go-kratos「server 全部就绪 → Register；Stop 先 Deregister」语义）。
//
// 契约：userFn 必须在 server 就绪后调用 app.Ready()（推荐经 hertz/kitex
// RunServer 的 WithOnReady），否则服务不会进入注册中心；Ready 永不触发时
// 注册 goroutine 随应用关闭的 ctx 取消退出，不产生任何注册。
func (c *NNSComponent) Init(ctx context.Context, app *bootstrap.App, mgr *lifecycle.Manager) error {
	if c.NacosCfg.Addr == "" {
		logger.Info("nacos addr empty, skipping NNS init")
		c.setRegisterState(&RegisterState{
			Phase:   PhaseSkipped,
			Service: c.NacosCfg.ServiceName,
			Since:   time.Now(),
		})
		return nil
	}

	nnsc := newNNSClient(c.NacosCfg, c.NSNOpts...)

	if err := nnsc.Init(ctx, c.NacosCfg.ServiceName, c.DepServices); err != nil {
		// 释放已创建的 naming/config 客户端（各带后台 goroutine），
		// 避免 init 失败路径泄漏资源
		nnsc.Close()
		if c.Required {
			return fmt.Errorf("nns init (required): %w", err)
		}
		logger.Warn("NNS init failed, continuing without NNS", "err", err)
		c.setRegisterState(&RegisterState{
			Phase:   PhaseFailed,
			Service: c.NacosCfg.ServiceName,
			Err:     err,
			Since:   time.Now(),
		})
		return nil
	}
	logger.Info("NNS initialized", "service", c.NacosCfg.ServiceName)

	if c.Port > 0 {
		c.setRegisterState(&RegisterState{
			Phase:   PhasePending,
			Service: c.NacosCfg.ServiceName,
			Port:    c.Port,
			Since:   time.Now(),
		})
		app.GoBackground("nns-register", func(ctx context.Context) {
			// 就绪门看门狗：Ready 迟迟不来时告警——此前是完全静默的，
			// 「注册失败」与「根本没尝试注册」在日志上无法区分（bootstrap#33）。
			timeout := c.ReadyTimeout
			if timeout == 0 {
				timeout = time.Duration(readyTimeoutNanos.Load())
			}
			fatal := c.ReadyFatal || readyFatal.Load() // 组件字段优先级高于全局缺省（#74）
			var timer *time.Timer
			var timeoutCh <-chan time.Time
			if timeout > 0 {
				timer = time.NewTimer(timeout)
				timeoutCh = timer.C
				defer timer.Stop()
			}

			select {
			case <-ctx.Done():
				c.markShutdownWithoutReady()
				return
			case <-app.ReadyChan():
			case <-timeoutCh:
				// 超时未就绪：明确告警——服务不会注册到注册中心。
				logger.Error("app.Ready() not called within timeout — service will NOT be registered to Nacos; "+
					"check that userFn calls app.Ready() after server starts (hertz/kitex RunServer WithOnReady)",
					"timeout", timeout.String(), "service", c.NacosCfg.ServiceName)
				c.markReadyWaitExceeded()
				// ReadyFatal（#74）：fail-fast 中止启动。裸 h.Spin() 消费方
				// 永远不会触发 app.Ready() ⇒ 静默永不注册——「用了 Nacos 但
				// 服务没进注册中心」的故障排查时不会先想到启动日志里一条 30s
				// 前的 ERROR，注册中心缺失等于服务不存在，早死优于静默降级。
				// 出口为 App.Fail（#133）：哨兵错误经 Bootstrapper.Run 的返回
				// 值上抛（退出码非 0，errors.Is(err, ErrReadyFatal) 可辨），
				// 且 userFn 不监听 ctx 的裸 Spin 形态下 Run 也被解除阻塞——
				// 此前的 RequestShutdown 纯取消出口在裸 Spin 下是 no-op
				// （Run 永不返回）、在监听 ctx 下返回 nil（退出码 0，启动
				// 失败被部署门禁当「成功」），fail-fast 双重失效。
				if fatal {
					c.setRegisterState(&RegisterState{
						Phase:             PhaseFailed,
						Service:           c.NacosCfg.ServiceName,
						Port:              c.Port,
						Err:               ErrReadyFatal,
						Since:             time.Now(),
						ReadyWaitExceeded: true,
					})
					app.Fail(ErrReadyFatal)
					return
				}
				// 超时后继续等就绪信号：慢启动不应被误判为失败，
				// 就绪后仍照常注册（只是已经告警过）。
				select {
				case <-ctx.Done():
					c.markShutdownWithoutReady()
					return
				case <-app.ReadyChan():
				}
			}

			if err := nnsc.Register(ctx, c.Port, registry.WithMetadata(version.Metadata())); err != nil {
				// #136：ctx 取消（关停窗口 in-flight）≠ 明确失败——请求可能
				// 已到达服务端并生效（gRPC 取消不回滚服务端状态），直接退出
				// 会留下幽灵实例（存活到 nacos ephemeral 心跳超时，流量继续
				// 打向已停止的 server）。best-effort 反注册 + 独立状态面。
				if errors.Is(err, context.Canceled) {
					c.abandonRegister(nnsc, err)
					return
				}
				logger.Error("nns register failed (deferred) — service will NOT be discoverable", "err", err)
				c.setRegisterState(&RegisterState{
					Phase:   PhaseFailed,
					Service: c.NacosCfg.ServiceName,
					Port:    c.Port,
					Err:     err,
					Since:   time.Now(),
				})
				// Required fail-fast 贯穿到注册失败（nns#77）：注册是 Ready
				// 之后的延迟动作，Init 阶段的 fail-fast 管不到这里——强依赖
				// 服务「未注册但还在跑」等于对集群不可用，与初始化失败同罪。
				// 出口为 App.Fail（#133）：错误经 Bootstrapper.Run 返回值上抛
				// （退出码非 0），且 userFn 不监听 ctx（裸 h.Spin()）时也能
				// 解除 Run 阻塞——RequestShutdown 的纯取消语义在两种形态下
				// 都失效（Run 永不返回 / 返回 nil 退出码 0）。
				if c.Required {
					logger.Error("nns register failed on required component, failing fast",
						"service", c.NacosCfg.ServiceName, "err", err)
					app.Fail(fmt.Errorf("nns: register failed on required component %s: %w",
						c.NacosCfg.ServiceName, err))
				}
				return
			}
			// 注册成功后才挂反注册。app.AddResource 落在 close 阶段且按注册
			// 逆序释放：userFn 先注册 server 资源 → Ready → 此处后注册
			// ⇒ 关停序为 nns-deregister → server.Shutdown（对齐 go-kratos Stop）。
			app.AddResource("nns-deregister", func(ctx context.Context) error {
				return nnsc.Deregister(ctx, c.Port)
			})
			c.setRegisterState(&RegisterState{
				Phase:   PhaseRegistered,
				Service: c.NacosCfg.ServiceName,
				Port:    c.Port,
				Since:   time.Now(),
			})
			logger.Info("registered (deferred)", "service", c.NacosCfg.ServiceName, "port", c.Port)
		})
	} else {
		c.setRegisterState(&RegisterState{
			Phase:   PhaseSkipped,
			Service: c.NacosCfg.ServiceName,
			Since:   time.Now(),
		})
	}

	mgr.Add("nns-client", func(_ context.Context) error {
		nnsc.Close()
		return nil
	}, 0)

	return nil
}

// markReadyWaitExceeded 标记就绪门等待已超时（看门狗已告警），
// 但注册流程仍在继续——状态保持 PhasePending。
//
// 状态快照以值拷贝后整体替换（不复用已发布的指针），避免与并发读
// RegistrationState 的调用方产生数据竞争。
func (c *NNSComponent) markReadyWaitExceeded() {
	st := c.registerState.Load()
	if st == nil {
		return
	}
	next := *st
	next.ReadyWaitExceeded = true
	c.setRegisterState(&next)
}

// markShutdownWithoutReady 标记进程在就绪信号到来前就关停（就绪门漏接）。
func (c *NNSComponent) markShutdownWithoutReady() {
	st := c.registerState.Load()
	if st == nil {
		return
	}
	if st.Phase != PhasePending {
		return
	}
	c.setRegisterState(&RegisterState{
		Phase:             PhaseShutdown,
		Service:           st.Service,
		Port:              st.Port,
		Since:             time.Now(),
		ReadyWaitExceeded: st.ReadyWaitExceeded,
	})
}

// abandonRegister 处理注册请求被 ctx 取消的场景（#136）：关停窗口内
// in-flight 的 nnsc.Register 因根 ctx 取消拿到 context.Canceled，但请求
// 可能已到达服务端并生效（gRPC 取消不回滚服务端状态）——此时反注册
// closer 尚未挂上（只在注册成功后挂），直接退出会留下幽灵实例。
//
// 处置：用独立短超时 ctx best-effort 调一次 nnsc.Deregister（不重试、
// 不拖住关停），状态落 PhaseAbandoned（与 PhaseFailed「明确失败」和
// 「从未注册」可区分）；反注册失败时打 ERROR 留痕并记入
// RegisterState.DeregisterErr，供巡检核查幽灵实例。
func (c *NNSComponent) abandonRegister(nnsc nnsClient, regErr error) {
	logger.Error("nns register cancelled during shutdown — registration may have reached the server, best-effort deregistering",
		"service", c.NacosCfg.ServiceName, "err", regErr)

	deregCtx, cancel := context.WithTimeout(context.Background(), abandonedDeregisterTimeout)
	deregErr := nnsc.Deregister(deregCtx, c.Port)
	cancel()
	if deregErr != nil {
		logger.Error("nns best-effort deregister failed after cancelled register — ghost instance possible until nacos ephemeral timeout",
			"service", c.NacosCfg.ServiceName, "err", deregErr)
	}

	c.setRegisterState(&RegisterState{
		Phase:         PhaseAbandoned,
		Service:       c.NacosCfg.ServiceName,
		Port:          c.Port,
		Err:           regErr,
		DeregisterErr: deregErr,
		Since:         time.Now(),
	})
}

// WithNNS 添加 NNS 服务治理组件。
//
// nacosCfg 是 Nacos 连接配置，port 是服务注册端口（0 则跳过注册），
// depServices 是依赖服务列表，nnsOpts 传递给 nns.New。
//
// nacos/v0.3.0 起注册延迟到 app.Ready() 之后：userFn 须在 server 就绪后
// 触发就绪门（推荐 hertz/kitex RunServer 的 WithOnReady），关停时反注册
// 先于 server 关闭执行。
//
// 就绪门看门狗（bootstrap#33）：等待 app.Ready() 超过 30s（默认）仍未就绪
// 则打 ERROR 日志，点明「服务不会注册到注册中心」。用 WithReadyTimeout 调整
// 超时；传负值关闭看门狗。
func WithNNS(nacosCfg nacospkg.Bootstrap, port int, depServices []string, nnsOpts ...nns.NNSOption) bootstrap.BootstrapperOption {
	return func(b *bootstrap.Bootstrapper) {
		b.AddComponent(&NNSComponent{
			NacosCfg:    nacosCfg,
			Port:        port,
			DepServices: depServices,
			NSNOpts:     nnsOpts,
		})
	}
}

// WithReadyTimeout 调整就绪门看门狗超时（bootstrap#33）。
//
// 等待 app.Ready() 超过该时长仍未就绪，则打 ERROR 日志点明「服务不会注册到
// 注册中心」。传负值关闭看门狗（恢复静默等待的旧行为）。
//
// ⚠️ 作用域是**进程级全局**（#76）：写入包级 atomic，此后本进程内**所有**
// NNS 组件（任何 Bootstrapper）在 Init 时读取——与函数选项的常规直觉相反，
// 且顺序无关、后写覆盖先写。同进程跑多个 Bootstrapper 的场景（测试/宿主进程）
// 会互相覆盖：需要 per-Bootstrapper 作用域时不要用本选项，改用组件字段
// NNSComponent.ReadyTimeout（显式构造组件 + Bootstrapper.AddComponent）。
// 文档曾宣称「须在 WithNNS 之前应用、仅作用于同一 Bootstrapper 随后加入的
// 组件」——与实现不符，已按真实行为修正（本条注释即权威）。
//
// 用法（单 Bootstrapper 进程，书写顺序无关）：
//
//	b, _ := bootstrap.New(
//	    bootstrap.WithServiceName("my-service"),
//	    nacos.WithReadyTimeout(10*time.Second),
//	    nacos.WithNNS(cfg.Nacos, cfg.Port, cfg.DepServices),
//	)
func WithReadyTimeout(d time.Duration) bootstrap.BootstrapperOption {
	return func(b *bootstrap.Bootstrapper) {
		readyTimeoutNanos.Store(int64(d))
	}
}

// WithReadyFatal 开启就绪门看门狗的 fail-fast（#74；出口语义修正见 #133）：
// 等待 app.Ready() 超时后**中止启动**——App.Fail(ErrReadyFatal) 触发优雅
// 关停并把哨兵错误经 Bootstrapper.Run 的返回值上抛（进程退出码非 0，
// errors.Is 可辨），而非只打一条 ERROR 继续跑——后者是「带日志的静默
// 降级」：服务照常起、但永远没进注册中心，集群部署下等于服务不存在，
// 故障排查时不会先想到去看启动日志里一条 30 秒前的 ERROR。
//
// 出口保证（#133）：userFn 不监听 app ctx 的裸 h.Spin() 形态下 Run 也会被
// Fail 解除阻塞并以 ErrReadyFatal 返回（此前 RequestShutdown 的纯取消出口
// 在该形态下是 no-op——进程带 ERROR 日志继续对外服务）；userFn 监听 ctx
// 且返回 nil 的形态下 Run 返回 ErrReadyFatal 而非 nil（退出码 0 曾把启动
// 失败当「成功」）。调用方也可经 NNSComponent.RegistrationState() 轮询
// 注册状态（PhaseFailed + Err 哨兵）。
//
// 适用形态：
//   - 集群部署（Nacos 是服务发现的唯一事实源，注册缺失=不可用）；
//   - 裸 h.Spin()/kitex 起服务、无 RunServer WithOnReady 接就绪门的消费方
//     （正是「静默永不注册」的高危形态，见 #74 的下游活案例）。
//
// 不适合：本地开发（Nacos 常不可达）、注册为纯软增强的服务（有其他发现路径）。
// 用法同 WithReadyTimeout：须在 WithNNS/WithRequiredNNS 之前应用。
func WithReadyFatal() bootstrap.BootstrapperOption {
	return func(b *bootstrap.Bootstrapper) {
		readyFatal.Store(true)
	}
}

// WithRequiredNNS 添加 NNS 组件并声明为强依赖：
// NNS 初始化失败时启动失败（fail-fast），而非默认的告警继续；
// 注册失败（Ready 之后的延迟动作）同样触发 fail-fast——App.Fail 把包装
// 错误经 Bootstrapper.Run 的返回值上抛（退出码非 0，nns#77；出口语义
// 修正见 #133），强依赖语义覆盖注册全流程。
func WithRequiredNNS(nacosCfg nacospkg.Bootstrap, port int, depServices []string, nnsOpts ...nns.NNSOption) bootstrap.BootstrapperOption {
	return func(b *bootstrap.Bootstrapper) {
		b.AddComponent(&NNSComponent{
			NacosCfg:    nacosCfg,
			Port:        port,
			DepServices: depServices,
			NSNOpts:     nnsOpts,
			Required:    true,
		})
	}
}
