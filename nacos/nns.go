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
package nacos

import (
	"context"
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

// readyTimeoutNanos 存放当前默认的就绪门看门狗超时（纳秒）。
//
// 包级可变状态用 atomic 保护（AGENTS.md 硬规则）：WithReadyTimeout 在
// 选项应用阶段写入，组件在 Init 阶段读取——二者都在 bootstrap.New/Run
// 之前/之中完成，故选项书写顺序不影响最终取值。
var readyTimeoutNanos atomic.Int64

func init() {
	readyTimeoutNanos.Store(int64(defaultReadyTimeout))
}

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
	Required bool
	// ReadyTimeout 是就绪门看门狗超时：等待 app.Ready() 超过该时长仍未
	// 就绪则打 ERROR 日志（点明漏接就绪门 ⇒ 服务不会注册到注册中心）。
	// 零值取默认 30s；负值表示关闭看门狗（恢复旧行为：静默等待）。
	ReadyTimeout time.Duration

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
	// Err 是注册失败时的错误（Phase == PhaseFailed 时非空）。
	Err error
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
	// PhaseFailed 表示注册失败（Err 非空）。
	PhaseFailed RegisterPhase = "failed"
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
// Phase 为 PhasePending（仍在等就绪信号）或 PhaseShutdown（等不到就绪信号
// 就被关停）都表示**未注册成功**。
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
				logger.Error("nns register failed (deferred) — service will NOT be discoverable", "err", err)
				c.setRegisterState(&RegisterState{
					Phase:   PhaseFailed,
					Service: c.NacosCfg.ServiceName,
					Port:    c.Port,
					Err:     err,
					Since:   time.Now(),
				})
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
// 用法（须在 WithNNS/WithRequiredNNS 之前应用，作用于同一 Bootstrapper
// 内随后加入的 NNS 组件）：
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

// WithRequiredNNS 添加 NNS 组件并声明为强依赖：
// NNS 初始化失败时启动失败（fail-fast），而非默认的告警继续。
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
