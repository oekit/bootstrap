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
//
// # 文件导览（#180 拆分，行为不变）
//
//   - nns.go（本文件）：组件定义、Init 编排、Option 集；
//   - nns_register.go：注册流程——就绪门看门狗（registerWhenReady）、
//     注册与反注册挂载（registerAndWire）、取消-放弃处置（abandonRegister）；
//   - nns_state.go：注册状态机——RegisterState/RegisterPhase、
//     RegistrationState 读取口与 mark* 迁移辅助。
package nacos

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/oekit/bootstrap"
	"github.com/oekit/core/lifecycle"
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
	//
	// 注意本字段只有「抬升」权：判定为 ReadyFatal || 全局 WithReadyFatal
	// 缺省，置 false 关不掉已开启的全局（bool 零值无法区分「未设置」与
	// 「显式 false」）——显式关闭用 ReadyFatalOverride（#202）。
	ReadyFatal bool
	// ReadyFatalOverride 是 ReadyFatal 的三态显式覆盖（#202）：
	//   - nil（缺省）：未覆盖——按 ReadyFatal || 全局 WithReadyFatal 缺省
	//     判定（现状不变）；
	//   - 非 nil：组件字段显式优先——*false 可在全局 WithReadyFatal 已置位时
	//     按组件粒度关掉 fail-fast（多 Bootstrapper / 宿主进程的逃生舱），
	//     *true 等价于 ReadyFatal: true。
	// 与 ReadyTimeout 的「负值显式关」同一救济口径：显式性由 *bool 承载
	//（裸 bool 的 false 是零值，表达不了「我显式关」）。
	ReadyFatalOverride *bool

	// registerState 记录注册流程状态，供 RegistrationState 查询
	// （状态机见 nns_state.go）。
	registerState atomic.Pointer[RegisterState]
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
// 编排三个关注点（#180 拆分后的编排骨架，注册流程在 nns_register.go、
// 状态机在 nns_state.go）：
//
//  1. 空地址短路（PhaseSkipped）与客户端构造（失败按 Required 决定
//     fail-fast 或告警续跑，失败路径释放已建客户端防泄漏）；
//  2. 端口启用时挂延迟注册后台任务（看门狗 → 注册 → 反注册挂载）；
//  3. 客户端关闭挂 mgr（nns-client job，关停尾部释放）。
//
// 契约：userFn 必须在 server 就绪后调用 app.Ready()（推荐经 hertz/kitex
// RunServer 的 WithOnReady），否则服务不会进入注册中心；Ready 永不触发时
// 注册 goroutine 随应用关闭的 ctx 取消退出，不产生任何注册。
func (c *NNSComponent) Init(ctx context.Context, app *bootstrap.App, mgr *lifecycle.Manager) error {
	// 1. 空地址短路 + 客户端构造与失败清理
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
		logger.Warn("NNS init failed, continuing without NNS", logx.ErrKey, err)
		c.setRegisterState(&RegisterState{
			Phase:   PhaseFailed,
			Service: c.NacosCfg.ServiceName,
			Err:     err,
			Since:   time.Now(),
		})
		return nil
	}
	logger.Info("NNS initialized", "service", c.NacosCfg.ServiceName)

	// 2. 延迟注册后台任务（Port > 0 才有注册面）
	if c.Port > 0 {
		c.setRegisterState(&RegisterState{
			Phase:   PhasePending,
			Service: c.NacosCfg.ServiceName,
			Port:    c.Port,
			Since:   time.Now(),
		})
		if err := app.TryGoBackground("nns-register", func(ctx context.Context) {
			if c.registerWhenReady(ctx, app) {
				c.registerAndWire(ctx, app, nnsc)
			}
		}); err != nil {
			// #186：组件 Init 撞上关停竞态窗口（app-lifecycle:cancel 已执行）
			// 时注册被拒——延迟注册永不发生。Required 强依赖按 fail-fast
			// 上抛（注册缺失=对集群不可用，与 Init 失败同罪）；软依赖告警
			// 续跑（对齐 Init 失败的处置口径）。
			if c.Required {
				return fmt.Errorf("nns: defer registration on required component %s: %w",
					c.NacosCfg.ServiceName, err)
			}
			logger.Warn("nns deferred registration dropped: app shutting down",
				"service", c.NacosCfg.ServiceName, logx.ErrKey, err)
		}
	} else {
		c.setRegisterState(&RegisterState{
			Phase:   PhaseSkipped,
			Service: c.NacosCfg.ServiceName,
			Since:   time.Now(),
		})
	}

	// 3. 客户端关闭挂关停尾部
	mgr.Add("nns-client", func(_ context.Context) error {
		nnsc.Close()
		return nil
	}, 0)

	return nil
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
// 注册中心」。**0 = 未配置**（#203）：回填默认 30s——配置零值透传
// WithReadyTimeout(cfg.GetDuration("ready_timeout")) 在配置未填时不再把
// 「没配置」静默变成「看门狗失效」（#33 补的「服务不会注册到注册中心」
// 可观测性被一个未文档化的零值掐掉）。传**负值**关闭看门狗（恢复静默
// 等待的旧行为）。与组件字段的反义在此点破：NNSComponent.ReadyTimeout 的
// 0 = 回退本全局值（非 0 才是组件优先），选项的 0 = 未配置回默认。
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
		if d == 0 {
			d = defaultReadyTimeout // 0 = 未配置 → 回填默认 30s（#203）
		}
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
//
// ⚠️ 作用域同 WithReadyTimeout（#76，进程级全局）：写入包级 atomic，顺序无关、
// 后写覆盖先写——消费点在注册 goroutine 运行期（组件字段与全局缺省取或，
// 裸 ReadyFatal 字段只有「抬升」权），选项应用全在 bootstrap.New 阶段、消费
// 在 Run 之后，与 WithNNS/WithRequiredNNS 的书写顺序无关。文档曾宣称
// 「须在 WithNNS 之前应用」——与实现不符，已按真实行为修正（本条注释即
// 权威，同 WithReadyTimeout 的纠错）。多 Bootstrapper 场景（测试/宿主进程）
// 会互相覆盖：需要按组件粒度**关掉** fail-fast 时，显式构造组件 +
// Bootstrapper.AddComponent 并置 NNSComponent.ReadyFatalOverride = &false
// （#202——裸 ReadyFatal=false 关不掉全局，只有抬升权；早期版本此处宣称
// 「用组件字段逃生」，探针证伪后已改指逃生舱字段）。
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
