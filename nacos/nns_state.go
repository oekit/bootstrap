// 注册状态机（bootstrap#33 快照面）：RegisterState/RegisterPhase 的类型与
// 取值、RegistrationState 读取口与 mark* 迁移辅助。自 nns.go 拆出（#180），
// 行为不变——Init 编排、注册流程（nns_register.go）各自只依赖本文件的状态口。

package nacos

import (
	"time"
)

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
