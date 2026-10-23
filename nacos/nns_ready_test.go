package nacos

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/oekit/bootstrap"
	"github.com/oekit/core/lifecycle"
	"github.com/oekit/logx"
	"github.com/oekit/registry/nacos"
)

// captureLogger 是捕获日志的 logx.Logger 测试替身。
//
// 只实现 bootstrap#33 测试所需的最小断言面：记录 Error 级别的消息，
// 供「就绪门看门狗必须打出 ERROR 告警」这类断言使用。
type captureLogger struct {
	mu   sync.Mutex
	errs []string
}

func (c *captureLogger) addErr(msg string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.errs = append(c.errs, msg)
}

// errorMessages 返回已捕获的 ERROR 消息副本。
func (c *captureLogger) errorMessages() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.errs...)
}

// containsSubstring 判断是否有 ERROR 消息包含给定子串。
func (c *captureLogger) containsSubstring(sub string) bool {
	for _, m := range c.errorMessages() {
		if strings.Contains(m, sub) {
			return true
		}
	}
	return false
}

// 以下方法满足 logx.Logger 接口（其余方法本测试用不到，空实现）。

func (c *captureLogger) Debug(string, ...any)       {}
func (c *captureLogger) Info(string, ...any)        {}
func (c *captureLogger) Warn(string, ...any)        {}
func (c *captureLogger) Error(msg string, _ ...any) { c.addErr(msg) }
func (c *captureLogger) Debugf(string, ...any)      {}
func (c *captureLogger) Infof(string, ...any)       {}
func (c *captureLogger) Warnf(string, ...any)       {}
func (c *captureLogger) Errorf(f string, a ...any)  { c.addErr(f) }
func (c *captureLogger) CtxDebug(context.Context, string, ...any) {
}
func (c *captureLogger) CtxInfo(context.Context, string, ...any) {
}
func (c *captureLogger) CtxWarn(context.Context, string, ...any) {
}
func (c *captureLogger) CtxError(_ context.Context, msg string, _ ...any) { c.addErr(msg) }
func (c *captureLogger) CtxDebugf(context.Context, string, ...any) {
}
func (c *captureLogger) CtxInfof(context.Context, string, ...any) {
}
func (c *captureLogger) CtxWarnf(context.Context, string, ...any) {
}
func (c *captureLogger) CtxErrorf(context.Context, string, ...any) {
	c.addErr("")
}

// LoggerBuilder 面：链式派生对测试无意义，原样返回自身。

func (c *captureLogger) WithCtx(context.Context) logx.Logger { return c }
func (c *captureLogger) WithService(string) logx.Logger      { return c }
func (c *captureLogger) WithAttrs(...logx.Attr) logx.Logger  { return c }
func (c *captureLogger) WithGroup(string) logx.Logger        { return c }
func (c *captureLogger) SetLevel(logx.Level)                 {}

// nacospkgBootstrap 返回测试用 Nacos 配置。
func nacospkgBootstrap() nacos.Bootstrap {
	return nacos.Bootstrap{Addr: "127.0.0.1:8848", ServiceName: "test-svc"}
}

// swapLogger 临时把全局默认 logger 换成捕获替身，返回还原函数。
//
// nacos 包的 logger 是 logx.Delegating(...)：每次调用都解析 logx.Default()，
// 故替换全局默认实现即可捕获日志（无需改被测包的包级变量）。
func swapLogger(t *testing.T, cap *captureLogger) func() {
	t.Helper()
	prev := logx.Default()
	logx.SetDefault(cap)
	return func() { logx.SetDefault(prev) }
}

// TestNNSComponent_ReadyWatchdog_AlertsWhenReadyNeverCalled 验证就绪门看门狗：
// app.Ready() 永不被调用时，超过超时必须打出 ERROR 告警，且告警内容点明
// 「服务不会注册到注册中心」——此前该场景**完全静默**，「注册失败」与
// 「根本没尝试注册」在日志上无法区分（bootstrap#33）。
func TestNNSComponent_ReadyWatchdog_AlertsWhenReadyNeverCalled(t *testing.T) {
	fake := withFakeNNS(t)
	comp := newTestComponent()
	// 缩短超时，避免测试等满 30s 默认值
	comp.ReadyTimeout = 50 * time.Millisecond

	cap := &captureLogger{}
	restore := swapLogger(t, cap)

	mgr := lifecycle.New(2 * time.Second)
	app := bootstrap.NewApp(mgr)
	if err := comp.Init(context.Background(), app, mgr); err != nil {
		t.Fatalf("Init: %v", err)
	}

	// 等待看门狗超时（超时后仍继续等就绪信号，故这里只等告警出现）
	deadline := time.Now().Add(3 * time.Second)
	for !cap.containsSubstring("app.Ready() not called") {
		if time.Now().After(deadline) {
			t.Fatalf("watchdog did not alert within timeout; errors=%v", cap.errorMessages())
		}
		time.Sleep(10 * time.Millisecond)
	}

	if !cap.containsSubstring("will NOT be registered to Nacos") {
		t.Errorf("watchdog alert must state service will not be registered; errors=%v", cap.errorMessages())
	}

	// 超时不得改变「注册行为」：Ready 未触发 ⇒ 仍不注册
	if n := fake.registerCount(); n != 0 {
		t.Fatalf("registered %d times without Ready, want 0", n)
	}

	restore()
	mgr.Shutdown(context.Background())
}

// TestNNSComponent_ReadyWatchdog_NoAlertWhenReadyCalled 验证正常路径不打
// 告警：Ready 被调用时注册照常发生，且不产生看门狗 ERROR。
func TestNNSComponent_ReadyWatchdog_NoAlertWhenReadyCalled(t *testing.T) {
	fake := withFakeNNS(t)
	comp := newTestComponent()
	comp.ReadyTimeout = 50 * time.Millisecond

	cap := &captureLogger{}
	restore := swapLogger(t, cap)

	mgr := lifecycle.New(5 * time.Second)
	app := bootstrap.NewApp(mgr)
	if err := comp.Init(context.Background(), app, mgr); err != nil {
		t.Fatalf("Init: %v", err)
	}

	app.Ready()

	deadline := time.Now().Add(3 * time.Second)
	for fake.registerCount() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("Register not called after Ready")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// 超过一次看门狗超时窗口，确认没有产生误告警
	time.Sleep(150 * time.Millisecond)
	if cap.containsSubstring("app.Ready() not called") {
		t.Errorf("unexpected watchdog alert on normal path; errors=%v", cap.errorMessages())
	}

	restore()
	mgr.Shutdown(context.Background())
}

// TestNNSComponent_RegistrationState 验证注册状态快照的各阶段迁移：
// pending（等就绪）→ registered（注册成功）；以及关停前未就绪 → shutdown。
func TestNNSComponent_RegistrationState(t *testing.T) {
	withFakeNNS(t)
	comp := newTestComponent()
	comp.ReadyTimeout = time.Hour // 本用例不触发看门狗

	mgr := lifecycle.New(5 * time.Second)
	app := bootstrap.NewApp(mgr)
	if err := comp.Init(context.Background(), app, mgr); err != nil {
		t.Fatalf("Init: %v", err)
	}

	st := comp.RegistrationState()
	if st == nil {
		t.Fatal("expected non-nil registration state after Init")
	}
	if st.Phase != PhasePending {
		t.Fatalf("phase = %q, want %q", st.Phase, PhasePending)
	}
	if st.Service != "test-svc" || st.Port != 8080 {
		t.Errorf("state = %+v, want service=test-svc port=8080", st)
	}

	app.Ready()

	deadline := time.Now().Add(3 * time.Second)
	for {
		st = comp.RegistrationState()
		if st.Phase == PhaseRegistered {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("phase = %q, want %q", st.Phase, PhaseRegistered)
		}
		time.Sleep(10 * time.Millisecond)
	}

	mgr.Shutdown(context.Background())
}

// TestNNSComponent_RegistrationState_ShutdownWithoutReady 验证就绪门漏接的
// 终态：进程在就绪信号到来前关停 ⇒ 状态为 shutdown（可被巡检发现
// 「进程活着但没进注册中心」）。
func TestNNSComponent_RegistrationState_ShutdownWithoutReady(t *testing.T) {
	withFakeNNS(t)
	comp := newTestComponent()
	comp.ReadyTimeout = time.Hour

	mgr := lifecycle.New(2 * time.Second)
	app := bootstrap.NewApp(mgr)
	if err := comp.Init(context.Background(), app, mgr); err != nil {
		t.Fatalf("Init: %v", err)
	}

	mgr.Shutdown(context.Background())

	// 关停是异步的：等待后台 goroutine 观察到 ctx 取消
	deadline := time.Now().Add(3 * time.Second)
	for {
		st := comp.RegistrationState()
		if st.Phase == PhaseShutdown {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("phase = %q, want %q", st.Phase, PhaseShutdown)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestNNSComponent_RegistrationState_SkippedWhenNoPort 验证未启用注册
// （Port <= 0）时状态为 skipped——「未注册成功」与「压根没打算注册」可区分。
func TestNNSComponent_RegistrationState_SkippedWhenNoPort(t *testing.T) {
	withFakeNNS(t)
	comp := &NNSComponent{
		NacosCfg: nacospkgBootstrap(),
		Port:     0,
	}

	mgr := lifecycle.New(2 * time.Second)
	app := bootstrap.NewApp(mgr)
	if err := comp.Init(context.Background(), app, mgr); err != nil {
		t.Fatalf("Init: %v", err)
	}

	st := comp.RegistrationState()
	if st == nil || st.Phase != PhaseSkipped {
		t.Fatalf("state = %+v, want phase %q", st, PhaseSkipped)
	}
}

// TestNNSComponent_ReadyWaitExceededFlag 验证看门狗超时后状态快照带上
// ReadyWaitExceeded 标记（供巡检区分「已告警但仍在等」）。
func TestNNSComponent_ReadyWaitExceededFlag(t *testing.T) {
	withFakeNNS(t)
	comp := newTestComponent()
	comp.ReadyTimeout = 30 * time.Millisecond

	mgr := lifecycle.New(2 * time.Second)
	app := bootstrap.NewApp(mgr)
	if err := comp.Init(context.Background(), app, mgr); err != nil {
		t.Fatalf("Init: %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for {
		st := comp.RegistrationState()
		if st.ReadyWaitExceeded {
			if st.Phase != PhasePending {
				t.Errorf("phase = %q, want %q (timeout must not fail the flow)", st.Phase, PhasePending)
			}
			mgr.Shutdown(context.Background())
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("ReadyWaitExceeded flag not set after watchdog timeout")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestWithReadyTimeout 验证 WithReadyTimeout 写入的默认超时被组件读取到
// （选项书写顺序不影响取值：选项在 New 阶段应用，Init 在 Run 阶段读取）。
func TestWithReadyTimeout(t *testing.T) {
	orig := readyTimeoutNanos.Load()
	t.Cleanup(func() { readyTimeoutNanos.Store(orig) })

	WithReadyTimeout(1234 * time.Millisecond)(&bootstrap.Bootstrapper{})

	if got := time.Duration(readyTimeoutNanos.Load()); got != 1234*time.Millisecond {
		t.Errorf("ready timeout = %v, want %v", got, 1234*time.Millisecond)
	}
}
