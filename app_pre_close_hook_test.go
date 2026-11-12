package bootstrap

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/oekit/core/lifecycle"
	"github.com/oekit/logx"
)

// logCapture 是捕获日志的 logx.Logger 测试替身（#186）：记录 Error 级别
// 消息，供「迟注册的钩子必须打 ERROR 留痕」这类断言使用。
type logCapture struct {
	mu   sync.Mutex
	errs []string
}

func (c *logCapture) addErr(msg string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.errs = append(c.errs, msg)
}

func (c *logCapture) containsSubstring(sub string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, m := range c.errs {
		if strings.Contains(m, sub) {
			return true
		}
	}
	return false
}

// 以下方法满足 logx.Logger 接口（非 Error 方法本测试用不到，空实现）。
func (c *logCapture) Debug(string, ...any)                     {}
func (c *logCapture) Info(string, ...any)                      {}
func (c *logCapture) Warn(string, ...any)                      {}
func (c *logCapture) Error(msg string, _ ...any)               { c.addErr(msg) }
func (c *logCapture) Debugf(string, ...any)                    {}
func (c *logCapture) Infof(string, ...any)                     {}
func (c *logCapture) Warnf(string, ...any)                     {}
func (c *logCapture) Errorf(f string, _ ...any)                { c.addErr(f) }
func (c *logCapture) CtxDebug(context.Context, string, ...any) {}
func (c *logCapture) CtxInfo(context.Context, string, ...any)  {}
func (c *logCapture) CtxWarn(context.Context, string, ...any)  {}
func (c *logCapture) CtxError(_ context.Context, msg string, _ ...any) {
	c.addErr(msg)
}
func (c *logCapture) CtxDebugf(context.Context, string, ...any) {}
func (c *logCapture) CtxInfof(context.Context, string, ...any)  {}
func (c *logCapture) CtxWarnf(context.Context, string, ...any)  {}
func (c *logCapture) CtxErrorf(_ context.Context, f string, _ ...any) {
	c.addErr(f)
}

// LoggerBuilder 面：链式派生对测试无意义，原样返回自身（logx.Delegating
// 解析后会对默认实现包 component attr——WithAttrs 返回自身才能保住捕获面）。
func (c *logCapture) WithCtx(context.Context) logx.Logger { return c }
func (c *logCapture) WithService(string) logx.Logger      { return c }
func (c *logCapture) WithAttrs(...logx.Attr) logx.Logger  { return c }
func (c *logCapture) WithGroup(string) logx.Logger        { return c }
func (c *logCapture) SetLevel(logx.Level)                 {}

// swapCaptureLogger 临时把全局默认 logger 换成捕获替身，返回还原函数。
// bootstrap 包的 logger 是 logx.Delegating(...)：每次调用解析 logx.Default()，
// 替换全局默认实现即可捕获日志。
func swapCaptureLogger(t *testing.T) *logCapture {
	t.Helper()
	cap := &logCapture{}
	prev := logx.Default()
	logx.SetDefault(cap)
	t.Cleanup(func() { logx.SetDefault(prev) })
	return cap
}

// TestApp_AddPreCloseHookAfterDrainLogsERROR 钉住 #186：前置钩子队列收敛
// （close 阶段的 runPreCloseHooks 取空返回）之后注册的钩子永远不会被执行，
// 注册时必须打 ERROR 留痕——此前 AddPreCloseHook 对该窗口完全静默，与
// AddResourceTimeout 的 closeDone ERROR 口径（#99）不一致。
func TestApp_AddPreCloseHookAfterDrainLogsERROR(t *testing.T) {
	mgr := lifecycle.New(2 * time.Second)
	app := NewApp(mgr)

	// 一个正常钩子：收敛前注册，照常执行（收敛语义不受影响）。
	earlyRan := false
	app.AddPreCloseHook("early", func(context.Context) error {
		earlyRan = true
		return nil
	})

	done := make(chan struct{})
	go func() {
		mgr.Shutdown(context.Background())
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("shutdown 挂死")
	}
	if !earlyRan {
		t.Fatal("收敛前注册的钩子应照常执行（#168 收敛语义）")
	}
	if !app.preCloseDone {
		t.Fatal("钩子队列收敛后 preCloseDone 应置位（#186）")
	}

	// 收敛后注册：永不执行，注册时必须打 ERROR 留痕。
	cap := swapCaptureLogger(t)
	lateRan := false
	app.AddPreCloseHook("late", func(context.Context) error {
		lateRan = true
		return nil
	})
	if lateRan {
		t.Fatal("收敛后注册的钩子不应被执行")
	}
	if !cap.containsSubstring("pre-close hook added after close phase completed") {
		t.Fatalf("迟注册的钩子应打 ERROR 留痕（#186），captured: %v", cap.errs)
	}
}

// TestApp_AddPreCloseHookDuringClosePhaseStillRuns 钉住收敛边界（#186）：
// 钩子执行期间新注册的钩子仍会被下一轮 take 补齐执行（drainClosers 收敛
// 语义），preCloseDone 不置位、不打 ERROR——「注定不执行」的判定边界是
// 收敛完成，而非 close 阶段开始。
func TestApp_AddPreCloseHookDuringClosePhaseStillRuns(t *testing.T) {
	mgr := lifecycle.New(2 * time.Second)
	app := NewApp(mgr)

	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	app.AddPreCloseHook("first", func(context.Context) error {
		close(firstStarted)
		<-releaseFirst
		return nil
	})

	go func() {
		// 第二个钩子在第一个钩子执行期间（close 阶段进行中）注册。
		<-firstStarted
		app.AddPreCloseHook("second", func(context.Context) error { return nil })
		close(releaseFirst)
	}()

	cap := swapCaptureLogger(t)
	done := make(chan struct{})
	go func() {
		mgr.Shutdown(context.Background())
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("shutdown 挂死：close 阶段进行中的钩子注册应被收敛轮次补齐")
	}
	if cap.containsSubstring("pre-close hook added after close phase completed") {
		t.Fatalf("close 阶段进行中注册的钩子会被执行，不应打 ERROR：captured %v", cap.errs)
	}
	if app.preCloseDone && len(app.preClose) != 0 {
		// 收敛后 preClose 队列必然已取空；此处钉住「second 在收敛前入队」。
		t.Fatal("close 阶段进行中注册的钩子应在收敛前入队执行")
	}
}
