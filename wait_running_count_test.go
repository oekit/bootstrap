package bootstrap

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/oekit/core/lifecycle"
	"github.com/oekit/logx"
)

// kvLogCapture 记录「消息 + kv 对」的日志测试替身（#205）：drain 超时告警的
// 契约面是 kv 字段（timeout / running），只记 msg 的 logCapture 断言不到
// 「running 数量」是否真的进了告警。
type kvLogCapture struct {
	mu    sync.Mutex
	lines []string
}

func (c *kvLogCapture) record(msg string, kv ...any) {
	var b strings.Builder
	b.WriteString(msg)
	for i := 0; i+1 < len(kv); i += 2 {
		fmt.Fprintf(&b, " %v=%v", kv[i], kv[i+1])
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lines = append(c.lines, b.String())
}

func (c *kvLogCapture) warnLines() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.lines...)
}

func (c *kvLogCapture) Debug(string, ...any)                     {}
func (c *kvLogCapture) Info(string, ...any)                      {}
func (c *kvLogCapture) Warn(msg string, kv ...any)               { c.record(msg, kv...) }
func (c *kvLogCapture) Error(msg string, kv ...any)              { c.record(msg, kv...) }
func (c *kvLogCapture) Debugf(string, ...any)                    {}
func (c *kvLogCapture) Infof(string, ...any)                     {}
func (c *kvLogCapture) Warnf(f string, a ...any)                 { c.record(f, a...) }
func (c *kvLogCapture) Errorf(f string, a ...any)                { c.record(f, a...) }
func (c *kvLogCapture) CtxDebug(context.Context, string, ...any) {}
func (c *kvLogCapture) CtxInfo(context.Context, string, ...any)  {}
func (c *kvLogCapture) CtxWarn(_ context.Context, msg string, kv ...any) {
	c.record(msg, kv...)
}
func (c *kvLogCapture) CtxError(_ context.Context, msg string, kv ...any) {
	c.record(msg, kv...)
}
func (c *kvLogCapture) CtxDebugf(context.Context, string, ...any) {}
func (c *kvLogCapture) CtxInfof(context.Context, string, ...any)  {}
func (c *kvLogCapture) CtxWarnf(_ context.Context, f string, a ...any) {
	c.record(f, a...)
}
func (c *kvLogCapture) CtxErrorf(_ context.Context, f string, a ...any) {
	c.record(f, a...)
}

// LoggerBuilder 面：链式派生原样返回自身（logx.Delegating 解析后包
// component attr，WithAttrs 返回自身才能保住捕获面）。
func (c *kvLogCapture) WithCtx(context.Context) logx.Logger { return c }
func (c *kvLogCapture) WithService(string) logx.Logger      { return c }
func (c *kvLogCapture) WithAttrs(...logx.Attr) logx.Logger  { return c }
func (c *kvLogCapture) WithGroup(string) logx.Logger        { return c }
func (c *kvLogCapture) SetLevel(logx.Level)                 {}

// swapKVLogger 临时把全局默认 logger 换成 kv 捕获替身（bootstrap 包 logger 是
// logx.Delegating，每次调用解析 logx.Default()）。
func swapKVLogger(t *testing.T) *kvLogCapture {
	t.Helper()
	c := &kvLogCapture{}
	prev := logx.Default()
	logx.SetDefault(c)
	t.Cleanup(func() { logx.SetDefault(prev) })
	return c
}

// TestTimedWait_DrainTimeoutWarnsWithRunningCount 钉住 #205：SetWaitTimeout
// godoc 承诺「超时后日志警告仍在运行的 goroutine 数量」，实现的告警只有
// timeout 一个字段——关停是排障黄金现场，这行告警是「有 goroutine 没退出、
// 进程正在放弃 drain」的唯一定位信号，拿不到泄漏规模（卡了 1 个还是 50 个）
// 只能事后 pprof 猜。修复后 GoBackground 维护在册计数（bgRunning），告警带
// running 字段。
func TestTimedWait_DrainTimeoutWarnsWithRunningCount(t *testing.T) {
	mgr := lifecycle.New(2 * time.Second)
	app := NewApp(mgr)

	release := make(chan struct{})
	started := make(chan struct{})
	var releaseOnce sync.Once
	releaseTask := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseTask) // 无论断言成败都不留卡死 goroutine（只关一次）

	if err := app.TryGoBackground("stuck-task", func(context.Context) {
		close(started)
		<-release // 不看 ctx：模拟卡死、永不自行退出的任务
	}); err != nil {
		t.Fatalf("GoBackground: %v", err)
	}
	<-started
	app.SetWaitTimeout(50 * time.Millisecond)

	cap := swapKVLogger(t)
	app.timedWait(context.Background())

	var drainWarn string
	for _, line := range cap.warnLines() {
		if strings.Contains(line, "drain timeout") {
			drainWarn = line
			break
		}
	}
	if drainWarn == "" {
		t.Fatalf("drain 超时必须打告警, captured: %v", cap.warnLines())
	}
	if !strings.Contains(drainWarn, "timeout=50ms") {
		t.Errorf("告警应保留 timeout 字段, got: %s", drainWarn)
	}
	if !strings.Contains(drainWarn, "running=1") {
		t.Errorf("告警必须带仍在运行的 goroutine 数量 running=1（#205 承诺）, got: %s", drainWarn)
	}

	// 对照：任务退出后再次 timedWait 应直接 drain，不再产生超时告警。
	releaseTask()
	before := len(cap.warnLines())
	app.timedWait(context.Background())
	for _, line := range cap.warnLines()[before:] {
		if strings.Contains(line, "drain timeout") {
			t.Errorf("任务已退出不应再报 drain 超时, got: %s", line)
		}
	}
}
