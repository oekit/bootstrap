package bootstrap

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oekit/core/lifecycle"
)

// TestCloseAll_AggregatesErrors 钉住 #80（单元面）：资源关闭失败的错误必须被
// closeAll 聚合返回——不再只打日志后丢弃。
func TestCloseAll_AggregatesErrors(t *testing.T) {
	a := NewApp(lifecycle.New(2 * time.Second))
	boom := errors.New("flush failed")
	a.AddResource("bad", func(context.Context) error { return boom })
	a.AddResource("good", func(context.Context) error { return nil })

	err := a.closeAll(context.Background())
	if err == nil {
		t.Fatal("closeAll 必须聚合关闭错误（#80）")
	}
	if !errors.Is(err, boom) {
		t.Fatalf("聚合错误应 errors.Is 可辨原始错误, got %v", err)
	}
	if !strings.Contains(err.Error(), "bad") {
		t.Fatalf("聚合错误应带资源名, got %v", err)
	}
}

// TestRun_ShutdownErrorsSurfaceInReturn 钉住 #80（端到端面）：资源关闭失败时
// Run 的返回值非 nil——进程退出码反映关停质量，不再「带着未落盘数据退出、
// 退出码 0」。
func TestRun_ShutdownErrorsSurfaceInReturn(t *testing.T) {
	b, err := New(WithServiceName("shutdown-err-svc"))
	if err != nil {
		t.Fatal(err)
	}
	err = b.Run(func(a *App) error {
		a.AddResource("bad-closer", func(context.Context) error {
			return errors.New("db flush failed")
		})
		// userFn 立即返回：触发正常关停序列（close 阶段跑 bad-closer）
		return nil
	})
	if err == nil {
		t.Fatal("资源关闭失败时 Run 必须返回非 nil（#80）")
	}
	if !strings.Contains(err.Error(), "db flush failed") {
		t.Fatalf("返回值应携带关闭错误, got %v", err)
	}
}

// TestApp_CloserPanic_IsolatedAndChainContinues 钉住 #132：closeAll 必须有
// 逐 closer panic 隔离——某资源 Close 在停机竞态下 panic（nil 解引用之类）
// 时转为错误并继续逆序链，其后的反注册/flush 不再被跳过。此前 panic 冲出
// 循环，仅靠 close job 级 recover 兜住不炸进程，其后全部 closer 漏调——
// 幽灵实例正是 closeAll 注释声称要消灭的东西。
func TestApp_CloserPanic_IsolatedAndChainContinues(t *testing.T) {
	mgr := lifecycle.New(2 * time.Second)
	app := NewApp(mgr)

	var afterPanicRan, lastRan atomic.Bool
	app.AddResource("after-panic", func(context.Context) error {
		afterPanicRan.Store(true)
		return nil
	})
	app.AddResource("panics", func(context.Context) error {
		panic("nil deref in teardown race")
	})
	app.AddResource("last", func(context.Context) error {
		lastRan.Store(true)
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
		t.Fatal("shutdown 挂死：closer panic 击穿 closeAll（#132）")
	}

	if !afterPanicRan.Load() || !lastRan.Load() {
		t.Fatal("closer panic 中断了逆序停机链，其后的 closer 被跳过（#132）")
	}
	// panic 转错误经 #80 链路上抛（退出码可感知），且带资源名。
	// #182：panic 值经 pkg/errors.Recovered 收编，文案为
	// "panic: <类型>: <值>"（资源名由 closeAll 的 "%s: %w" 包装携带）。
	errs := mgr.Errors()
	if len(errs) == 0 {
		t.Fatal("closer panic 应转为关闭错误并上抛（#132）")
	}
	joined := false
	for _, err := range errs {
		if strings.Contains(err.Error(), "panics") && strings.Contains(err.Error(), "panic:") {
			joined = true
		}
	}
	if !joined {
		t.Fatalf("panic 错误应带资源名与 panic 标记, got %v", errs)
	}
}
