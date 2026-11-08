package bootstrap

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oekit/core/lifecycle"
)

// TestRun_WaitTimeoutZero_WaitsBeyondShutdownTimeout 钉住 #134（全链路面）：
// SetWaitTimeout(0) 承诺「无限等待」，wait job 必须不被 lifecycle.Manager 的
// 默认 job 预算（Bootstrapper 下即 WithShutdownTimeout）静默打断——此前
// wait job 以 timeout=0 注册（⇒ Manager 默认预算），预算到点 ctx.Done 触发、
// drain 被放弃，「0 = 无限等待」只有在同时 WithShutdownTimeout(0) 时才成立
// 且耦合未记录。本用例预算 200ms、后台任务 800ms 后放行：Run 必须等到任务
// 退出才返回（突破预算），退出码为 0（正常 drain 完成，非超时放弃）。
func TestRun_WaitTimeoutZero_WaitsBeyondShutdownTimeout(t *testing.T) {
	b, err := New(
		WithServiceName("wait-zero-svc"),
		WithShutdownTimeout(200*time.Millisecond), // wait job 若仍吃默认预算，drain 会被砍到 200ms
	)
	if err != nil {
		t.Fatal(err)
	}

	release := make(chan struct{})
	var bgDone atomic.Bool
	start := time.Now()
	runDone := make(chan error, 1)
	go func() {
		runDone <- b.Run(func(a *App) error {
			a.SetWaitTimeout(0) // 文档承诺：无限等待
			a.GoBackground("hanger", func(_ context.Context) {
				<-release
				bgDone.Store(true)
			})
			return nil // userFn 立即返回 → 触发关停序列
		})
	}()

	// 在预算（200ms）到点之后才放行后台任务
	tr := time.AfterFunc(800*time.Millisecond, func() { close(release) })
	defer tr.Stop()

	select {
	case err := <-runDone:
		if elapsed := time.Since(start); elapsed < 800*time.Millisecond {
			t.Fatalf("Run 在 %v 后返回——waitTimeout=0 被 wait job 预算静默截断（#134）", elapsed)
		}
		if !bgDone.Load() {
			t.Fatal("drain 未等待后台任务完成即放弃（#134）")
		}
		if err != nil {
			t.Fatalf("正常 drain 完成后 Run 应返回 nil, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run 挂死")
	}
}

// TestRun_DefaultWaitTimeout_NotCappedByShutdownTimeout 钉住 #134 的另一半：
// 默认 waitTimeout（30s）同样不被 WithShutdownTimeout 截断——预算 150ms、
// 后台任务在 ctx 取消后 300ms 退出（ < 30s drain），Run 必须等它退出。
func TestRun_DefaultWaitTimeout_NotCappedByShutdownTimeout(t *testing.T) {
	b, err := New(
		WithServiceName("wait-default-svc"),
		WithShutdownTimeout(150*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	runDone := make(chan error, 1)
	go func() {
		runDone <- b.Run(func(a *App) error {
			// 后台任务感知 ctx 取消，但故意拖 300ms（超过 150ms 预算，
			// 远小于 30s 默认 drain）才退出
			a.GoBackground("slow-exit", func(ctx context.Context) {
				<-ctx.Done()
				time.Sleep(300 * time.Millisecond)
			})
			return nil
		})
	}()

	select {
	case err := <-runDone:
		if elapsed := time.Since(start); elapsed < 300*time.Millisecond {
			t.Fatalf("Run 在 %v 后返回——默认 30s drain 被预算砍到 150ms（#134）", elapsed)
		}
		if err != nil {
			t.Fatalf("Run 应返回 nil, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run 挂死")
	}
}
