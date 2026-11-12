package bootstrap

import (
	"context"
	"errors"
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
			goBackgroundOrFail(t, a, "hanger", func(_ context.Context) {
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
			goBackgroundOrFail(t, a, "slow-exit", func(ctx context.Context) {
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

// TestApp_AddResourceTimeout_NotCappedByCloseJobBudget 钉住 #135：独立预算
// 不被 close job 预算（WithShutdownTimeout，默认 10s）封顶——octopus 停机
// 编排场景：close job 预算先被前序资源耗尽后，预算化资源仍拿到完整个人
// 预算，其后的资源也仍被执行。此前 runCloserWithBudget 挂 AfterFunc(parent)，
// close job 预算到点同步取消所有预算化 closer，「outbox 排空 20s」在默认
// 配置下只拿到 10s 且无任何报错。
func TestApp_AddResourceTimeout_NotCappedByCloseJobBudget(t *testing.T) {
	mgr := lifecycle.New(150 * time.Millisecond) // close job 预算（模拟默认 10s 被前序资源耗尽的形态）
	app := NewApp(mgr)

	start := time.Now()
	var outboxElapsed atomic.Int64
	var afterRan atomic.Bool
	// 注册序 = [nns-deregister, outbox] ⇒ 逆序执行 = outbox（预算化、慢）先跑，
	// nns-deregister 在 close job 预算到点之后执行——正是 octopus 编排形态。
	app.AddResource("nns-deregister", func(context.Context) error {
		afterRan.Store(true) // close job 预算到点后仍必须执行
		return nil
	})
	app.AddResourceTimeout("outbox", 600*time.Millisecond, func(ctx context.Context) error {
		<-ctx.Done() // 预算到点（而非 close job 预算）才被切断
		outboxElapsed.Store(int64(time.Since(start)))
		return ctx.Err()
	})

	done := make(chan struct{})
	go func() {
		mgr.Shutdown(context.Background())
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown 挂死")
	}

	if got := time.Duration(outboxElapsed.Load()); got < 600*time.Millisecond {
		t.Fatalf("outbox closer 只跑了 %v——独立预算被 close job 预算封顶（#135）", got)
	}
	if !afterRan.Load() {
		t.Fatal("close job 预算到点后，后续资源的 closer 未执行（#135）")
	}
	// outbox 超自己预算的错误仍经 #80 链路上抛（退出码可感知）
	errs := mgr.Errors()
	if len(errs) == 0 || !errors.Is(errs[0], context.DeadlineExceeded) {
		t.Fatalf("outbox 超预算错误应上抛, got %v", errs)
	}
}
