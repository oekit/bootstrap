package bootstrap

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// TestWorkerManager_TickerPanicDoesNotKillLoop 回归：ticker 回调裸调时
// 一次 panic 就永久打死该周期任务（GoSafe 的 recover 在 goroutine 出口）。
// 钉住：panic 后后续 tick 继续执行。
func TestWorkerManager_TickerPanicDoesNotKillLoop(t *testing.T) {
	wm := NewWorkerManager(context.Background())
	defer wm.Close()

	var runs atomic.Int32
	wm.AddTicker("panicky", 10*time.Millisecond, func(ctx context.Context) {
		if runs.Add(1) == 1 {
			panic("first tick boom")
		}
	})

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && runs.Load() < 2 {
		time.Sleep(10 * time.Millisecond)
	}
	if got := runs.Load(); got < 2 {
		t.Fatalf("ticker died after first panic: runs=%d", got)
	}
}

// TestWorkerManager_AddTickerRejectsNonPositiveInterval 钉住 #101：interval
// 非正必须在注册入口被拒绝。time.NewTimer/NewTicker 对非正 interval panic，
// 且 panic 位置在内层 recover（只包住 fn）之外、被 GoSafe 出口 recover 兜住，
// 旧实现表现为：wg 计数平衡、调用方无报错、任务从未运行且无日志。
func TestWorkerManager_AddTickerRejectsNonPositiveInterval(t *testing.T) {
	wm := NewWorkerManager(context.Background())
	defer wm.Close()

	for _, d := range []time.Duration{0, -time.Second} {
		err := wm.AddTicker("bad", d, func(context.Context) {})
		if err == nil {
			t.Fatalf("interval=%v 必须被拒绝注册（#101）", d)
		}
		if !errors.Is(err, ErrInvalidInterval) {
			t.Fatalf("interval=%v 应返回 ErrInvalidInterval, got %v", d, err)
		}
	}
}

// TestWorkerManager_AddAfterCloseReturnsError 钉住 #103：Close 后 AddTicker/
// AddOnce 的注册从「静默吞掉（无返回值、无日志）」改为显式拒绝——返回
// ErrManagerClosed 并打 Warn 留痕。组件 Init 恰好在关停竞态窗口注册时，
// 不再是「任务永不执行且无任何线索」。
func TestWorkerManager_AddAfterCloseReturnsError(t *testing.T) {
	wm := NewWorkerManager(context.Background())
	wm.Close()

	var ran atomic.Bool
	if err := wm.AddTicker("late", time.Second, func(context.Context) { ran.Store(true) }); !errors.Is(err, ErrManagerClosed) {
		t.Fatalf("AddTicker after Close = %v, want ErrManagerClosed（#103）", err)
	}
	if err := wm.AddOnce("late-once", func(context.Context) { ran.Store(true) }); !errors.Is(err, ErrManagerClosed) {
		t.Fatalf("AddOnce after Close = %v, want ErrManagerClosed（#103）", err)
	}

	time.Sleep(20 * time.Millisecond)
	if ran.Load() {
		t.Fatal("Close 后注册的任务不应执行（#103）")
	}
}

// TestWorkerManager_SlowTaskNoBackToBack 钉住 #98 缺口 1：fn 耗时超过
// interval 时不得背靠背补跑。旧实现 ticker 通道缓冲 1，fn 阻塞期间堆积
// 恰好一枚滴答，fn 返回后 select 立即选中它——慢任务周期性背靠背空转
// （24h 间隔的 gc 类任务跑超时后马上又来一轮）。完成驱动节拍（fn 返回后
// 重新起表）后，下一轮距上次完成至少一个 interval。
func TestWorkerManager_SlowTaskNoBackToBack(t *testing.T) {
	wm := NewWorkerManager(context.Background())
	defer wm.Close()

	const interval = 80 * time.Millisecond
	started := make(chan struct{}, 4)
	fnReturned := make(chan struct{}, 4)
	var runs atomic.Int32
	release := make(chan struct{})
	if err := wm.AddTicker("slow", interval, func(context.Context) {
		runs.Add(1)
		started <- struct{}{}
		<-release // 把 fn 挂起远超 interval，期间堆积恰好 1 枚滴答
		fnReturned <- struct{}{}
	}); err != nil {
		t.Fatalf("AddTicker: %v", err)
	}

	<-started // 第一轮开始
	time.Sleep(3 * interval)
	close(release) // 放行全部轮次
	<-fnReturned   // 第一轮返回（旧实现此刻会立即背靠背补跑堆积滴答）

	// 完成驱动：fn 返回后半个 interval 内不得开始新的一轮。
	// 旧实现的堆积滴答在 fn 返回后 µs 级被 select 选中，判别窗口充足。
	time.Sleep(interval / 2)
	if got := runs.Load(); got != 1 {
		t.Fatalf("fn 返回后半个 interval 内出现背靠背执行: runs=%d, want 1（#98）", got)
	}

	// 循环仍活着：下一轮应在 fn 返回 + interval 附近出现。
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && runs.Load() < 2 {
		time.Sleep(5 * time.Millisecond)
	}
	if got := runs.Load(); got < 2 {
		t.Fatal("完成驱动节拍下后续 tick 未再执行——周期任务死了（#98）")
	}
}

// TestWorkerManager_CloseTimeout 钉住 #98 缺口 2：fn 不感知 ctx 时 Close 死等
// 无超时、停机序列永久挂死。CloseTimeout(d) 超时后放弃等待并 ERROR 留痕。
func TestWorkerManager_CloseTimeout(t *testing.T) {
	wm := NewWorkerManager(context.Background())

	release := make(chan struct{})
	defer close(release) // 收尾放行被放弃的任务，避免跨用例泄漏
	if err := wm.AddOnce("ignores-ctx", func(context.Context) {
		<-release // 模拟不感知 ctx 的网络 IO / LLM 调用
	}); err != nil {
		t.Fatalf("AddOnce: %v", err)
	}

	start := time.Now()
	wm.CloseTimeout(50 * time.Millisecond)
	elapsed := time.Since(start)

	if elapsed < 40*time.Millisecond {
		t.Fatalf("CloseTimeout 未等满预算即返回: elapsed=%v（#98）", elapsed)
	}
	if elapsed > 3*time.Second {
		t.Fatalf("CloseTimeout 仍在死等: elapsed=%v（#98）", elapsed)
	}
}

// TestWorkerManager_CloseTimeout_NonPositiveWaitsLikeClose 钉住
// CloseTimeout 的 d<=0 分支：等同 Close，等待所有任务退出——由 ctx-aware
// 任务在 cancel 后退出验证（Close 本身的无限等待语义不变，向后兼容）。
func TestWorkerManager_CloseTimeout_NonPositiveWaitsLikeClose(t *testing.T) {
	wm := NewWorkerManager(context.Background())
	if err := wm.AddTicker("ctx-aware", time.Hour, func(ctx context.Context) {
		<-ctx.Done()
	}); err != nil {
		t.Fatalf("AddTicker: %v", err)
	}

	start := time.Now()
	wm.CloseTimeout(0) // d<=0 → 无限等待路径
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("d<=0 应等同 Close（等 ctx-aware 任务退出）: elapsed=%v", elapsed)
	}
}
