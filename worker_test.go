package bootstrap

import (
	"context"
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
