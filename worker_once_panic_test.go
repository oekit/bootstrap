package bootstrap

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestWorkerManager_PanicLoggedWithTaskName 钉住 #256：AddOnce 与 AddTicker
// 两个注册入口的 panic 留痕口径必须一致——都就地收编为带 task 字段的 ERROR
// 日志。此前 AddOnce 没有内层 recover，panic 直落 async.GoSafe 出口的全局
// handler（缺省 stdlib log、不带任务标识）：同一 WorkerManager 里 AddTicker
// 能按 task 定位、AddOnce 只有一条无名 panic 栈，排障无法回答「是哪个 once
// 任务炸的」。
func TestWorkerManager_PanicLoggedWithTaskName(t *testing.T) {
	cap := swapKVLogger(t)
	wm := NewWorkerManager(t.Context())

	if err := wm.AddOnce("once-boom", func(context.Context) {
		panic("once boom")
	}); err != nil {
		t.Fatalf("AddOnce: %v", err)
	}
	if err := wm.AddTicker("tick-boom", time.Millisecond, func(context.Context) {
		panic("tick boom")
	}); err != nil {
		t.Fatalf("AddTicker: %v", err)
	}
	// 给 tick 至少一轮触发窗口；Close 关闸并等存量任务退出（panic 已被
	// 就地收编，wg.Done 照常执行，Close 不会被 panic 卡住）
	time.Sleep(50 * time.Millisecond)
	wm.Close()

	lines := cap.warnLines()
	for _, task := range []string{"once-boom", "tick-boom"} {
		var found bool
		for _, line := range lines {
			if strings.Contains(line, "background task panicked") && strings.Contains(line, "task="+task) {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("%s 的 panic 必须打带 task 字段的 ERROR 日志（#256），captured: %v", task, lines)
		}
	}
}
