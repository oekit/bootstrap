package bootstrap

import (
	"strings"
	"testing"
	"time"
)

// TestRun_UserFnPanicReturnsError 回归测试：userFn panic 此前被 async.GoSafe
// 的外层 recover 静默吞掉，errCh 永远等不到值，select 在无信号环境下永久
// 阻塞（进程假活）。panic 现在转为 error 返回并触发正常关闭。
func TestRun_UserFnPanicReturnsError(t *testing.T) {
	b, err := New(
		WithServiceName("panic-test"),
		WithShutdownTimeout(500*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		done <- b.Run(func(app *App) error {
			panic("boom")
		})
	}()

	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "panicked") {
			t.Fatalf("expected panic converted to error, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after userFn panic — process would hang forever")
	}
}
