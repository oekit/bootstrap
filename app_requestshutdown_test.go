package bootstrap

import (
	"testing"
	"time"

	"github.com/oekit/core/lifecycle"
)

// TestApp_RequestShutdownCancelsContext 验证 RequestShutdown 取消根 context
// （解除 userFn 对 Context().Done() 的阻塞）且幂等。
func TestApp_RequestShutdownCancelsContext(t *testing.T) {
	app := NewApp(lifecycle.New(time.Second))

	app.RequestShutdown()

	select {
	case <-app.Context().Done():
	case <-time.After(time.Second):
		t.Fatal("RequestShutdown did not cancel app context")
	}

	// 重复调用不 panic、不再有副作用
	app.RequestShutdown()
}
