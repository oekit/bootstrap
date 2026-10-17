package bootstrap

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/cloudwego/hertz/pkg/common/test/assert"
	"github.com/oekit/core/lifecycle"
)

type mockComponent struct {
	name      string
	initErr   error
	initFn    func(ctx context.Context, app *App, mgr *lifecycle.Manager) error
	mu        sync.Mutex
	initCount int
	initOrder int
}

var (
	orderMu      sync.Mutex
	orderCounter int
)

func (m *mockComponent) Name() string { return m.name }

func (m *mockComponent) Init(ctx context.Context, app *App, mgr *lifecycle.Manager) error {
	m.mu.Lock()
	m.initCount++
	m.mu.Unlock()

	orderMu.Lock()
	orderCounter++
	m.initOrder = orderCounter
	orderMu.Unlock()

	if m.initFn != nil {
		return m.initFn(ctx, app, mgr)
	}
	return m.initErr
}

func resetOrderCounter() {
	orderMu.Lock()
	orderCounter = 0
	orderMu.Unlock()
}

func TestNew_RequiresServiceName(t *testing.T) {
	_, err := New()
	if err == nil {
		t.Fatal("expected error for missing ServiceName")
	}
}

func TestNew_WithServiceName_Success(t *testing.T) {
	b, err := New(WithServiceName("test"))
	assert.Nil(t, err)
	if b == nil {
		t.Fatal("expected non-nil Bootstrapper")
	}
}

func TestNew_WithComponent_Custom(t *testing.T) {
	resetOrderCounter()
	mc := &mockComponent{name: "custom"}

	b, _ := New(
		WithServiceName("test"),
		WithShutdownTimeout(100*time.Millisecond),
		WithComponent(mc),
	)

	err := b.Run(func(app *App) error { return nil })
	assert.Nil(t, err)

	if mc.initCount != 1 {
		t.Errorf("expected 1 init call, got %d", mc.initCount)
	}
}

func TestNew_ComponentsInitInOrder(t *testing.T) {
	resetOrderCounter()
	first := &mockComponent{name: "first"}
	second := &mockComponent{name: "second"}
	third := &mockComponent{name: "third"}

	b, _ := New(
		WithServiceName("test"),
		WithShutdownTimeout(100*time.Millisecond),
		WithComponent(first),
		WithComponent(second),
		WithComponent(third),
	)

	_ = b.Run(func(app *App) error { return nil })

	if first.initOrder != 1 || second.initOrder != 2 || third.initOrder != 3 {
		t.Errorf("init order wrong: first=%d, second=%d, third=%d",
			first.initOrder, second.initOrder, third.initOrder)
	}
}

func TestNew_ComponentInitError_StopsAndPropagates(t *testing.T) {
	failing := &mockComponent{
		name:    "failing",
		initErr: errors.New("component init failed"),
	}

	b, _ := New(
		WithServiceName("test"),
		WithShutdownTimeout(100*time.Millisecond),
		WithComponent(failing),
	)

	err := b.Run(func(app *App) error { return nil })
	if err == nil {
		t.Fatal("expected error from failing component")
	}
}

func TestNew_LocalMode_NoInfraComponents(t *testing.T) {
	b, _ := New(
		WithServiceName("local-test"),
		WithShutdownTimeout(100*time.Millisecond),
	)

	called := false
	err := b.Run(func(app *App) error {
		called = true
		return nil
	})
	assert.Nil(t, err)
	assert.DeepEqual(t, true, called)
}

func TestNew_WithShutdownHook(t *testing.T) {
	hookCalled := false

	b, _ := New(
		WithServiceName("test"),
		WithShutdownTimeout(100*time.Millisecond),
		WithShutdownHook("custom-cleanup", func(_ context.Context) error {
			hookCalled = true
			return nil
		}),
	)

	_ = b.Run(func(app *App) error { return nil })

	if !hookCalled {
		t.Error("shutdown hook was not called")
	}
}

var _ = lifecycle.New

// TestNew_ComponentInitError_RollsBackEarlierComponents 回归测试（W1-T4）：
// 后续组件 Init 失败时，必须执行已初始化组件注册的关闭钩子（回滚），
// 否则 tracing exporter 等资源会泄漏到进程结束。
func TestNew_ComponentInitError_RollsBackEarlierComponents(t *testing.T) {
	hookRan := false
	first := &mockComponent{
		name: "first",
		initFn: func(_ context.Context, _ *App, mgr *lifecycle.Manager) error {
			mgr.Add("first-cleanup", func(context.Context) error {
				hookRan = true
				return nil
			}, 0)
			return nil
		},
	}
	failing := &mockComponent{name: "failing", initErr: errors.New("boom")}

	b, _ := New(
		WithServiceName("test"),
		WithShutdownTimeout(100*time.Millisecond),
		WithComponent(first),
		WithComponent(failing),
	)

	err := b.Run(func(app *App) error { return nil })
	if err == nil {
		t.Fatal("expected error from failing component")
	}
	if !hookRan {
		t.Fatal("expected rollback to run shutdown hooks of earlier components")
	}
}

// TestNew_ComponentInitPanic_ConvertedToError 回归测试（W1-T4）：
// 组件 Init panic 必须转为 error 并走回滚路径，而不是击穿 Bootstrapper.Run。
func TestNew_ComponentInitPanic_ConvertedToError(t *testing.T) {
	hookRan := false
	first := &mockComponent{
		name: "first",
		initFn: func(_ context.Context, _ *App, mgr *lifecycle.Manager) error {
			mgr.Add("first-cleanup", func(context.Context) error {
				hookRan = true
				return nil
			}, 0)
			return nil
		},
	}
	panicking := &mockComponent{
		name: "panicking",
		initFn: func(context.Context, *App, *lifecycle.Manager) error {
			panic("init exploded")
		},
	}

	b, _ := New(
		WithServiceName("test"),
		WithShutdownTimeout(100*time.Millisecond),
		WithComponent(first),
		WithComponent(panicking),
	)

	err := b.Run(func(app *App) error { return nil })
	if err == nil {
		t.Fatal("expected error from panicking component")
	}
	if !hookRan {
		t.Fatal("expected rollback to run shutdown hooks of earlier components")
	}
}
