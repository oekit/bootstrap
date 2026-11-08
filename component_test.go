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

// serviceNameAwareComponent 是 ServiceNameAware 的最小实现，记录回填次数
// 与取值（幂等实现：SetServiceName 只覆写同名字段）。
type serviceNameAwareComponent struct {
	mockComponent
	mu       sync.Mutex
	svcName  string
	setCalls int
}

func (m *serviceNameAwareComponent) SetServiceName(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.svcName = name
	m.setCalls++
}

func (m *serviceNameAwareComponent) snapshot() (string, int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.svcName, m.setCalls
}

// TestAddComponent_AfterNew_BackfillsServiceName 钉住 #131：New 之后经
// AddComponent 挂载的 ServiceNameAware 组件必须就地回填服务名——此前
// 回填只发生在 New 内部，后挂组件静默拿到空服务名且无任何信号，与
// 「与 WithComponent 等价」的注释承诺不符。
func TestAddComponent_AfterNew_BackfillsServiceName(t *testing.T) {
	b, err := New(WithServiceName("late-svc"))
	if err != nil {
		t.Fatal(err)
	}
	aware := &serviceNameAwareComponent{mockComponent: mockComponent{name: "aware"}}
	b.AddComponent(aware)

	name, calls := aware.snapshot()
	if name != "late-svc" {
		t.Fatalf("New 后挂载的组件应回填服务名, got %q (#131)", name)
	}
	if calls != 1 {
		t.Fatalf("SetServiceName 调用次数 = %d, want 1", calls)
	}
}

// TestAddComponent_BeforeServiceName_FilledByNew 钉住选项路径：New 之前
// 挂载（WithComponent 先于 WithServiceName）时由 New 的统一回填兜底
// （既有行为回归钉），且与 AddComponent 的就地回填叠加后同值幂等。
func TestAddComponent_BeforeServiceName_FilledByNew(t *testing.T) {
	aware := &serviceNameAwareComponent{mockComponent: mockComponent{name: "aware"}}
	// WithServiceName 在前：AddComponent 就地回填 + New 统一回填（幂等，两次同值）
	if _, err := New(WithServiceName("opt-svc"), WithComponent(aware)); err != nil {
		t.Fatal(err)
	}
	name, _ := aware.snapshot()
	if name != "opt-svc" {
		t.Fatalf("选项路径组件应拿到服务名, got %q", name)
	}

	aware2 := &serviceNameAwareComponent{mockComponent: mockComponent{name: "aware2"}}
	// WithComponent 在前：AddComponent 时 serviceName 未定，由 New 回填
	if _, err := New(WithComponent(aware2), WithServiceName("opt-svc-2")); err != nil {
		t.Fatal(err)
	}
	name2, calls2 := aware2.snapshot()
	if name2 != "opt-svc-2" {
		t.Fatalf("WithComponent 先于 WithServiceName 时应由 New 回填, got %q", name2)
	}
	if calls2 != 1 {
		t.Fatalf("SetServiceName 调用次数 = %d, want 1", calls2)
	}
}
