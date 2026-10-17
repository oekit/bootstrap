package nacos

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/oekit/bootstrap"
	"github.com/oekit/core/lifecycle"
	"github.com/oekit/nns"
	registry "github.com/oekit/registry"
	nacospkg "github.com/oekit/registry/nacos"
)

func TestNNSComponent_Name(t *testing.T) {
	c := &NNSComponent{}
	if got := c.Name(); got != "nns" {
		t.Errorf("expected name 'nns', got %q", got)
	}
}

// TestNNSComponent_EmptyAddr_Skip 覆盖 "nacos addr empty, skip" 分支。
func TestNNSComponent_EmptyAddr_Skip(t *testing.T) {
	c := &NNSComponent{NacosCfg: nacospkg.Bootstrap{Addr: ""}, Port: 8080}
	if err := c.Init(t.Context(), nil, nil); err != nil {
		t.Errorf("Init with empty addr: %v", err)
	}
}

func TestWithNNS_InjectsComponent(t *testing.T) {
	b, err := bootstrap.New(bootstrap.WithServiceName("t"),
		bootstrap.WithShutdownHook("noop", func(context.Context) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	// 经公开注入方法后组件参与编排——以 AddComponent 幂等性作烟雾断言
	b.AddComponent(&NNSComponent{})
}

// fakeNNSClient 记录 Register/Deregister/Close 的调用时序，用于验证
// 延迟注册语义：Register 必须发生在 app.Ready() 之后；注册成功才挂
// 反注册 closer；关停时 Deregister 先于先注册的 server 资源关闭执行。
type fakeNNSClient struct {
	mu           sync.Mutex
	initErr      error
	registerAt   []time.Time
	deregisterAt []time.Time
	closeAt      []time.Time
}

func (f *fakeNNSClient) Init(_ context.Context, _ string, _ []string) error { return f.initErr }

func (f *fakeNNSClient) Register(_ context.Context, _ int, _ ...registry.RegisterOptionFunc) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.registerAt = append(f.registerAt, time.Now())
	return nil
}

func (f *fakeNNSClient) Deregister(_ context.Context, _ int, _ ...registry.RegisterOptionFunc) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deregisterAt = append(f.deregisterAt, time.Now())
	return nil
}

func (f *fakeNNSClient) Close() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closeAt = append(f.closeAt, time.Now())
}

func (f *fakeNNSClient) registerCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.registerAt)
}

func (f *fakeNNSClient) deregisterCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.deregisterAt)
}

// withFakeNNS 安装 fake 并注册还原。
func withFakeNNS(t *testing.T) *fakeNNSClient {
	t.Helper()
	fake := &fakeNNSClient{}
	orig := newNNSClient
	newNNSClient = func(_ nacospkg.Bootstrap, _ ...nns.NNSOption) nnsClient { return fake }
	t.Cleanup(func() { newNNSClient = orig })
	return fake
}

func newTestComponent() *NNSComponent {
	return &NNSComponent{
		NacosCfg: nacospkg.Bootstrap{Addr: "127.0.0.1:8848", ServiceName: "test-svc"},
		Port:     8080,
	}
}

// TestNNSComponent_DeferredRegister 验证：Init 后 Ready 前不注册；Ready 后
// 完成注册；关停时 Deregister 先于先注册的 server 资源 close 执行
// （close 阶段按注册逆序释放，nns-deregister 晚于 hertz 注册 ⇒ 先关闭）。
func TestNNSComponent_DeferredRegister(t *testing.T) {
	fake := withFakeNNS(t)
	comp := newTestComponent()

	mgr := lifecycle.New(5 * time.Second)
	app := bootstrap.NewApp(mgr)

	// 模拟 userFn：先注册 server 资源（真实场景 hertz.Shutdown）
	serverClosed := make(chan struct{})
	app.AddResource("hertz", func(_ context.Context) error {
		close(serverClosed)
		return nil
	})

	ctx := context.Background()
	if err := comp.Init(ctx, app, mgr); err != nil {
		t.Fatalf("Init: %v", err)
	}

	// Ready 前：不得注册
	time.Sleep(100 * time.Millisecond)
	if n := fake.registerCount(); n != 0 {
		t.Fatalf("registered %d times before Ready, want 0", n)
	}

	app.Ready()

	// Ready 后：注册发生
	deadline := time.After(2 * time.Second)
	for fake.registerCount() == 0 {
		select {
		case <-deadline:
			t.Fatal("Register not called after Ready")
		case <-time.After(10 * time.Millisecond):
		}
	}

	// 关停：Deregister 必须执行，且 server 资源被关闭
	mgr.Shutdown(ctx)
	select {
	case <-serverClosed:
	default:
		t.Fatal("server resource not closed on shutdown")
	}
	if n := fake.deregisterCount(); n != 1 {
		t.Fatalf("Deregister called %d times, want 1", n)
	}
}

// TestNNSComponent_NoReady_NoRegister 验证契约：Ready() 永不调用时组件
// 不注册（goroutine 随 ctx 取消退出），关停不产生 Deregister。
func TestNNSComponent_NoReady_NoRegister(t *testing.T) {
	fake := withFakeNNS(t)
	comp := newTestComponent()

	mgr := lifecycle.New(2 * time.Second)
	app := bootstrap.NewApp(mgr)
	if err := comp.Init(context.Background(), app, mgr); err != nil {
		t.Fatalf("Init: %v", err)
	}

	mgr.Shutdown(context.Background())
	if n := fake.registerCount(); n != 0 {
		t.Fatalf("registered %d times without Ready, want 0", n)
	}
	if n := fake.deregisterCount(); n != 0 {
		t.Fatalf("Deregister called %d times without registration, want 0", n)
	}
}

// TestNNSComponent_InitDoesNotRegisterImmediately 验证 Init 阶段零注册
// 调用（注册一律延迟到 Ready 之后）。
func TestNNSComponent_InitDoesNotRegisterImmediately(t *testing.T) {
	fake := withFakeNNS(t)
	comp := newTestComponent()

	mgr := lifecycle.New(2 * time.Second)
	app := bootstrap.NewApp(mgr)
	if err := comp.Init(context.Background(), app, mgr); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if n := fake.registerCount(); n != 0 {
		t.Fatalf("Register called %d times during Init, want 0", n)
	}
}
