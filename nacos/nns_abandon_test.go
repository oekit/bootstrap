package nacos

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/oekit/bootstrap"
	"github.com/oekit/core/lifecycle"
	"github.com/oekit/nns"
	registry "github.com/oekit/registry"
	nacospkg "github.com/oekit/registry/nacos"
)

// abandonFakeClient 模拟「慢 Nacos 下注册 in-flight 时关停」的时序（#136）：
// Register 已到达服务端并生效（gRPC 取消不回滚服务端状态），随后因根 ctx
// 取消返回 context.Canceled；Deregister 记录是否被 best-effort 调用、拿到
// 的 ctx 是否带独立 deadline。
type abandonFakeClient struct {
	fakeNNSClient

	enterOnce sync.Once
	entered   chan struct{}

	mu           sync.Mutex
	serverHas    bool  // 服务端已提交注册（取消不回滚）
	deregistered bool  // best-effort 反注册是否执行且成功
	deregHadDdl  bool  // 反注册拿到的 ctx 是否带 deadline（独立短超时）
	deregErr     error // 可注入反注册失败
}

func (f *abandonFakeClient) Register(ctx context.Context, _ int, _ ...registry.RegisterOptionFunc) error {
	f.enterOnce.Do(func() { close(f.entered) })
	f.mu.Lock()
	f.serverHas = true // 请求已到服务端并生效
	f.mu.Unlock()
	<-ctx.Done() // in-flight 中被关停取消
	return ctx.Err()
}

func (f *abandonFakeClient) Deregister(ctx context.Context, _ int, _ ...registry.RegisterOptionFunc) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.deregErr != nil {
		return f.deregErr
	}
	_, ok := ctx.Deadline()
	f.deregHadDdl = ok
	f.deregistered = true
	f.serverHas = false
	return nil
}

func (f *abandonFakeClient) snapshot() (serverHas, deregistered, deregHadDdl bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.serverHas, f.deregistered, f.deregHadDdl
}

// withAbandonFake 安装 abandonFakeClient 并注册还原。
func withAbandonFake(t *testing.T, deregErr error) *abandonFakeClient {
	t.Helper()
	fake := &abandonFakeClient{entered: make(chan struct{}), deregErr: deregErr}
	orig := newNNSClient
	newNNSClient = func(_ nacospkg.Bootstrap, _ ...nns.NNSOption) nnsClient { return fake }
	t.Cleanup(func() { newNNSClient = orig })
	return fake
}

// waitAbandoned 轮询直到状态落 PhaseAbandoned（或超时 fatal）。
func waitAbandoned(t *testing.T, comp *NNSComponent) *RegisterState {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		st := comp.RegistrationState()
		if st != nil && st.Phase == PhaseAbandoned {
			return st
		}
		if time.Now().After(deadline) {
			t.Fatalf("状态应落 PhaseAbandoned, got %+v", st)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestNNSComponent_RegisterCancelled_BestEffortDeregister 钉住 #136 主路径：
// Ready 后 Register in-flight 中触发关停（根 ctx 取消）时——
//   - 必须用独立短超时 ctx best-effort 调一次 Deregister（此前错误分支直接
//     return，反注册 closer 只在注册成功后挂，nns-deregister 永远不会执行，
//     幽灵实例存活到 nacos ephemeral 心跳超时）；
//   - 状态落 PhaseAbandoned（与 PhaseFailed「明确失败」和「从未注册」可
//     区分），DeregisterErr 为 nil（反注册成功）。
func TestNNSComponent_RegisterCancelled_BestEffortDeregister(t *testing.T) {
	fake := withAbandonFake(t, nil)
	comp := newTestComponent()
	comp.ReadyTimeout = time.Hour // 本用例不触发看门狗

	mgr := lifecycle.New(2 * time.Second)
	app := bootstrap.NewApp(mgr)
	if err := comp.Init(context.Background(), app, mgr); err != nil {
		t.Fatalf("Init: %v", err)
	}

	app.Ready()
	select {
	case <-fake.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("Register 未发起")
	}

	// 模拟 SIGTERM：关停取消根 ctx，Register in-flight 被取消
	app.RequestShutdown()

	st := waitAbandoned(t, comp)
	serverHas, deregistered, deregHadDdl := fake.snapshot()
	if !deregistered {
		t.Fatal("Register 被取消后未 best-effort 反注册（#136）——幽灵实例窗口")
	}
	if !deregHadDdl {
		t.Fatal("best-effort 反注册应使用带独立 deadline 的短超时 ctx，而非已取消的根 ctx（#136）")
	}
	if serverHas {
		t.Fatal("反注册成功后服务端不应再有实例")
	}
	if st.DeregisterErr != nil {
		t.Fatalf("反注册成功时 DeregisterErr 应为 nil, got %v", st.DeregisterErr)
	}
	if !errors.Is(st.Err, context.Canceled) {
		t.Fatalf("Err 应可 errors.Is(context.Canceled), got %v", st.Err)
	}
	if st.Port != 8080 {
		t.Errorf("状态快照应携带 Port, got %+v", st)
	}

	mgr.Shutdown(context.Background())
}

// TestNNSComponent_RegisterCancelled_DeregisterFailureRecorded 钉住 #136 的
// 反注册失败面：best-effort 反注册失败时错误记入 RegisterState.DeregisterErr
// 并打 ERROR 留痕（幽灵实例窗口可被巡检发现），状态仍为 PhaseAbandoned，
// 且不阻塞/不影响关停序列。
func TestNNSComponent_RegisterCancelled_DeregisterFailureRecorded(t *testing.T) {
	deregBoom := errors.New("nacos down")
	fake := withAbandonFake(t, deregBoom)
	comp := newTestComponent()
	comp.ReadyTimeout = time.Hour

	mgr := lifecycle.New(2 * time.Second)
	app := bootstrap.NewApp(mgr)
	if err := comp.Init(context.Background(), app, mgr); err != nil {
		t.Fatalf("Init: %v", err)
	}

	app.Ready()
	select {
	case <-fake.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("Register 未发起")
	}
	app.RequestShutdown()

	st := waitAbandoned(t, comp)
	if st.DeregisterErr == nil || !errors.Is(st.DeregisterErr, deregBoom) {
		t.Fatalf("反注册失败应记入 DeregisterErr, got %v（#136）", st.DeregisterErr)
	}
	if serverHas, _, _ := fake.snapshot(); !serverHas {
		t.Fatal("反注册失败时服务端实例仍在（幽灵窗口应可由状态面发现）")
	}

	mgr.Shutdown(context.Background())
}

// TestNNSComponent_RegisterFailed_PlainError_NoDeregister 对照组：明确失败
// （非 ctx 取消）保持既有语义——PhaseFailed、不触发反注册（客户端明确拿到
// 失败响应，未产生注册，无需反注册）。
func TestNNSComponent_RegisterFailed_PlainError_NoDeregister(t *testing.T) {
	fake := withFailingRegisterNNS(t, errors.New("nacos unreachable"))
	comp := newTestComponent() // Required=false：软依赖语义
	comp.ReadyTimeout = time.Hour

	mgr := lifecycle.New(2 * time.Second)
	app := bootstrap.NewApp(mgr)
	if err := comp.Init(context.Background(), app, mgr); err != nil {
		t.Fatalf("Init: %v", err)
	}

	app.Ready()
	deadline := time.After(3 * time.Second)
	for fake.registered.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("Register not attempted after Ready")
		case <-time.After(10 * time.Millisecond):
		}
	}

	st := comp.RegistrationState()
	if st == nil || st.Phase != PhaseFailed {
		t.Fatalf("明确失败应落 PhaseFailed, got %+v", st)
	}
	if n := fake.deregisterCount(); n != 0 {
		t.Fatalf("明确失败不应触发反注册, got %d 次（#136）", n)
	}
	if st.DeregisterErr != nil {
		t.Fatalf("PhaseFailed 不应携带 DeregisterErr, got %v", st.DeregisterErr)
	}

	mgr.Shutdown(context.Background())
}
