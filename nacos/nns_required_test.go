package nacos

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oekit/bootstrap"
	"github.com/oekit/core/lifecycle"
	"github.com/oekit/nns"
	registry "github.com/oekit/registry"
	nacospkg "github.com/oekit/registry/nacos"
)

// registerFailingClient 包装 fakeNNSClient，让 Register 恒失败并记录尝试次数
// （nns#77 的注册失败场景）。
type registerFailingClient struct {
	fakeNNSClient
	regErr     error
	registered atomic.Int32
}

func (f *registerFailingClient) Register(_ context.Context, _ int, _ ...registry.RegisterOptionFunc) error {
	f.registered.Add(1)
	return f.regErr
}

// withFailingRegisterNNS 安装「Init 成功、Register 失败」的 fake 并注册还原。
func withFailingRegisterNNS(t *testing.T, regErr error) *registerFailingClient {
	t.Helper()
	fake := &registerFailingClient{regErr: regErr}
	orig := newNNSClient
	newNNSClient = func(_ nacospkg.Bootstrap, _ ...nns.NNSOption) nnsClient { return fake }
	t.Cleanup(func() { newNNSClient = orig })
	return fake
}

// TestNNSComponent_RequiredRegisterFail_ShutsDown 钉住 nns#77：Required=true 时
// 注册失败（Ready 之后的延迟动作）必须 RequestShutdown 中止启动——此前只打
// ERROR 并置 PhaseFailed，强依赖服务在未注册状态下继续跑（假活）。
func TestNNSComponent_RequiredRegisterFail_ShutsDown(t *testing.T) {
	fake := withFailingRegisterNNS(t, errors.New("nacos unreachable"))
	comp := newTestComponent()
	comp.Required = true

	mgr := lifecycle.New(2 * time.Second)
	app := bootstrap.NewApp(mgr)
	if err := comp.Init(context.Background(), app, mgr); err != nil {
		t.Fatalf("Init: %v", err)
	}

	app.Ready()

	// 等 Register 被尝试并失败
	deadline := time.After(3 * time.Second)
	for fake.registered.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("Register not attempted after Ready")
		case <-time.After(10 * time.Millisecond):
		}
	}

	// fail-fast：根 ctx 被 RequestShutdown 取消（userFn 因此解除阻塞、
	// 走优雅关停——启动语境下即中止启动）
	select {
	case <-app.Context().Done():
	case <-time.After(2 * time.Second):
		t.Fatal("Required=true + Register 失败未触发 RequestShutdown（强依赖未 fail-fast）")
	}

	if st := comp.RegistrationState(); st == nil || st.Phase != PhaseFailed {
		t.Fatalf("注册失败后状态应为 PhaseFailed, got %+v", st)
	}

	mgr.Shutdown(context.Background())
}

// TestNNSComponent_RequiredRegisterFail_SoftDependency_KeepsRunning 对照组：
// Required=false（软依赖）时注册失败只记 ERROR + PhaseFailed，不触发关停
// （既有软降级语义不变）。
func TestNNSComponent_RequiredRegisterFail_SoftDependency_KeepsRunning(t *testing.T) {
	fake := withFailingRegisterNNS(t, errors.New("nacos unreachable"))
	comp := newTestComponent() // Required 默认 false

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

	// 软依赖：不关停，根 ctx 保持活跃
	select {
	case <-app.Context().Done():
		t.Fatal("Required=false 时注册失败不应触发 RequestShutdown（软依赖语义被破坏）")
	case <-time.After(200 * time.Millisecond):
	}

	if st := comp.RegistrationState(); st == nil || st.Phase != PhaseFailed {
		t.Fatalf("注册失败后状态应为 PhaseFailed, got %+v", st)
	}

	mgr.Shutdown(context.Background())
}
