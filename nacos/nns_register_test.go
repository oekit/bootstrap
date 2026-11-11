package nacos

import (
	"context"
	"testing"
	"time"

	"github.com/oekit/bootstrap"
	"github.com/oekit/core/lifecycle"
	nacospkg "github.com/oekit/registry/nacos"
)

// nns_register_test.go 钉住 #180 拆分后的看门狗行为面：registerWhenReady
// 抽为私有方法后可脱离完整 Run 流程单测（拆分前是 Init 内 120 行闭包，
// 只能端到端覆盖）。以下用例与 nns_ready_test.go / nns_ready_fatal_test.go
// 的端到端面互补，聚焦方法级契约。

// newRegisterFixture 构造干净的组件 + App（包级 readyFatal 缺省复位由
// 调用方负责——与 nns_ready_fatal_test.go 的隔离手法一致）。
func newRegisterFixture(t *testing.T, readyTimeout time.Duration) (*NNSComponent, *bootstrap.App) {
	t.Helper()
	c := &NNSComponent{
		NacosCfg:     nacospkg.Bootstrap{Addr: "127.0.0.1:8848", ServiceName: "svc"},
		Port:         8080,
		ReadyTimeout: readyTimeout,
	}
	c.setRegisterState(&RegisterState{Phase: PhasePending, Service: "svc", Port: 8080, Since: time.Now()})
	app := bootstrap.NewApp(lifecycle.New(5 * time.Second))
	return c, app
}

// TestRegisterWhenReady_ReadyProceeds 就绪信号到来 → true，状态保持
// PhasePending（迁移到 PhaseRegistered 是 registerAndWire 的职责）。
func TestRegisterWhenReady_ReadyProceeds(t *testing.T) {
	c, app := newRegisterFixture(t, 0)
	go app.Ready()

	if !c.registerWhenReady(context.Background(), app) {
		t.Fatal("就绪后 registerWhenReady 应返回 true")
	}
	if st := c.RegistrationState(); st.Phase != PhasePending {
		t.Fatalf("registerWhenReady 只负责等就绪，状态应保持 pending, got %s", st.Phase)
	}
}

// TestRegisterWhenReady_ShutdownWithoutReady ctx 取消（关停）→ false，
// 状态标 PhaseShutdown（就绪门漏接的可观测面）。
func TestRegisterWhenReady_ShutdownWithoutReady(t *testing.T) {
	c, app := newRegisterFixture(t, 0)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if c.registerWhenReady(ctx, app) {
		t.Fatal("关停中 registerWhenReady 应返回 false")
	}
	if st := c.RegistrationState(); st.Phase != PhaseShutdown {
		t.Fatalf("关停且未就绪应标 shutdown, got %s", st.Phase)
	}
}

// TestRegisterWhenReady_WatchdogTimesOutThenWaits 钉住看门狗的非 fatal 分支
// （#33）：超时先告警（markReadyWaitExceeded），随后继续等就绪——最终就绪
// 时仍返回 true（慢启动不误杀）。
func TestRegisterWhenReady_WatchdogTimesOutThenWaits(t *testing.T) {
	c, app := newRegisterFixture(t, 20*time.Millisecond)
	go func() {
		time.Sleep(80 * time.Millisecond) // 晚于 20ms 看门狗超时
		app.Ready()
	}()

	if !c.registerWhenReady(context.Background(), app) {
		t.Fatal("超时告警后就绪仍应继续注册")
	}
	st := c.RegistrationState()
	if !st.ReadyWaitExceeded {
		t.Fatal("看门狗超时应标记 ReadyWaitExceeded")
	}
	if st.Phase != PhasePending {
		t.Fatalf("告警后继续等，状态应保持 pending, got %s", st.Phase)
	}
}

// TestRegisterWhenReady_WatchdogFatalAborts 钉住看门狗的 fatal 分支（#74）：
// 超时 + ReadyFatal → false，状态标 PhaseFailed + ErrReadyFatal
// （app.Fail 已触发，注册永不发生）。
func TestRegisterWhenReady_WatchdogFatalAborts(t *testing.T) {
	prevFatal := readyFatal.Load()
	defer readyFatal.Store(prevFatal) // 测试间隔离（包级状态）
	readyFatal.Store(true)

	c, app := newRegisterFixture(t, 20*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		<-app.Context().Done() // Fail 触发的关停解除阻塞，避免 ctx 泄漏等待
	}()

	if c.registerWhenReady(ctx, app) {
		t.Fatal("ReadyFatal 超时后不应继续注册")
	}
	st := c.RegistrationState()
	if st.Phase != PhaseFailed || st.Err != ErrReadyFatal {
		t.Fatalf("应标 failed + ErrReadyFatal, got phase=%s err=%v", st.Phase, st.Err)
	}
	if !st.ReadyWaitExceeded {
		t.Fatal("fatal 分支同样应标记 ReadyWaitExceeded")
	}
	select {
	case <-app.Context().Done():
	default:
		t.Fatal("fail-fast 应已触发关停（app.Fail）")
	}
}
