package nacos

import (
	"errors"
	"testing"
	"time"

	"github.com/oekit/bootstrap"
)

// TestNNSComponent_ReadyFatal_RunReturnsFatalError 钉住 #133 场景 A（裸
// h.Spin() 形态，穿过 Bootstrapper.Run 验证返回值）：WithReadyFatal 看门狗
// 超时后，userFn 不监听任何 context（h.Spin() 只等系统信号）时 Run 也必须
// 以 ErrReadyFatal 返回——此前出口是 RequestShutdown（纯 cancel），Run 的
// select 永远等不到事件，进程带着 ERROR 日志继续对外服务，fail-fast 是
// no-op。
func TestNNSComponent_ReadyFatal_RunReturnsFatalError(t *testing.T) {
	fake := withFakeNNS(t)
	comp := newTestComponent()
	comp.ReadyTimeout = 50 * time.Millisecond
	comp.ReadyFatal = true

	b, err := bootstrap.New(
		bootstrap.WithServiceName("fatal-run-svc"),
		bootstrap.WithComponent(comp),
	)
	if err != nil {
		t.Fatal(err)
	}

	hang := make(chan struct{}) // 裸 h.Spin()：不监听 app ctx，只等系统信号
	defer close(hang)           // Run 返回后放行 userFn goroutine

	runDone := make(chan error, 1)
	go func() {
		runDone <- b.Run(func(_ *bootstrap.App) error {
			<-hang
			return nil
		})
	}()

	select {
	case err := <-runDone:
		if !errors.Is(err, ErrReadyFatal) {
			t.Fatalf("Run 应返回 ErrReadyFatal, got %v（#133 场景 A）", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("fatal 后 Run 仍阻塞——裸 Spin 形态 fail-fast 失效（#133 场景 A）")
	}
	if n := fake.registerCount(); n != 0 {
		t.Fatalf("fatal 路径不应注册, got %d", n)
	}
}

// TestNNSComponent_RequiredRegisterFail_RunReturnsError 钉住 #133 场景 B
// （穿过 Bootstrapper.Run 验证返回值）：userFn 监听 ctx 且返回 nil 时，
// Required 注册失败后 Run 必须返回携带注册错误的结果而非 nil——此前哨兵
// 只存在于 RegistrationState() 查询里，启动失败在部署门禁/systemd
// Restart=on-failure/对账链路上表现为「成功」（退出码 0）。
func TestNNSComponent_RequiredRegisterFail_RunReturnsError(t *testing.T) {
	fake := withFailingRegisterNNS(t, errors.New("nacos unreachable"))
	comp := newTestComponent()
	comp.Required = true

	b, err := bootstrap.New(
		bootstrap.WithServiceName("required-run-svc"),
		bootstrap.WithComponent(comp),
	)
	if err != nil {
		t.Fatal(err)
	}

	runDone := make(chan error, 1)
	go func() {
		runDone <- b.Run(func(a *bootstrap.App) error {
			a.Ready()            // server 就绪 → 触发延迟注册（随后注册失败）
			<-a.Context().Done() // 监听 ctx 的形态
			return nil           // userFn 自身无错误
		})
	}()

	select {
	case err := <-runDone:
		if err == nil || !errors.Is(err, fake.regErr) {
			t.Fatalf("Required 注册失败后 Run 应返回携带注册错误的结果, got %v（#133 场景 B）", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run 挂死")
	}
	if st := comp.RegistrationState(); st == nil || st.Phase != PhaseFailed {
		t.Fatalf("注册失败后状态应为 PhaseFailed, got %+v", st)
	}
}
