package bootstrap

import (
	"errors"
	"testing"
	"time"

	"github.com/oekit/core/lifecycle"
)

var errBoom = errors.New("fatal: dependency unavailable")

// TestRun_Fail_UnblocksRunAndReturnsError 钉住 #133 场景 A（裸 Spin 形态）：
// userFn 不监听任何 context（h.Spin() 只等系统信号）时，App.Fail 必须解除
// Run 的阻塞并把错误经返回值上抛（退出码非 0）——此前 fail-fast 的唯一
// 出口 RequestShutdown 只取消 ctx，Run 的 select 永远等不到事件，进程带着
// ERROR 日志继续对外服务，「早死」没有发生。
func TestRun_Fail_UnblocksRunAndReturnsError(t *testing.T) {
	b, err := New(WithServiceName("fail-fast-svc"))
	if err != nil {
		t.Fatal(err)
	}

	hang := make(chan struct{}) // 裸 h.Spin()：不监听 app ctx，只等外部信号
	defer close(hang)           // Run 返回后放行 userFn goroutine

	runDone := make(chan error, 1)
	go func() {
		runDone <- b.Run(func(a *App) error {
			a.Fail(errBoom) // 等价「看门狗到点」：启动期发现服务不可用
			<-hang
			return nil
		})
	}()

	select {
	case err := <-runDone:
		if !errors.Is(err, errBoom) {
			t.Fatalf("Run 应返回 Fail 的错误, got %v（#133 场景 A）", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Fail 后 Run 仍阻塞——裸 Spin 形态 fail-fast 失效（#133 场景 A）")
	}
	// #141 起该形态的返回时点 = 关停完成 + lateUserFnErrGrace（userFn 永不
	// 返回，补收窗口到点即返回）：守护上限从 3s 放宽到 10s，防慢 CI 误报；
	// 断言本身（Run 必须返回且携带 fatal 错误）不变。
}

// TestRun_Fail_WhileUserFnListensContext 钉住 #133 场景 B：userFn 监听 ctx
// 且返回 nil 时，Run 必须返回 Fail 的错误而非 nil——此前 errReadyFatal 哨兵
// 只存在于 RegistrationState() 查询里，启动失败在部署门禁/systemd
// Restart=on-failure 链路上表现为「成功」（退出码 0）。
func TestRun_Fail_WhileUserFnListensContext(t *testing.T) {
	b, err := New(WithServiceName("fail-listen-svc"))
	if err != nil {
		t.Fatal(err)
	}

	runDone := make(chan error, 1)
	go func() {
		runDone <- b.Run(func(a *App) error {
			go func() {
				time.Sleep(50 * time.Millisecond)
				a.Fail(errBoom)
			}()
			<-a.Context().Done() // 监听 ctx 的形态
			return nil           // userFn 自身无错误
		})
	}()

	select {
	case err := <-runDone:
		if !errors.Is(err, errBoom) {
			t.Fatalf("userFn 返回 nil 时 Run 应携带 fatal 错误, got %v（#133 场景 B）", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run 挂死")
	}
}

// TestRun_Fail_NilErrorIsRequestShutdown 语义边界：Fail(nil) 只触发关停、
// 不上抛错误（等同 RequestShutdown）。
func TestRun_Fail_NilErrorIsRequestShutdown(t *testing.T) {
	b, err := New(WithServiceName("fail-nil-svc"))
	if err != nil {
		t.Fatal(err)
	}
	err = b.Run(func(a *App) error {
		go func() {
			time.Sleep(50 * time.Millisecond)
			a.Fail(nil)
		}()
		<-a.Context().Done()
		return nil
	})
	if err != nil {
		t.Fatalf("Fail(nil) 不应上抛错误, got %v", err)
	}
}

// TestRun_Fail_FirstErrorWins 幂等语义：首个非 nil 错误生效，后续 Fail 不
// 覆盖。
func TestRun_Fail_FirstErrorWins(t *testing.T) {
	b, err := New(WithServiceName("fail-first-svc"))
	if err != nil {
		t.Fatal(err)
	}
	errAnother := errors.New("second fatal")
	err = b.Run(func(a *App) error {
		a.Fail(errBoom)
		a.Fail(errAnother) // 不得覆盖
		<-a.Context().Done()
		return nil
	})
	if !errors.Is(err, errBoom) || errors.Is(err, errAnother) {
		t.Fatalf("首个 Fail 错误应生效, got %v", err)
	}
}

// TestRun_Fail_MergedWithUserFnError userFn 自身错误与 fatal 错误并存时
// 两者都出现在返回值（runErr 优先、fatal 附加）。
func TestRun_Fail_MergedWithUserFnError(t *testing.T) {
	b, err := New(WithServiceName("fail-merge-svc"))
	if err != nil {
		t.Fatal(err)
	}
	errUser := errors.New("userFn boom")
	err = b.Run(func(a *App) error {
		a.Fail(errBoom)
		return errUser
	})
	if err == nil || !errors.Is(err, errUser) || !errors.Is(err, errBoom) {
		t.Fatalf("userFn 错误与 fatal 错误应同时可辨, got %v", err)
	}
}

// TestApp_Fail_AfterUserFnReturned userFn 已返回（Run 已进入 select 之后的
// 关停路径）时 Fail 仍生效：错误并入返回值。
func TestApp_Fail_AfterUserFnReturned(t *testing.T) {
	app := NewApp(lifecycle.New(time.Second))
	app.Fail(errBoom)
	if got := app.fatalError(); !errors.Is(got, errBoom) {
		t.Fatalf("fatalError 应可读回 Fail 的错误, got %v", got)
	}
	select {
	case <-app.fatalCh:
	default:
		t.Fatal("fatalCh 应已投递错误（Run 的唤醒出口）")
	}
	// 幂等：重复 Fail 不覆盖、不阻塞
	app.Fail(errors.New("second"))
	if got := app.fatalError(); !errors.Is(got, errBoom) {
		t.Fatalf("首个错误应生效, got %v", got)
	}
}

// TestRun_LateUserFnErrorCollectedWithinGrace 回归（#141）：fatal 出口唤醒
// Run 后，userFn **晚于关停完成**才返回的错误必须并入返回值——此前补收是
// 非阻塞读，慢 flush/慢回滚形态（issue 实测 400ms 后返回）的错误被静默丢弃，
// 退出码只反映 fatal，关停期的业务失败对部署门禁不可见（正是 #80/#133 要防
// 的事）。修复后关停完成后再等 lateUserFnErrGrace 小窗口补收，fatal 错误
// 以「; fatal: 」附加、两条都可辨。
func TestRun_LateUserFnErrorCollectedWithinGrace(t *testing.T) {
	b, err := New(WithServiceName("late-err-svc"))
	if err != nil {
		t.Fatal(err)
	}

	errLate := errors.New("shutdown flush failed")
	runDone := make(chan error, 1)
	go func() {
		runDone <- b.Run(func(a *App) error {
			a.Fail(errBoom)                    // fatal 出口唤醒 Run
			time.Sleep(400 * time.Millisecond) // 慢 flush 形态：晚于关停完成才返回
			return errLate
		})
	}()

	select {
	case err := <-runDone:
		if !errors.Is(err, errLate) {
			t.Fatalf("晚于关停完成的 userFn 错误应并入返回值（#141）, got %v", err)
		}
		if !errors.Is(err, errBoom) {
			t.Fatalf("fatal 错误仍应可辨（runErr 优先、fatal 附加）, got %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run 挂死")
	}
}
