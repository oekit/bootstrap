package bootstrap

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/oekit/core/lifecycle"
	pkgerrors "github.com/oekit/pkg/errors"
)

// TestRun_UserFnPanicReturnsError 回归测试：userFn panic 此前被 async.GoSafe
// 的外层 recover 静默吞掉，errCh 永远等不到值，select 在无信号环境下永久
// 阻塞（进程假活）。panic 现在转为 error 返回并触发正常关闭。
// #182：panic 值经 pkg/errors.Recovered 收编，文案为
// "bootstrap: userFn: panic: <类型>: <值>"。
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
		if err == nil || !strings.Contains(err.Error(), "panic: string: boom") {
			t.Fatalf("expected panic converted to error, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after userFn panic — process would hang forever")
	}
}

// TestRun_UserFnPanic_ErrorIdentity 回归 #182：userFn 以 panic(err) 失败时，
// 错误身份经 pkg/errors.Recovered 收编后不丢——errors.Is/As 可穿透
// PanicError 直达原始错误（旧实现 fmt.Errorf("...: %v", recover()) 把身份
// 拍平成纯文本，部署门禁按哨兵错误分流的能力当场丢失）。
func TestRun_UserFnPanic_ErrorIdentity(t *testing.T) {
	b, err := New(
		WithServiceName("panic-identity"),
		WithShutdownTimeout(500*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	sentinel := errors.New("userFn sentinel")

	done := make(chan error, 1)
	go func() {
		done <- b.Run(func(app *App) error {
			panic(sentinel)
		})
	}()

	select {
	case err := <-done:
		if !errors.Is(err, sentinel) {
			t.Fatalf("panic(error) 的错误身份丢失（#182）, got %v", err)
		}
		var perr *pkgerrors.PanicError
		if !errors.As(err, &perr) {
			t.Fatalf("应为 *pkgerrors.PanicError（#182 单源）, got %T", err)
		}
		if len(perr.Stack) == 0 {
			t.Fatal("PanicError 应携带 panic 现场栈")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after userFn panic")
	}
}

// panicInitComponent 在 Init 阶段以 panic(err) 失败的组件（#182 回归用）。
type panicInitComponent struct {
	sentinel error
}

func (c *panicInitComponent) Name() string { return "panic-init" }

func (c *panicInitComponent) Init(_ context.Context, _ *App, _ *lifecycle.Manager) error {
	panic(c.sentinel)
}

// TestRun_ComponentInitPanic_ErrorIdentity 回归 #182（safeInit 出口）：组件
// 初始化 panic(err) 的错误身份经 Recovered 收编后可被 errors.Is 穿透——
// 回滚路径照常执行（panic 不再冲出 Run 的初始化循环），且部署门禁仍能
// 按哨兵错误判定启动失败类别。
func TestRun_ComponentInitPanic_ErrorIdentity(t *testing.T) {
	sentinel := errors.New("init panic sentinel")
	b, err := New(
		WithServiceName("init-panic-identity"),
		WithShutdownTimeout(500*time.Millisecond),
		WithComponent(&panicInitComponent{sentinel: sentinel}),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	runErr := b.Run(func(app *App) error { return nil })
	if runErr == nil {
		t.Fatal("组件 init panic 必须转为 Run 的返回错误")
	}
	if !errors.Is(runErr, sentinel) {
		t.Fatalf("init panic(error) 的错误身份丢失（#182）, got %v", runErr)
	}
	if !strings.Contains(runErr.Error(), "panic-init") {
		t.Fatalf("返回值应经 rollback 携带组件名, got %v", runErr)
	}
}
