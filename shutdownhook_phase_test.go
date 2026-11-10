package bootstrap

// WithShutdownHook 阶段位与启动回滚错误面的回归测试（#168 + #169）：
//   - #168：缺省关闭钩子是前置钩子（drain 之后、资源关闭之前执行——flush
//     型钩子的依赖资源仍在位）；Late=true 才落到旧位置（四阶段之后）；
//     钩子错误进关停错误上抛链路；
//   - #169：启动失败回滚（组件 Init 失败 / beforeServe 失败）的回滚期关停
//     错误并入 Run 返回值，与 #80 正常关停面同口径——不再只进日志。
import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/oekit/core/lifecycle"
)

// recorder 线性记录事件序（单 goroutine 关停序列，普通切片即可）。
type recorder struct{ events []string }

func (r *recorder) record(e string) { r.events = append(r.events, e) }

// stubComponent 测试组件：Init 可失败、可注册会失败的 closer。
type stubComponent struct {
	name     string
	initErr  error
	closeErr error
}

func (s *stubComponent) Name() string { return s.name }

func (s *stubComponent) Init(_ context.Context, app *App, _ *lifecycle.Manager) error {
	if s.initErr != nil {
		return s.initErr
	}
	if s.closeErr != nil {
		app.AddResource(s.name, func(context.Context) error { return s.closeErr })
	}
	return nil
}

// TestWithShutdownHook_RunsBeforeResourceClose 钉住 #168 缺省语义：
// WithShutdownHook 的钩子先于 AddResource 资源关闭执行（drain 之后）——
// flush 型钩子的依赖资源此刻仍在位。旧形态：钩子被静默排到
// app-lifecycle:wait 之后，资源已全部关闭才跑。
func TestWithShutdownHook_RunsBeforeResourceClose(t *testing.T) {
	rec := &recorder{}
	b, err := New(
		WithServiceName("hook-order-svc"),
		WithShutdownHook("hook-flush", func(context.Context) error {
			rec.record("hook")
			return nil
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Run(func(app *App) error {
		app.AddResource("db", func(context.Context) error {
			rec.record("db-close")
			return nil
		})
		return nil
	}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	want := []string{"hook", "db-close"}
	if len(rec.events) != len(want) {
		t.Fatalf("事件序 = %v, want %v", rec.events, want)
	}
	for i := range want {
		if rec.events[i] != want[i] {
			t.Fatalf("事件序 = %v, want %v（#168：钩子必须先于资源关闭）", rec.events, want)
		}
	}
}

// TestShutdownHookComponent_LateRunsAfterEverything 钉住 Late=true 的
// 显式晚窗：钩子排在全部资源关闭与后台排空之后（旧位置的显式回归）。
func TestShutdownHookComponent_LateRunsAfterEverything(t *testing.T) {
	rec := &recorder{}
	b, err := New(
		WithServiceName("hook-late-svc"),
		WithComponent(&ShutdownHookComponent{
			ComponentName: "hook-late",
			Hook: func(context.Context) error {
				rec.record("hook-late")
				return nil
			},
			Late: true,
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Run(func(app *App) error {
		app.AddResource("db", func(context.Context) error {
			rec.record("db-close")
			return nil
		})
		return nil
	}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	want := []string{"db-close", "hook-late"}
	if len(rec.events) != len(want) {
		t.Fatalf("事件序 = %v, want %v", rec.events, want)
	}
	for i := range want {
		if rec.events[i] != want[i] {
			t.Fatalf("事件序 = %v, want %v（Late 钩子必须在资源关闭之后）", rec.events, want)
		}
	}
}

// TestWithShutdownHook_ErrorSurfacesInReturn 钉住 #168 错误面：钩子失败
// 进关停错误上抛链路（与 #80 资源关闭失败同口径），Run 返回值可见。
func TestWithShutdownHook_ErrorSurfacesInReturn(t *testing.T) {
	flushFailed := errors.New("flush failed")
	b, err := New(
		WithServiceName("hook-err-svc"),
		WithShutdownHook("hook-flush", func(context.Context) error { return flushFailed }),
	)
	if err != nil {
		t.Fatal(err)
	}
	runErr := b.Run(func(app *App) error { return nil })
	if runErr == nil {
		t.Fatal("钩子失败必须经 Run 返回值上抛（#168）")
	}
	if !errors.Is(runErr, flushFailed) {
		t.Fatalf("返回值应 errors.Is 可辨钩子错误, got %v", runErr)
	}
	if !strings.Contains(runErr.Error(), "hook-flush") {
		t.Fatalf("返回值应带钩子名, got %v", runErr)
	}
}

// TestRun_RollbackErrorsJoinedInReturn 钉住 #169（组件 Init 失败路径）：
// 回滚期 closer 失败并入返回值——「; rollback: 」附加在主因之后，
// errors.Is 双方可辨；不再只进日志。
func TestRun_RollbackErrorsJoinedInReturn(t *testing.T) {
	rollbackCloseFailed := errors.New("rollback close also failed")
	initBoom := errors.New("init boom")
	b, err := New(
		WithServiceName("rollback-svc"),
		WithComponent(&stubComponent{name: "leaky", closeErr: rollbackCloseFailed}),
		WithComponent(&stubComponent{name: "bad", initErr: initBoom}),
	)
	if err != nil {
		t.Fatal(err)
	}
	runErr := b.Run(func(app *App) error { return nil })
	if runErr == nil {
		t.Fatal("Init 失败必须返回错误")
	}
	if !errors.Is(runErr, initBoom) {
		t.Fatalf("返回值应含主因（init 错误）, got %v", runErr)
	}
	if !errors.Is(runErr, rollbackCloseFailed) {
		t.Fatalf("回滚期关停错误必须并入返回值（#169），got %v", runErr)
	}
	if !strings.Contains(runErr.Error(), "rollback") {
		t.Fatalf("返回值应带 rollback 标注, got %v", runErr)
	}
}

// TestRun_BeforeServeRollbackErrorsJoinedInReturn 钉住 #169（beforeServe
// 失败路径）：与 Init 失败路径同口径，回滚期关停错误并入返回值。
func TestRun_BeforeServeRollbackErrorsJoinedInReturn(t *testing.T) {
	rollbackCloseFailed := errors.New("rollback close also failed")
	beforeServeBoom := errors.New("beforeServe boom")
	b, err := New(
		WithServiceName("rollback-b4s-svc"),
		WithComponent(&stubComponent{name: "leaky", closeErr: rollbackCloseFailed}),
		WithBeforeServe(func() error { return beforeServeBoom }),
	)
	if err != nil {
		t.Fatal(err)
	}
	runErr := b.Run(func(app *App) error { return nil })
	if runErr == nil {
		t.Fatal("beforeServe 失败必须返回错误")
	}
	if !errors.Is(runErr, beforeServeBoom) {
		t.Fatalf("返回值应含主因（beforeServe 错误）, got %v", runErr)
	}
	if !errors.Is(runErr, rollbackCloseFailed) {
		t.Fatalf("回滚期关停错误必须并入返回值（#169），got %v", runErr)
	}
}
