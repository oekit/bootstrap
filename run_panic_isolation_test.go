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

// rollbackProbeComponent 在 Init 里向 lifecycle.Manager 注册关闭钩子——
// 钩子跑过 = rollback 执行过（#201 探针口径：panic 冲出 Run 时钩子被整体跳过）。
type rollbackProbeComponent struct {
	closed chan struct{}
}

func (c *rollbackProbeComponent) Name() string { return "rollback-probe" }

func (c *rollbackProbeComponent) Init(_ context.Context, _ *App, mgr *lifecycle.Manager) error {
	mgr.Add("rollback-probe", func(context.Context) error {
		select {
		case c.closed <- struct{}{}:
		default:
		}
		return nil
	}, 0)
	return nil
}

func rollbackRan(c *rollbackProbeComponent) bool {
	select {
	case <-c.closed:
		return true
	case <-time.After(3 * time.Second):
		return false
	}
}

// TestRun_BeforeServePanic_ConvertsToErrorAndRollsBack 钉住 #201（形态一）：
// beforeServe 循环此前是裸调用——panic 直接冲出 Run，rollback 不执行，已初始化
// 组件的关闭链（tracing exporter flush、nacos 断连、AddResource closer）被整体
// 跳过，且不经 #182 的 panic→error 单源（调用方拿不到 bootstrap: 前缀错误）。
// 修复后与 safeInit 同款隔离：panic 走既有「beforeServe[%d] failed」回滚分支，
// panic(error) 的哨兵身份经 Recovered 收编后仍可 errors.Is 穿透。
func TestRun_BeforeServePanic_ConvertsToErrorAndRollsBack(t *testing.T) {
	sentinel := errors.New("beforeserve boom")
	probe := &rollbackProbeComponent{closed: make(chan struct{}, 1)}

	b, err := New(
		WithServiceName("beforeserve-panic"),
		WithShutdownTimeout(time.Second),
		WithComponent(probe),
		WithBeforeServe(func() error { panic(sentinel) }),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	userRan := false
	runErr := b.Run(func(app *App) error {
		userRan = true
		return nil
	})

	if runErr == nil {
		t.Fatal("beforeServe panic 必须转为 Run 的返回错误，而不是冲出 Run")
	}
	if !strings.Contains(runErr.Error(), "beforeServe[0] failed") {
		t.Fatalf("应走既有 beforeServe[i] failed 回滚分支, got %v", runErr)
	}
	if !strings.Contains(runErr.Error(), "panic") {
		t.Fatalf("panic 值应收编进错误文案（#182 单源）, got %v", runErr)
	}
	if !errors.Is(runErr, sentinel) {
		t.Fatalf("panic(error) 的哨兵身份丢失（#182）, got %v", runErr)
	}
	var perr *pkgerrors.PanicError
	if !errors.As(runErr, &perr) {
		t.Fatalf("应为 *pkgerrors.PanicError（#182 单源）, got %T", runErr)
	}
	if userRan {
		t.Fatal("beforeServe panic 后 userFn 不应启动")
	}
	if !rollbackRan(probe) {
		t.Fatal("beforeServe panic 必须执行 rollback——已初始化组件的关闭钩子被跳过 = 资源泄漏（#201）")
	}
}

// TestRun_NilComponent_FailsFastWithoutPanicEscape 钉住 #201（形态二）：
// WithComponent(nil) 时 safeInit 内部已 recover 了 nil Init 的 panic，紧接的
// 错误格式化 c.Name()（裸 nil 接口调用）又 panic——已收编的错误在包装时二次
// 爆炸并冲出 Run，同样跳过 rollback。修复后组件名经 panic 隔离的 componentName
// 先取：Run 返回「component <nil> init failed」错误且 rollback 照常执行。
func TestRun_NilComponent_FailsFastWithoutPanicEscape(t *testing.T) {
	probe := &rollbackProbeComponent{closed: make(chan struct{}, 1)}

	b, err := New(
		WithServiceName("nil-component"),
		WithShutdownTimeout(time.Second),
		WithComponent(probe),
		WithComponent(nil),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// 若 Name()/Init 的裸 panic 仍冲出 Run，本用例会以 panic 形式失败。
	runErr := b.Run(func(app *App) error { return nil })

	if runErr == nil {
		t.Fatal("WithComponent(nil) 必须以错误失败（构造面非法值不得运行期裸 panic）")
	}
	if !strings.Contains(runErr.Error(), "component <nil> init failed") {
		t.Fatalf("错误应点名 nil 组件, got %v", runErr)
	}
	if !rollbackRan(probe) {
		t.Fatal("nil 组件失败同样必须 rollback（#201：错误格式化二次爆炸曾跳过回滚）")
	}
}
