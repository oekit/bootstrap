package bootstrap

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/oekit/core/lifecycle"
	pkgerrors "github.com/oekit/pkg/errors"
)

// TestRunCloserWithBudget_AlignsManagerSemantics 钉住 #178：runCloserWithBudget
// 与 core/lifecycle.Manager.runCloser（本执行器的规范原型）的隔离粒度对齐——
//
//  1. panic 隔离且身份保留：panic(err) 经 pkg/errors.Recovered 收编为
//     *pkgerrors.PanicError（errors.Is 可穿透，#182；对齐 Manager 的
//     per-closer recover，Manager 侧栈与身份的同类承诺见 pkg#381）；
//  2. timeout > 0：预算自持（从 Background 派生）——已取消的 parent 不得
//     截断预算化 closer（#135）。这是与 Manager.runCloser 的**刻意分歧**：
//     Manager 带 parent AfterFunc 取消传播，App 的独立预算语义要求不传播，
//     本分支钉住该分歧不被未来「对齐」回退；
//  3. timeout <= 0：透传 parent ctx（受 close job 预算约束）。
func TestRunCloserWithBudget_AlignsManagerSemantics(t *testing.T) {
	sentinel := errors.New("teardown panic sentinel")

	// 1) panic 隔离 + 身份保留
	err := runCloserWithBudget(context.Background(), namedCloser{
		name: "boom",
		closer: func(context.Context) error {
			panic(sentinel)
		},
	})
	var perr *pkgerrors.PanicError
	if !errors.As(err, &perr) {
		t.Fatalf("panic 应转为 *pkgerrors.PanicError（#178/#182 对齐 Manager per-closer recover）, got %T: %v", err, err)
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("panic(err) 的错误身份应保留（#182）, got %v", err)
	}
	if len(perr.Stack) == 0 {
		t.Fatal("PanicError 应携带 panic 现场栈")
	}

	// 2) timeout>0：预算自持——parent 已取消，closer 的 ctx 仍活着
	parent, cancelParent := context.WithCancel(context.Background())
	cancelParent()
	released := make(chan struct{})
	go func() {
		defer close(released)
		if err := runCloserWithBudget(parent, namedCloser{
			name:    "independent",
			timeout: 80 * time.Millisecond,
			closer: func(ctx context.Context) error {
				select {
				case <-ctx.Done():
					return ctx.Err() // parent 取消若被传播会走这里
				case <-time.After(30 * time.Millisecond):
					return nil // 30ms < 80ms 预算：预算自持时正常走这里
				}
			},
		}); err != nil {
			t.Errorf("预算化 closer 被已取消的 parent 截断（#135 分歧面被回退）, got %v", err)
		}
	}()
	select {
	case <-released:
	case <-time.After(3 * time.Second):
		t.Fatal("预算化 closer 挂死")
	}

	// 3) timeout<=0：透传 parent（受 close job 预算约束的语义）
	pctx, pcancel := context.WithCancel(context.Background())
	pcancel()
	if err := runCloserWithBudget(pctx, namedCloser{
		name: "passthrough",
		closer: func(ctx context.Context) error {
			return ctx.Err()
		},
	}); !errors.Is(err, context.Canceled) {
		t.Fatalf("timeout<=0 应透传 parent ctx, got %v", err)
	}
}

// TestDrainClosers_FIFOAndLIFOOrders 钉住 #177 收敛改造的行为保持：
// runPreCloseHooks 的引擎按 FIFO（注册序）执行钩子、closeAll 按 LIFO
// （注册逆序）执行资源 closer，聚合口径（"%s: %w" + errors.Join）不变。
func TestDrainClosers_FIFOAndLIFOOrders(t *testing.T) {
	mgr := lifecycle.New(2 * time.Second)
	app := NewApp(mgr)

	var order []string
	app.AddPreCloseHook("hook-1", func(context.Context) error {
		order = append(order, "hook-1")
		return nil
	})
	app.AddPreCloseHook("hook-2", func(context.Context) error {
		order = append(order, "hook-2")
		return nil
	})
	app.AddResource("res-1", func(context.Context) error {
		order = append(order, "res-1")
		return nil
	})
	app.AddResource("res-2", func(context.Context) error {
		order = append(order, "res-2")
		return nil
	})

	// 与生产 close 阶段同序：先钩子后资源。
	if err := app.runPreCloseHooks(context.Background()); err != nil {
		t.Fatalf("runPreCloseHooks: %v", err)
	}
	if err := app.closeAll(context.Background()); err != nil {
		t.Fatalf("closeAll: %v", err)
	}
	want := []string{"hook-1", "hook-2", "res-2", "res-1"}
	if len(order) != len(want) {
		t.Fatalf("执行序 = %v, want %v", order, want)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("执行序 = %v, want %v（#177 改造不得改取向）", order, want)
		}
	}
}

// TestDrainClosers_AggregatesNamedErrors 钉住 #177 收敛改造的聚合口径：
// 失败 closer 以 "name: %w" 进 errors.Join，errors.Is 可辨（#80 同口径）。
func TestDrainClosers_AggregatesNamedErrors(t *testing.T) {
	mgr := lifecycle.New(2 * time.Second)
	app := NewApp(mgr)

	boom := errors.New("close boom")
	app.AddResource("good", func(context.Context) error { return nil })
	app.AddResource("bad", func(context.Context) error { return boom })

	if err := app.closeAll(context.Background()); err == nil {
		t.Fatal("closeAll 必须聚合关闭错误")
	} else if !errors.Is(err, boom) {
		t.Fatalf("聚合错误应 errors.Is 可辨原始错误, got %v", err)
	}
}
