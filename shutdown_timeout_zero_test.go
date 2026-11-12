package bootstrap

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/oekit/core/lifecycle"
)

// deadlineProbe 在 Init 里注册一个记录 closer ctx 截止时间的探针——
// closer 的预算来自 lifecycle.Manager 的默认预算（job timeout=0 回退），
// 即 Bootstrapper 的 shutdownTimeout（#206 观测面）。
type deadlineProbe struct {
	done chan struct{}
	has  bool
	left time.Duration
}

func (p *deadlineProbe) Name() string { return "deadline-probe" }

func (p *deadlineProbe) Init(_ context.Context, _ *App, mgr *lifecycle.Manager) error {
	mgr.Add("deadline-probe", func(ctx context.Context) error {
		dl, ok := ctx.Deadline()
		p.has = ok
		if ok {
			p.left = time.Until(dl)
		}
		close(p.done)
		return nil
	}, 0)
	return nil
}

// shutdownTimeoutDeadline 走 rollback 路径（beforeServe 返回 error ⇒ Run 立即
// 回滚 ⇒ mgr.Shutdown 执行探针 closer），返回探针观测到的关停预算。
func shutdownTimeoutDeadline(t *testing.T, opts ...BootstrapperOption) *deadlineProbe {
	t.Helper()
	p := &deadlineProbe{done: make(chan struct{})}
	base := []BootstrapperOption{
		WithServiceName("shutdown-timeout-probe"),
		WithComponent(p),
		WithBeforeServe(func() error { return errors.New("probe: abort before serve") }),
	}
	b, err := New(append(base, opts...)...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	_ = b.Run(func(app *App) error { return nil })
	select {
	case <-p.done: // closer 已执行，字段就绪
	case <-time.After(3 * time.Second):
		t.Fatal("rollback 未执行探针 closer——回滚路径缺失（#206 观测面失效）")
	}
	return p
}

// TestWithShutdownTimeout_ZeroNormalizesToDefault 钉住 #206：
// WithShutdownTimeout(0) 此前静默变成「关停永不超时」——closer ctx 无任何
// deadline，closer 挂死时 Run 永不返回（只能靠 systemd/K8s kill -9 兜底，
// 且全程无日志点明），而 godoc 只写「默认 10 秒」，0 的语义只存在于 #134
// 测试注释里。修复后 0 = 未配置回填默认 10s；负值保留为「关闭超时」哨兵。
func TestWithShutdownTimeout_ZeroNormalizesToDefault(t *testing.T) {
	t.Run("zero 回填默认 10s", func(t *testing.T) {
		p := shutdownTimeoutDeadline(t, WithShutdownTimeout(0))
		if !p.has {
			t.Fatal("WithShutdownTimeout(0) 必须回填默认 10s 预算，而不是关闭超时（#206）")
		}
		if p.left <= 9*time.Second || p.left > 10*time.Second {
			t.Fatalf("closer 预算应≈10s（默认值），got %v", p.left)
		}
	})

	t.Run("不传选项即默认 10s", func(t *testing.T) {
		p := shutdownTimeoutDeadline(t)
		if !p.has {
			t.Fatal("缺省路径必须有 10s 预算")
		}
		if p.left <= 9*time.Second || p.left > 10*time.Second {
			t.Fatalf("缺省预算应≈10s, got %v", p.left)
		}
	})

	t.Run("显式 2s 保持", func(t *testing.T) {
		p := shutdownTimeoutDeadline(t, WithShutdownTimeout(2*time.Second))
		if !p.has {
			t.Fatal("显式 2s 必须有预算")
		}
		if p.left <= 1500*time.Millisecond || p.left > 2*time.Second {
			t.Fatalf("closer 预算应≈2s, got %v", p.left)
		}
	})

	t.Run("负值 = 关闭超时哨兵", func(t *testing.T) {
		p := shutdownTimeoutDeadline(t, WithShutdownTimeout(-time.Second))
		if p.has {
			t.Fatalf("负值应关闭关停超时（closer 不设兜底预算），got deadline %v", p.left)
		}
	})
}
