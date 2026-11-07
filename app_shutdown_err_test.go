package bootstrap

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/oekit/core/lifecycle"
)

// TestCloseAll_AggregatesErrors 钉住 #80（单元面）：资源关闭失败的错误必须被
// closeAll 聚合返回——不再只打日志后丢弃。
func TestCloseAll_AggregatesErrors(t *testing.T) {
	a := NewApp(lifecycle.New(2 * time.Second))
	boom := errors.New("flush failed")
	a.AddResource("bad", func(context.Context) error { return boom })
	a.AddResource("good", func(context.Context) error { return nil })

	err := a.closeAll(context.Background())
	if err == nil {
		t.Fatal("closeAll 必须聚合关闭错误（#80）")
	}
	if !errors.Is(err, boom) {
		t.Fatalf("聚合错误应 errors.Is 可辨原始错误, got %v", err)
	}
	if !strings.Contains(err.Error(), "bad") {
		t.Fatalf("聚合错误应带资源名, got %v", err)
	}
}

// TestRun_ShutdownErrorsSurfaceInReturn 钉住 #80（端到端面）：资源关闭失败时
// Run 的返回值非 nil——进程退出码反映关停质量，不再「带着未落盘数据退出、
// 退出码 0」。
func TestRun_ShutdownErrorsSurfaceInReturn(t *testing.T) {
	b, err := New(WithServiceName("shutdown-err-svc"))
	if err != nil {
		t.Fatal(err)
	}
	err = b.Run(func(a *App) error {
		a.AddResource("bad-closer", func(context.Context) error {
			return errors.New("db flush failed")
		})
		// userFn 立即返回：触发正常关停序列（close 阶段跑 bad-closer）
		return nil
	})
	if err == nil {
		t.Fatal("资源关闭失败时 Run 必须返回非 nil（#80）")
	}
	if !strings.Contains(err.Error(), "db flush failed") {
		t.Fatalf("返回值应携带关闭错误, got %v", err)
	}
}
