package healthz

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func reset() {
	ResetForTest()
}

// TestHandle_NoChecks 验证无 check 时直接返 200 + "ok"。
func TestHandle_NoChecks(t *testing.T) {
	reset()

	status, body := Handle(context.Background())
	if status != 200 {
		t.Errorf("status = %d, want 200", status)
	}
	if !strings.Contains(body, `"status":"ok"`) {
		t.Errorf("body = %s, want status:ok", body)
	}
}

// TestHandle_AllPass 验证所有 check 通过 → 200。
func TestHandle_AllPass(t *testing.T) {
	reset()

	RegisterCheck("mysql", func(_ context.Context) error { return nil })
	RegisterCheck("redis", func(_ context.Context) error { return nil })

	status, body := Handle(context.Background())
	if status != 200 {
		t.Errorf("status = %d, want 200", status)
	}
	if !strings.Contains(body, `"status":"ok"`) {
		t.Errorf("body = %s, want status:ok", body)
	}
}

// TestHandle_OneFails 验证任一 check 失败 → 503 + 失败详情。
func TestHandle_OneFails(t *testing.T) {
	reset()

	RegisterCheck("mysql", func(_ context.Context) error { return nil })
	RegisterCheck("redis", func(_ context.Context) error { return errors.New("connection refused") })

	status, body := Handle(context.Background())
	if status != 503 {
		t.Errorf("status = %d, want 503", status)
	}
	if !strings.Contains(body, `"status":"unhealthy"`) {
		t.Errorf("body = %s, want status:unhealthy", body)
	}
	if !strings.Contains(body, `"redis":"connection refused"`) {
		t.Errorf("body = %s, want redis:connection refused", body)
	}
	if strings.Contains(body, `"mysql"`) {
		t.Errorf("body should not include successful checks, got %s", body)
	}
}

// TestHandle_AllFail 验证所有 check 失败 → 503 + 全部详情。
func TestHandle_AllFail(t *testing.T) {
	reset()

	RegisterCheck("a", func(_ context.Context) error { return errors.New("err-a") })
	RegisterCheck("b", func(_ context.Context) error { return errors.New("err-b") })

	status, body := Handle(context.Background())
	if status != 503 {
		t.Errorf("status = %d, want 503", status)
	}
	if !strings.Contains(body, "err-a") || !strings.Contains(body, "err-b") {
		t.Errorf("body should contain both err-a and err-b, got %s", body)
	}
}

// TestHandle_CheckPanic 验证 check panic 被 recover,当作失败处理。
func TestHandle_CheckPanic(t *testing.T) {
	reset()

	RegisterCheck("explode", func(_ context.Context) error {
		panic("check went boom")
	})

	status, body := Handle(context.Background())
	if status != 503 {
		t.Errorf("status = %d, want 503", status)
	}
	if !strings.Contains(body, "explode") {
		t.Errorf("body = %s, want contain explode", body)
	}
	if !strings.Contains(body, "panic") {
		t.Errorf("body = %s, want contain panic", body)
	}
}

// TestHandle_MarkClosed 验证关闭后 Handle 返 503 + "shutting down"。
func TestHandle_MarkClosed(t *testing.T) {
	reset()

	RegisterCheck("mysql", func(_ context.Context) error { return nil })
	MarkClosed()

	status, body := Handle(context.Background())
	if status != 503 {
		t.Errorf("status = %d, want 503", status)
	}
	if !strings.Contains(body, `"closed":true`) {
		t.Errorf("body = %s, want closed:true", body)
	}
}

// TestHandle_RegisterOverwrite 验证同名 check 覆盖(不重复)。
func TestHandle_RegisterOverwrite(t *testing.T) {
	reset()

	RegisterCheck("a", func(_ context.Context) error { return errors.New("old") })
	RegisterCheck("a", func(_ context.Context) error { return nil }) // 覆盖

	status, _ := Handle(context.Background())
	if status != 200 {
		t.Errorf("status = %d, want 200 (overwrite ok)", status)
	}
}

// TestHandle_Deregister 验证 Deregister 后不再调起。
func TestHandle_Deregister(t *testing.T) {
	reset()

	RegisterCheck("temp", func(_ context.Context) error { return errors.New("fail") })
	status, _ := Handle(context.Background())
	if status != 503 {
		t.Fatalf("setup: status = %d, want 503", status)
	}
	DeregisterCheck("temp")
	status, _ = Handle(context.Background())
	if status != 200 {
		t.Errorf("after Deregister: status = %d, want 200", status)
	}
}

// TestHandle_SlowCheckTimeout 钉住 2026-09-05 修复:挂死的 check 不再无限
// 阻塞 Handle — 总超时(默认 5s,测试里调短)后统一记 "check timeout"
// 返回 503,ctx 也注入了 deadline。
func TestHandle_SlowCheckTimeout(t *testing.T) {
	ResetForTest()
	t.Cleanup(func() {
		SetCheckTimeout(defaultCheckTimeout)
		ResetForTest()
	})
	SetCheckTimeout(50 * time.Millisecond)

	RegisterCheck("slow", func(ctx context.Context) error {
		<-ctx.Done() // 模拟挂死但 ctx-aware 的依赖
		return ctx.Err()
	})
	RegisterCheck("fast", func(_ context.Context) error { return nil })

	start := time.Now()
	code, body := Handle(context.Background())
	elapsed := time.Since(start)

	if code != 503 {
		t.Errorf("status = %d, want 503", code)
	}
	if !strings.Contains(body, "check timeout") {
		t.Errorf("body = %s, want slow check recorded as timeout", body)
	}
	if elapsed > 2*time.Second {
		t.Errorf("Handle elapsed = %v, want bounded by check timeout", elapsed)
	}
}
