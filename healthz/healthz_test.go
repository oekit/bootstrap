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

// TestHandle_SlowCheckTimeout_StableWording 钉住「同一次超时的对外文案必须唯一」。
//
// **为什么单次调用不够**：单次 TestHandle_SlowCheckTimeout 恰好没踩中 CI 上
// 偶发的那条路径——总超时到点时，check 的 ctx 被 cancel 并返回
// context.DeadlineExceeded，而 collect 侧的定时器**在同一时刻**到期；
// 「谁先被 select 选中」决定了这同一次超时被记成
// "check timeout after 50ms" 还是 "context deadline exceeded"。
// 于是同一份代码在不同机器/负载下产出不同的 body：CI 上偶发红
// （bootstrap#48），本地却恒绿。
//
// 修复后判据唯一（见 failureText：只有 cause 是我们自己的总超时标记时
// 才归一），因此**多轮跑必须每轮都是同一个文案**。用多轮把那条偶发路径
// 变成必然被撞上的回归——单轮撞不上等于没测。
func TestHandle_SlowCheckTimeout_StableWording(t *testing.T) {
	ResetForTest()
	t.Cleanup(func() {
		SetCheckTimeout(defaultCheckTimeout)
		ResetForTest()
	})
	SetCheckTimeout(20 * time.Millisecond)

	RegisterCheck("slow", func(ctx context.Context) error {
		<-ctx.Done() // 挂死但 ctx-aware：超时到点后返回 ctx.Err()
		return ctx.Err()
	})
	RegisterCheck("fast", func(_ context.Context) error { return nil })

	const rounds = 200
	for i := 0; i < rounds; i++ {
		code, body := Handle(context.Background())
		if code != 503 {
			t.Fatalf("round %d: status = %d, want 503", i, code)
		}
		if !strings.Contains(body, "check timeout") {
			t.Fatalf("round %d: body = %s, want slow check recorded as timeout", i, body)
		}
	}
}

// TestHandle_CheckOwnTimeoutNotTotalTimeout 钉住「归一不能误伤」：
// check **自己**的依赖超时（DB 层抛的 context.DeadlineExceeded）发生在
// 总超时之前，那是 check 自己的错，应原样上报，不能被归一成
// "check timeout"——否则运维分不清「依赖挂了」和「健康检查整体超时」。
func TestHandle_CheckOwnTimeoutNotTotalTimeout(t *testing.T) {
	ResetForTest()
	t.Cleanup(func() {
		SetCheckTimeout(defaultCheckTimeout)
		ResetForTest()
	})
	SetCheckTimeout(2 * time.Second)

	RegisterCheck("db", func(_ context.Context) error {
		return context.DeadlineExceeded // DB 层自己的超时，与总超时无关
	})

	code, body := Handle(context.Background())
	if code != 503 {
		t.Fatalf("status = %d, want 503", code)
	}
	if !strings.Contains(body, "context deadline exceeded") {
		t.Errorf("body = %s, want check's own timeout reported verbatim", body)
	}
	if strings.Contains(body, "check timeout") {
		t.Errorf("body = %s, must not mislabel check's own timeout as total timeout", body)
	}
}
