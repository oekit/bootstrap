package healthz

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	pkgerrors "github.com/oekit/pkg/errors"
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

// TestHandle_DegradedStill200 钉住 oekit#140：check 返回 Degraded 表示软依赖
// 降级——HTTP 仍 200（不触发 503 摘流）、整体 status="degraded"、降级项
// 记入 degraded map 且不出现在 checks（硬失败）map。
func TestHandle_DegradedStill200(t *testing.T) {
	reset()

	RegisterCheck("db", func(_ context.Context) error { return nil })
	RegisterCheck("cache", func(_ context.Context) error {
		return Degraded(errors.New("cache unreachable"))
	})

	status, body := Handle(context.Background())
	if status != 200 {
		t.Errorf("status = %d, want 200（降级不摘流，oekit#140）", status)
	}
	if !strings.Contains(body, `"status":"degraded"`) {
		t.Errorf("body = %s, want status:degraded", body)
	}
	if !strings.Contains(body, `"degraded":{"cache":"cache unreachable"}`) {
		t.Errorf("body = %s, want degraded map 带降级原因", body)
	}
	if strings.Contains(body, `"checks"`) {
		t.Errorf("body = %s, 降级项不得进入 checks（硬失败 map）", body)
	}
}

// TestHandle_Degraded_HardFailureWins 钉住 oekit#140：降级与硬失败并存时
// 摘流优先——503 + unhealthy，硬失败进 checks，降级项仍随 body 可见。
func TestHandle_Degraded_HardFailureWins(t *testing.T) {
	reset()

	RegisterCheck("mysql", func(_ context.Context) error {
		return errors.New("connection refused")
	})
	RegisterCheck("cache", func(_ context.Context) error {
		return Degraded(errors.New("cache unreachable"))
	})

	status, body := Handle(context.Background())
	if status != 503 {
		t.Errorf("status = %d, want 503（硬失败优先于降级）", status)
	}
	if !strings.Contains(body, `"status":"unhealthy"`) {
		t.Errorf("body = %s, want status:unhealthy", body)
	}
	if !strings.Contains(body, `"mysql":"connection refused"`) {
		t.Errorf("body = %s, want checks 带硬失败详情", body)
	}
	if !strings.Contains(body, `"degraded":{"cache":"cache unreachable"}`) {
		t.Errorf("body = %s, want degraded map 仍可见", body)
	}
}

// TestHandle_Degraded_MarkClosedStill503 钉住 oekit#140：registry 进入关闭态
// 后一律 503——降级语义不改变「应用关闭即不可用」的判定。
func TestHandle_Degraded_MarkClosedStill503(t *testing.T) {
	reset()

	RegisterCheck("cache", func(_ context.Context) error {
		return Degraded(errors.New("cache unreachable"))
	})
	MarkClosed()

	status, body := Handle(context.Background())
	if status != 503 {
		t.Errorf("status = %d, want 503（closed 优先）", status)
	}
	if !strings.Contains(body, `"closed":true`) {
		t.Errorf("body = %s, want closed:true", body)
	}
}

// TestDegradedError_UnwrapChain 钉住 Degraded 的错误链语义（与
// hertz/health.DegradedError 对齐）：errors.Is/As 能穿透到底层错误。
func TestDegradedError_UnwrapChain(t *testing.T) {
	inner := errors.New("dial timeout")
	err := Degraded(inner)

	var deg *DegradedError
	if !errors.As(err, &deg) {
		t.Fatal("errors.As 应识别 DegradedError")
	}
	if !errors.Is(err, inner) {
		t.Fatal("errors.Is 应穿透到底层错误")
	}
	if got := err.Error(); got != "dial timeout" {
		t.Errorf("Error() = %q, want 底层错误文案", got)
	}
	if got := Degraded(nil).Error(); got != "degraded" {
		t.Errorf("Degraded(nil).Error() = %q, want %q", got, "degraded")
	}
}

// TestSafeRun_PanicErrorIdentity 回归 #182：check 以 panic(err) 失败时，
// 错误身份经 pkg/errors.Recovered 收编后不丢——errors.Is/As 可穿透
// PanicError 直达原始错误（旧实现 fmt.Errorf("check panic: %v", r) 把身份
// 拍平成纯文本）；栈与类型信息在场。
func TestSafeRun_PanicErrorIdentity(t *testing.T) {
	sentinel := errors.New("check sentinel boom")

	err := safeRun(context.Background(), func(context.Context) error {
		panic(sentinel)
	})
	if err == nil {
		t.Fatal("check panic 必须转为错误")
	}
	var perr *pkgerrors.PanicError
	if !errors.As(err, &perr) {
		t.Fatalf("应为 *pkgerrors.PanicError（#182 单源）, got %T: %v", err, err)
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("panic(error) 的错误身份丢失（#182）, got %v", err)
	}
	if len(perr.Stack) == 0 {
		t.Fatal("PanicError 应携带 panic 现场栈")
	}
}
