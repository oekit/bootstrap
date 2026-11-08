package nacos

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/oekit/bootstrap"
	"github.com/oekit/core/lifecycle"
	"github.com/oekit/logx"
)

// TestNNSComponent_ReadyFatal_AbortsStartup 钉住 #74：ReadyFatal 开启后，看门狗
// 超时**中止启动**（Init 挂的注册 goroutine 以 ErrReadyFatal 告终），而非只打
// ERROR 继续静默降级。
func TestNNSComponent_ReadyFatal_AbortsStartup(t *testing.T) {
	fake := withFakeNNS(t)
	comp := newTestComponent()
	comp.ReadyTimeout = 50 * time.Millisecond
	comp.ReadyFatal = true

	cap := &captureLogger{}
	restore := swapLogger(t, cap)
	defer restore()

	mgr := lifecycle.New(2 * time.Second)
	app := bootstrap.NewApp(mgr)
	if err := comp.Init(context.Background(), app, mgr); err != nil {
		t.Fatalf("Init: %v", err)
	}

	// 等待看门狗超时并触发 fatal 路径：注册状态应进入 PhaseFailed + ErrReadyFatal
	deadline := time.Now().Add(3 * time.Second)
	var st *RegisterState
	for time.Now().Before(deadline) {
		if st = comp.RegistrationState(); st != nil && st.Phase == PhaseFailed {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if st == nil || st.Phase != PhaseFailed {
		t.Fatalf("ReadyFatal 超时后应进入 PhaseFailed, got %+v", st)
	}
	if !errors.Is(st.Err, ErrReadyFatal) {
		t.Fatalf("Err 应可 errors.Is(ErrReadyFatal), got %v", st.Err)
	}
	if n := fake.registerCount(); n != 0 {
		t.Fatalf("fatal 路径不应注册, got %d", n)
	}
	if !cap.containsSubstring("app.Ready() not called") {
		t.Errorf("fatal 前仍应打看门狗 ERROR; errors=%v", cap.errorMessages())
	}

	mgr.Shutdown(context.Background())
}

// TestWithReadyFatal_GlobalOption 全局选项形态：WithReadyFatal 须在 WithNNS
// 之前应用时生效（readyFatal 全局缺省被组件读取）。
func TestWithReadyFatal_GlobalOption(t *testing.T) {
	withFakeNNS(t)

	cap := &captureLogger{}
	restore := swapLogger(t, cap)
	defer restore()

	prevFatal := readyFatal.Load()
	defer readyFatal.Store(prevFatal) // 测试间隔离（包级状态）
	readyFatal.Store(true)

	comp := newTestComponent()
	comp.ReadyTimeout = 50 * time.Millisecond

	mgr := lifecycle.New(2 * time.Second)
	app := bootstrap.NewApp(mgr)
	if err := comp.Init(context.Background(), app, mgr); err != nil {
		t.Fatalf("Init: %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if st := comp.RegistrationState(); st != nil && st.Phase == PhaseFailed {
			if !errors.Is(st.Err, ErrReadyFatal) {
				t.Fatalf("全局选项应触发 fatal 哨兵, got %v", st.Err)
			}
			mgr.Shutdown(context.Background())
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("全局 WithReadyFatal 未生效; errors=%v", cap.errorMessages())
}

// TestNNSComponent_ReadyFatal_Off 默认（不开 fatal）行为不变：超时只告警，
// 不进 PhaseFailed（#33 语义回归钉）。
func TestNNSComponent_ReadyFatal_Off(t *testing.T) {
	fake := withFakeNNS(t)
	comp := newTestComponent()
	comp.ReadyTimeout = 50 * time.Millisecond

	cap := &captureLogger{}
	restore := swapLogger(t, cap)
	defer restore()

	mgr := lifecycle.New(2 * time.Second)
	app := bootstrap.NewApp(mgr)
	if err := comp.Init(context.Background(), app, mgr); err != nil {
		t.Fatalf("Init: %v", err)
	}

	// 等过看门狗超时窗口
	time.Sleep(150 * time.Millisecond)
	if st := comp.RegistrationState(); st != nil && st.Phase == PhaseFailed {
		t.Fatalf("默认不开 fatal 不应进入 PhaseFailed, got %+v", st)
	}
	if n := fake.registerCount(); n != 0 {
		t.Fatalf("Ready 未触发不应注册, got %d", n)
	}

	mgr.Shutdown(context.Background())
}

// 编译期守住 logx 引用（swapLogger 依赖），避免 unused import 漂移。
var _ = logx.Default
