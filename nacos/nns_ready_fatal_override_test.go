package nacos

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/oekit/bootstrap"
	"github.com/oekit/core/lifecycle"
)

func boolPtr(v bool) *bool { return &v }

// TestReadyFatalOverride_EscapesGlobalFatal 钉住 #202（逃生舱形态）：
// 全局 WithReadyFatal() 置位后，组件显式 ReadyFatalOverride=false 必须能按
// 组件粒度关掉 fail-fast——此前 `c.ReadyFatal || readyFatal.Load()` 下组件
// 字段只有「开启」权，多 Bootstrapper / 宿主进程场景无法按组件关掉全局
// fail-fast（行内注释「组件字段优先级高于全局缺省」不成立）。
// 对照组：同全局开关下未覆盖的组件照常 fail-fast（全局语义不变）。
func TestReadyFatalOverride_EscapesGlobalFatal(t *testing.T) {
	fake := withFakeNNS(t)

	prevFatal := readyFatal.Load()
	defer readyFatal.Store(prevFatal)
	readyFatal.Store(true) // 唯一变量：全局开关 ON

	comp := newTestComponent()
	comp.ReadyTimeout = 50 * time.Millisecond
	comp.ReadyFatalOverride = boolPtr(false) // 显式关（逃生舱）

	cap := &captureLogger{}
	restore := swapLogger(t, cap)
	defer restore()

	mgr := lifecycle.New(2 * time.Second)
	app := bootstrap.NewApp(mgr)
	if err := comp.Init(context.Background(), app, mgr); err != nil {
		t.Fatalf("Init: %v", err)
	}

	// 过看门狗窗口：组件显式关 ⇒ 只告警、不进 PhaseFailed（#33 语义）。
	time.Sleep(150 * time.Millisecond)
	st := comp.RegistrationState()
	if st != nil && st.Phase == PhaseFailed {
		t.Fatalf("组件显式 ReadyFatalOverride=false 应覆盖全局 fail-fast，不应 PhaseFailed, got %+v", st)
	}
	if fake.registerCount() != 0 {
		t.Fatal("Ready 未触发不应注册")
	}
	// 全局置位下告警仍应打（可观测性不因关闭 fail-fast 而消失）。
	if !cap.containsSubstring("app.Ready() not called") {
		t.Errorf("看门狗超时仍应打 ERROR 告警, errors=%v", cap.errorMessages())
	}

	mgr.Shutdown(context.Background())
}

// TestReadyFatalOverride_TrueLiftsGlobal 钉住 #202 的抬升面：全局关闭时
// 组件显式 Override=true 等价于 ReadyFatal: true——三态字段双向都可用。
func TestReadyFatalOverride_TrueLiftsGlobal(t *testing.T) {
	fake := withFakeNNS(t)

	prevFatal := readyFatal.Load()
	defer readyFatal.Store(prevFatal)
	readyFatal.Store(false) // 全局 OFF

	comp := newTestComponent()
	comp.ReadyTimeout = 50 * time.Millisecond
	comp.ReadyFatalOverride = boolPtr(true)

	cap := &captureLogger{}
	restore := swapLogger(t, cap)
	defer restore()

	mgr := lifecycle.New(2 * time.Second)
	app := bootstrap.NewApp(mgr)
	if err := comp.Init(context.Background(), app, mgr); err != nil {
		t.Fatalf("Init: %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if st := comp.RegistrationState(); st != nil && st.Phase == PhaseFailed {
			if !errors.Is(st.Err, ErrReadyFatal) {
				t.Fatalf("Override=true 应触发 fatal 哨兵, got %v", st.Err)
			}
			if fake.registerCount() != 0 {
				t.Fatal("fatal 路径不应注册")
			}
			mgr.Shutdown(context.Background())
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("ReadyFatalOverride=true 应 fail-fast 进 PhaseFailed, captured=%v", cap.errorMessages())
}

// TestReadyFatalOrSemanticsUnchanged 钉住 #202 的兼容面：nil 覆盖（缺省）
// 维持现状判定——裸 ReadyFatal=true 仍可从全局关闭抬升（#74 原语义不变）。
func TestReadyFatalOrSemanticsUnchanged(t *testing.T) {
	withFakeNNS(t)

	prevFatal := readyFatal.Load()
	defer readyFatal.Store(prevFatal)
	readyFatal.Store(false) // 全局 OFF，唯一抬升来源是裸字段

	comp := newTestComponent()
	comp.ReadyTimeout = 50 * time.Millisecond
	comp.ReadyFatal = true // 无 Override（nil）

	cap := &captureLogger{}
	restore := swapLogger(t, cap)
	defer restore()

	mgr := lifecycle.New(2 * time.Second)
	app := bootstrap.NewApp(mgr)
	if err := comp.Init(context.Background(), app, mgr); err != nil {
		t.Fatalf("Init: %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if st := comp.RegistrationState(); st != nil && st.Phase == PhaseFailed {
			mgr.Shutdown(context.Background())
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("裸 ReadyFatal=true 的抬升语义（#74）不得被 #202 改动, captured=%v", cap.errorMessages())
}
