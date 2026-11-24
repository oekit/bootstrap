package nacos

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/oekit/bootstrap"
	"github.com/oekit/core/lifecycle"
	nacospkg "github.com/oekit/registry/nacos"
)

// TestNNSComponent_SetServiceName_BackfillsOnlyWhenEmpty 钉住 #255①：
// NNSComponent 必须实现 bootstrap.ServiceNameAware——空值回填（WithServiceName
// 的服务名补进 NacosCfg.ServiceName）、幂等（New 统一回填与 AddComponent 就地
// 补做可能各跑一次，不互相覆盖）、不覆盖 yaml 显式配置的 nacos.service_name。
func TestNNSComponent_SetServiceName_BackfillsOnlyWhenEmpty(t *testing.T) {
	c := &NNSComponent{NacosCfg: nacospkg.Bootstrap{Addr: "127.0.0.1:8848"}}
	c.SetServiceName("svc-a")
	if got := c.NacosCfg.ServiceName; got != "svc-a" {
		t.Fatalf("空值回填 = %q, want svc-a", got)
	}
	c.SetServiceName("svc-b")
	if got := c.NacosCfg.ServiceName; got != "svc-a" {
		t.Fatalf("SetServiceName 应幂等（不覆盖已有值）= %q, want svc-a", got)
	}

	explicit := &NNSComponent{NacosCfg: nacospkg.Bootstrap{
		Addr:        "127.0.0.1:8848",
		ServiceName: "yaml-svc",
	}}
	explicit.SetServiceName("other")
	if got := explicit.NacosCfg.ServiceName; got != "yaml-svc" {
		t.Fatalf("yaml 显式配置的 service_name 不得被覆盖 = %q, want yaml-svc", got)
	}
}

// TestBootstrapper_WithServiceName_BackfillsNNSComponent 钉住 #255 的全链路：
// bootstrap.New(WithServiceName) 挂载的 NNSComponent 拿到服务名，且 Init 把
// 它传给 nns 客户端——此前组件不实现 ServiceNameAware，WithServiceName 的
// 服务名到不了 NacosCfg，漏配 service_name 时三链失效且无一条指出根因。
// 书写顺序取「组件先挂、服务名后给」：回填由 New 的统一回填完成。
func TestBootstrapper_WithServiceName_BackfillsNNSComponent(t *testing.T) {
	fake := withFakeNNS(t)
	comp := &NNSComponent{
		NacosCfg: nacospkg.Bootstrap{Addr: "127.0.0.1:8848"}, // 漏写 service_name
		Port:     0,                                          // 无注册面：Init 不起后台任务
	}
	if _, err := bootstrap.New(
		bootstrap.WithComponent(comp),
		bootstrap.WithServiceName("injected-svc"),
	); err != nil {
		t.Fatal(err)
	}
	if got := comp.NacosCfg.ServiceName; got != "injected-svc" {
		t.Fatalf("New 未回填服务名 = %q, want injected-svc", got)
	}

	mgr := lifecycle.New(2 * time.Second)
	app := bootstrap.NewApp(mgr)
	if err := comp.Init(t.Context(), app, mgr); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if got := fake.initServiceName(); got != "injected-svc" {
		t.Fatalf("nns 客户端收到的服务名 = %q, want injected-svc", got)
	}
	mgr.Shutdown(context.Background())
}

// TestNNSComponent_InitMissingServiceName_SoftSkips 钉住 #255②（软依赖）：
// Addr 已配但 ServiceName 为空时在建客户端前短路——不产生三条泛化失败日志，
// 状态记 PhaseSkipped + ErrServiceNameEmpty（文案点名 nacos.service_name），
// 对照 Addr=="" 的一条 Info 口径。
func TestNNSComponent_InitMissingServiceName_SoftSkips(t *testing.T) {
	fake := withFakeNNS(t)
	comp := &NNSComponent{NacosCfg: nacospkg.Bootstrap{Addr: "127.0.0.1:8848"}, Port: 8080}

	mgr := lifecycle.New(2 * time.Second)
	app := bootstrap.NewApp(mgr)
	if err := comp.Init(t.Context(), app, mgr); err != nil {
		t.Fatalf("软依赖缺服务名不应报错, got %v", err)
	}
	if n := fake.initCount(); n != 0 {
		t.Fatalf("缺服务名应在建客户端前短路, client.Init 被调 %d 次", n)
	}
	st := comp.RegistrationState()
	if st == nil {
		t.Fatal("RegistrationState 不应为 nil")
	}
	if st.Phase != PhaseSkipped {
		t.Fatalf("Phase = %q, want skipped", st.Phase)
	}
	if !errors.Is(st.Err, ErrServiceNameEmpty) {
		t.Fatalf("跳过根因应记 ErrServiceNameEmpty, got %v", st.Err)
	}
	if !strings.Contains(st.Err.Error(), "nacos.service_name") {
		t.Fatalf("错误文案必须点名 nacos.service_name, got %s", st.Err)
	}
	mgr.Shutdown(context.Background())
}

// TestNNSComponent_InitMissingServiceName_RequiredFails 钉住 #255②（强依赖）：
// Required 场景缺服务名 fail-fast——返回 ErrServiceNameEmpty（errors.Is 可辨、
// 文案点名 nacos.service_name），且同样不建客户端。
func TestNNSComponent_InitMissingServiceName_RequiredFails(t *testing.T) {
	fake := withFakeNNS(t)
	comp := &NNSComponent{
		NacosCfg: nacospkg.Bootstrap{Addr: "127.0.0.1:8848"},
		Port:     8080,
		Required: true,
	}

	mgr := lifecycle.New(2 * time.Second)
	app := bootstrap.NewApp(mgr)
	err := comp.Init(t.Context(), app, mgr)
	if err == nil {
		t.Fatal("Required 场景缺服务名必须 fail-fast")
	}
	if !errors.Is(err, ErrServiceNameEmpty) {
		t.Fatalf("错误应包 ErrServiceNameEmpty, got %v", err)
	}
	if !strings.Contains(err.Error(), "nacos.service_name") {
		t.Fatalf("错误文案必须点名 nacos.service_name, got %s", err)
	}
	if n := fake.initCount(); n != 0 {
		t.Fatalf("缺服务名应在建客户端前短路, client.Init 被调 %d 次", n)
	}
	mgr.Shutdown(context.Background())
}
