package obs

import (
	"context"
	"testing"
	"time"

	"github.com/oekit/core/lifecycle"
	"github.com/oekit/tracing"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

func TestMetricsComponent_NameAndSetServiceName(t *testing.T) {
	c := &MetricsComponent{ServiceName: "test"}
	if got := c.Name(); got != "metrics" {
		t.Errorf("expected name 'metrics', got %q", got)
	}
	c.SetServiceName("svc-2")
	if c.ServiceName != "svc-2" {
		t.Errorf("SetServiceName: got %q", c.ServiceName)
	}
	if err := c.Init(t.Context(), nil, nil); err != nil {
		t.Errorf("Init: %v", err)
	}
}

// TestTracingComponent_EmptyEndpoint_InstallsLocalProvider 钉住 tracing#60：
// 空 Endpoint 不再「什么都不装」（连传播器都没有，跨服务 trace 断链且静默），
// 而是与直接调用 tracing.Init 的空端点语义对齐——安装本地 provider + 全局
// 传播器（不建导出器、无网络），并注册 shutdown 钩子。
func TestTracingComponent_EmptyEndpoint_InstallsLocalProvider(t *testing.T) {
	c := &TracingComponent{Config: tracing.Config{}}
	mgr := lifecycle.New(2 * time.Second)

	if err := c.Init(t.Context(), nil, mgr); err != nil {
		t.Fatalf("Init with empty endpoint: %v", err)
	}

	// 传播器已安装：能从 W3C traceparent 头提取有效 SpanContext
	// （未装传播器时 noop propagator 提取不到——正是 #60 的断链形态）
	carrier := propagation.MapCarrier{
		"traceparent": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
	}
	ctx := otel.GetTextMapPropagator().Extract(context.Background(), carrier)
	if !trace.SpanContextFromContext(ctx).IsValid() {
		t.Error("global propagator must extract traceparent after Init with empty endpoint")
	}

	// shutdown 钩子已注册且可执行（本地 provider 关闭无网络副作用；
	// Manager.Shutdown 无返回值，钩子内 provider.Shutdown 的失败会经
	// Manager 的错误日志面暴露——此处只需不 panic）
	mgr.Shutdown(context.Background())
}
