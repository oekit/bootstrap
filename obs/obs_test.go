package obs

import (
	"testing"

	"github.com/oekit/tracing"
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

// TestTracingComponent_EmptyEndpoint_NoOp 覆盖 Endpoint 为空的早退路径。
func TestTracingComponent_EmptyEndpoint_NoOp(t *testing.T) {
	c := &TracingComponent{Config: tracing.Config{}}
	if err := c.Init(t.Context(), nil, nil); err != nil {
		t.Errorf("Init with empty endpoint: %v", err)
	}
}
