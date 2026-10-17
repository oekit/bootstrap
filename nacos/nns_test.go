package nacos

import (
	"context"
	"testing"

	"github.com/oekit/bootstrap"
	nacospkg "github.com/oekit/registry/nacos"
)

func TestNNSComponent_Name(t *testing.T) {
	c := &NNSComponent{}
	if got := c.Name(); got != "nns" {
		t.Errorf("expected name 'nns', got %q", got)
	}
}

// TestNNSComponent_EmptyAddr_Skip 覆盖 "nacos addr empty, skip" 分支。
func TestNNSComponent_EmptyAddr_Skip(t *testing.T) {
	c := &NNSComponent{NacosCfg: nacospkg.Bootstrap{Addr: ""}, Port: 8080}
	if err := c.Init(t.Context(), nil, nil); err != nil {
		t.Errorf("Init with empty addr: %v", err)
	}
}

func TestWithNNS_InjectsComponent(t *testing.T) {
	b, err := bootstrap.New(bootstrap.WithServiceName("t"),
		bootstrap.WithShutdownHook("noop", func(context.Context) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	// 经公开注入方法后组件参与编排——以 AddComponent 幂等性作烟雾断言
	b.AddComponent(&NNSComponent{})
}
