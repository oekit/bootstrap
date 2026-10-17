// Package obs 提供 bootstrap 的可观测性接入组件（独立嵌套 module）。
//
// 以 BootstrapperOption 函数的形式注入 Prometheus 指标（oekit/metrics）
// 与 OpenTelemetry 追踪（oekit/tracing），使根包 github.com/oekit/bootstrap
// 保持零 prometheus/otel 依赖。
//
// # 用法
//
//	b, _ := bootstrap.New(
//	    bootstrap.WithServiceName("my-service"),
//	    obs.WithMetrics(),
//	    obs.WithTracing(tracing.Config{Endpoint: "otel:4317"}),
//	)
//	err := b.Run(userFn)
package obs

import (
	"context"
	"fmt"

	"github.com/oekit/bootstrap"
	"github.com/oekit/core/lifecycle"
	"github.com/oekit/logx"
	"github.com/oekit/metrics"
	"github.com/oekit/tracing"
)

var logger = logx.Delegating("bootstrap-obs")

// MetricsComponent Prometheus 指标注册组件。
type MetricsComponent struct {
	// ServiceName 用于 Prometheus 指标中的 service 标签。
	// 由 bootstrap.New 在应用完全部选项后经 ServiceNameAware 接口统一回填。
	ServiceName string
}

// Name 返回组件名称。
func (c *MetricsComponent) Name() string { return "metrics" }

// SetServiceName 实现 bootstrap.ServiceNameAware，由 New 统一回填。
func (c *MetricsComponent) SetServiceName(name string) { c.ServiceName = name }

// Init 注册 Prometheus 指标。
func (c *MetricsComponent) Init(_ context.Context, _ *bootstrap.App, _ *lifecycle.Manager) error {
	metrics.Init(c.ServiceName)
	logger.Info("metrics initialized")
	return nil
}

// TracingComponent OpenTelemetry 追踪初始化组件。
type TracingComponent struct {
	Config tracing.Config
}

// Name 返回组件名称。
func (c *TracingComponent) Name() string { return "tracing" }

// Init 初始化 OpenTelemetry 追踪，并注册 shutdown 钩子。
func (c *TracingComponent) Init(_ context.Context, _ *bootstrap.App, mgr *lifecycle.Manager) error {
	if c.Config.Endpoint == "" {
		return nil
	}

	provider, err := tracing.Init(c.Config)
	if err != nil {
		return fmt.Errorf("tracing init: %w", err)
	}
	mgr.Add("tracing", func(ctx context.Context) error {
		return provider.Shutdown(ctx)
	}, 0)
	logger.Info("tracing initialized", "endpoint", c.Config.Endpoint)
	return nil
}

// WithMetrics 添加 Prometheus 指标注册组件。
// serviceName 由 bootstrap.New 经 ServiceNameAware 统一回填，与
// WithServiceName 的调用顺序无关。
func WithMetrics() bootstrap.BootstrapperOption {
	return func(b *bootstrap.Bootstrapper) {
		b.AddComponent(&MetricsComponent{})
	}
}

// WithTracing 添加 OpenTelemetry 追踪组件。
func WithTracing(cfg tracing.Config) bootstrap.BootstrapperOption {
	return func(b *bootstrap.Bootstrapper) {
		b.AddComponent(&TracingComponent{Config: cfg})
	}
}
