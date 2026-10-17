// Package bootstrap 定义组件化启动编排。
//
// 本文件保留组件接口与轻量组件；重量级组件（metrics/tracing/NNS）
// 位于嵌套 module：github.com/oekit/bootstrap/obs 与
// github.com/oekit/bootstrap/nacos，以 BootstrapperOption 注入。
package bootstrap

import (
	"context"

	"github.com/oekit/core/lifecycle"
)

// Component 是可插拔的启动组件接口。
// 每个组件负责一项启动任务（如指标注册、追踪初始化、服务注册等），
// 并可通过 mgr 注册关闭钩子。
type Component interface {
	// Name 返回组件标识，用于日志。
	Name() string
	// Init 执行组件初始化，可通过 app 和 mgr 注册资源和关闭钩子。
	Init(ctx context.Context, app *App, mgr *lifecycle.Manager) error
}

// ShutdownHookComponent 通用关闭钩子组件，用于注册自定义资源释放逻辑。
type ShutdownHookComponent struct {
	ComponentName string
	Hook          func(ctx context.Context) error
}

// Name 返回组件名称。
func (c *ShutdownHookComponent) Name() string {
	if c.ComponentName == "" {
		return "shutdown-hook"
	}
	return c.ComponentName
}

// Init 将关闭钩子注册到 lifecycle.Manager。
func (c *ShutdownHookComponent) Init(_ context.Context, _ *App, mgr *lifecycle.Manager) error {
	mgr.Add(c.Name(), c.Hook, 0)
	return nil
}
