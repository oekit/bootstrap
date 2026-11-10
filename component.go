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
//
// 阶段位（#168）：默认注册为**前置关闭钩子**（App.AddPreCloseHook）——在
// drain 之后、AddResource 资源关闭之前执行，flush/WAL 摘要/许可证释放类
// 钩子的依赖资源（DB/连接）此刻仍在位。此前钩子经 mgr.Add 落到
// app-lifecycle:wait 之后（资源已全部关闭），flush 在依赖已关后才跑，
// 是 App 文档自己点名的反模式；需要旧的「全部关闭之后」窗口时置
// Late=true（钩子排在 app-lifecycle 四阶段之后执行）。
type ShutdownHookComponent struct {
	ComponentName string
	Hook          func(ctx context.Context) error
	// Late 置 true 时钩子经 mgr.Add 注册，排到 app-lifecycle:wait 之后
	// （全部资源关闭与后台排空之后）执行；缺省 false = 前置关闭钩子。
	Late bool
}

// Name 返回组件名称。
func (c *ShutdownHookComponent) Name() string {
	if c.ComponentName == "" {
		return "shutdown-hook"
	}
	return c.ComponentName
}

// Init 注册关闭钩子：缺省挂前置钩子窗（资源关闭之前，#168）；
// Late=true 时保持旧位置（mgr.Add，四阶段之后）。
func (c *ShutdownHookComponent) Init(_ context.Context, app *App, mgr *lifecycle.Manager) error {
	if c.Late {
		mgr.Add(c.Name(), c.Hook, 0)
		return nil
	}
	app.AddPreCloseHook(c.Name(), c.Hook)
	return nil
}
