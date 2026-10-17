// Package nacos 提供 bootstrap 的 Nacos/NNS 服务治理接入组件。
//
// 本包是独立嵌套 module（github.com/oekit/bootstrap/nacos），以
// BootstrapperOption 函数的形式向 bootstrap 注入 NNS 能力，使根包
// github.com/oekit/bootstrap 保持零 Nacos 依赖——不需要 Nacos 的服务
// 不会在 go.mod 中引入 nacos-sdk-go 及其传递依赖（阿里云 SDK 等 50+ 模块）。
//
// # 用法
//
//	b, _ := bootstrap.New(
//	    bootstrap.WithServiceName("my-service"),
//	    nacos.WithNNS(cfg.Nacos, cfg.Port, cfg.DepServices),
//	)
//	err := b.Run(userFn)
//
// 强依赖场景（初始化失败即启动失败）使用 WithRequiredNNS。
package nacos

import (
	"context"
	"fmt"

	"github.com/oekit/bootstrap"
	"github.com/oekit/core/lifecycle"
	"github.com/oekit/core/version"
	"github.com/oekit/logx"
	nns "github.com/oekit/nns"
	"github.com/oekit/registry"
	nacospkg "github.com/oekit/registry/nacos"
)

var logger = logx.Delegating("bootstrap-nacos")

// NNSComponent NNS 服务治理组件，负责 NNS 客户端初始化和服务注册。
// （自 bootstrap 根包迁移，行为不变。）
type NNSComponent struct {
	NacosCfg    nacospkg.Bootstrap
	Port        int
	DepServices []string
	NSNOpts     []nns.NNSOption
	// Required 为 true 时，NNS 初始化失败将返回错误（fail-fast），
	// 触发 Bootstrapper 的回滚与启动失败；为 false（默认）时仅告警并
	// 继续启动（服务对 NNS 为软依赖）。
	Required bool
}

// Name 返回组件名称。
func (c *NNSComponent) Name() string { return "nns" }

// nnsClient 是组件对 NNS 门面的最小依赖面（*nns.NNSClient 隐式满足），
// 供测试替换（newNNSClient 缝）。
type nnsClient interface {
	Init(ctx context.Context, serviceName string, depServices []string) error
	Register(ctx context.Context, port int, opts ...registry.RegisterOptionFunc) error
	Deregister(ctx context.Context, port int, opts ...registry.RegisterOptionFunc) error
	Close()
}

// newNNSClient 是 nns.New 的测试缝。
var newNNSClient = func(cfg nacospkg.Bootstrap, opts ...nns.NNSOption) nnsClient {
	return nns.New(cfg, opts...)
}

// Init 初始化 NNS 客户端；服务注册延迟到 app.Ready() 之后（nacos/v0.3.0 起，
// 对齐 go-kratos「server 全部就绪 → Register；Stop 先 Deregister」语义）。
//
// 契约：userFn 必须在 server 就绪后调用 app.Ready()（推荐经 hertz/kitex
// RunServer 的 WithOnReady），否则服务不会进入注册中心；Ready 永不触发时
// 注册 goroutine 随应用关闭的 ctx 取消退出，不产生任何注册。
func (c *NNSComponent) Init(ctx context.Context, app *bootstrap.App, mgr *lifecycle.Manager) error {
	if c.NacosCfg.Addr == "" {
		logger.Info("nacos addr empty, skipping NNS init")
		return nil
	}

	nnsc := newNNSClient(c.NacosCfg, c.NSNOpts...)

	if err := nnsc.Init(ctx, c.NacosCfg.ServiceName, c.DepServices); err != nil {
		// 释放已创建的 naming/config 客户端（各带后台 goroutine），
		// 避免 init 失败路径泄漏资源
		nnsc.Close()
		if c.Required {
			return fmt.Errorf("nns init (required): %w", err)
		}
		logger.Warn("NNS init failed, continuing without NNS", "err", err)
		return nil
	}
	logger.Info("NNS initialized", "service", c.NacosCfg.ServiceName)

	if c.Port > 0 {
		app.GoBackground("nns-register", func(ctx context.Context) {
			select {
			case <-ctx.Done():
				return
			case <-app.ReadyChan():
			}
			if err := nnsc.Register(ctx, c.Port, registry.WithMetadata(version.Metadata())); err != nil {
				logger.Error("nns register failed (deferred) — service will NOT be discoverable", "err", err)
				return
			}
			// 注册成功后才挂反注册。app.AddResource 落在 close 阶段且按注册
			// 逆序释放：userFn 先注册 server 资源 → Ready → 此处后注册
			// ⇒ 关停序为 nns-deregister → server.Shutdown（对齐 go-kratos Stop）。
			app.AddResource("nns-deregister", func(ctx context.Context) error {
				return nnsc.Deregister(ctx, c.Port)
			})
			logger.Info("registered (deferred)", "service", c.NacosCfg.ServiceName, "port", c.Port)
		})
	}

	mgr.Add("nns-client", func(_ context.Context) error {
		nnsc.Close()
		return nil
	}, 0)

	return nil
}

// WithNNS 添加 NNS 服务治理组件。
//
// nacosCfg 是 Nacos 连接配置，port 是服务注册端口（0 则跳过注册），
// depServices 是依赖服务列表，nnsOpts 传递给 nns.New。
//
// nacos/v0.3.0 起注册延迟到 app.Ready() 之后：userFn 须在 server 就绪后
// 触发就绪门（推荐 hertz/kitex RunServer 的 WithOnReady），关停时反注册
// 先于 server 关闭执行。
func WithNNS(nacosCfg nacospkg.Bootstrap, port int, depServices []string, nnsOpts ...nns.NNSOption) bootstrap.BootstrapperOption {
	return func(b *bootstrap.Bootstrapper) {
		b.AddComponent(&NNSComponent{
			NacosCfg:    nacosCfg,
			Port:        port,
			DepServices: depServices,
			NSNOpts:     nnsOpts,
		})
	}
}

// WithRequiredNNS 添加 NNS 组件并声明为强依赖：
// NNS 初始化失败时启动失败（fail-fast），而非默认的告警继续。
func WithRequiredNNS(nacosCfg nacospkg.Bootstrap, port int, depServices []string, nnsOpts ...nns.NNSOption) bootstrap.BootstrapperOption {
	return func(b *bootstrap.Bootstrapper) {
		b.AddComponent(&NNSComponent{
			NacosCfg:    nacosCfg,
			Port:        port,
			DepServices: depServices,
			NSNOpts:     nnsOpts,
			Required:    true,
		})
	}
}
