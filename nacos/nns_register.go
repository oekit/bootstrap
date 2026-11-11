// 注册流程（延迟注册面）：就绪门看门狗（registerWhenReady）→ 注册与反注册
// 挂载（registerAndWire）→ 取消-放弃处置（abandonRegister）。自 nns.go 拆出
// （#180），行为不变——拆分前是 Init 内 120 行的 GoBackground 闭包
// （select 嵌套最深 5 层、6 个关注点）。

package nacos

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/oekit/bootstrap"
	"github.com/oekit/core/version"
	"github.com/oekit/registry"
)

// abandonedDeregisterTimeout 是注册被取消后 best-effort 反注册的独立短超时
// （#136）：不能用已取消的根 ctx（Deregister 会立即失败），也不允许反注册
// 拖住关停——超时/失败仅留痕，不重试。
const abandonedDeregisterTimeout = 2 * time.Second

// registerWhenReady 等待就绪门并执行看门狗纪律（#33/#74），返回是否应继续
// 注册：true = 已就绪（含「超时告警后继续等、最终就绪」的形态）；false =
// 关停中 ctx 取消（状态已标 PhaseShutdown）或 ReadyFatal 超时已 fail-fast
// （状态已标 PhaseFailed，ErrReadyFatal 已经 App.Fail 上抛），不再注册。
//
// 看门狗：Ready 迟迟不来时告警——此前是完全静默的，「注册失败」与
// 「根本没尝试注册」在日志上无法区分（bootstrap#33）。超时非 fatal 时
// 继续等就绪信号：慢启动不应被误判为失败，就绪后仍照常注册（只是已经
// 告警过）。ReadyFatal（#74）：fail-fast 中止启动。裸 h.Spin() 消费方
// 永远不会触发 app.Ready() ⇒ 静默永不注册——「用了 Nacos 但服务没进
// 注册中心」的故障排查时不会先想到启动日志里一条 30s 前的 ERROR，注册
// 中心缺失等于服务不存在，早死优于静默降级。出口为 App.Fail（#133）：
// 哨兵错误经 Bootstrapper.Run 的返回值上抛（退出码非 0，
// errors.Is(err, ErrReadyFatal) 可辨），且 userFn 不监听 ctx 的裸 Spin
// 形态下 Run 也被解除阻塞——此前的 RequestShutdown 纯取消出口在裸 Spin
// 下是 no-op（Run 永不返回）、在监听 ctx 下返回 nil（退出码 0，启动
// 失败被部署门禁当「成功」），fail-fast 双重失效。
func (c *NNSComponent) registerWhenReady(ctx context.Context, app *bootstrap.App) bool {
	timeout := c.ReadyTimeout
	if timeout == 0 {
		timeout = time.Duration(readyTimeoutNanos.Load())
	}
	fatal := c.ReadyFatal || readyFatal.Load() // 组件字段优先级高于全局缺省（#74）
	var timer *time.Timer
	var timeoutCh <-chan time.Time
	if timeout > 0 {
		timer = time.NewTimer(timeout)
		timeoutCh = timer.C
		defer timer.Stop()
	}

	select {
	case <-ctx.Done():
		c.markShutdownWithoutReady()
		return false
	case <-app.ReadyChan():
		return true
	case <-timeoutCh:
		// 超时未就绪：明确告警——服务不会注册到注册中心。
		logger.Error("app.Ready() not called within timeout — service will NOT be registered to Nacos; "+
			"check that userFn calls app.Ready() after server starts (hertz/kitex RunServer WithOnReady)",
			"timeout", timeout.String(), "service", c.NacosCfg.ServiceName)
		c.markReadyWaitExceeded()
		if fatal {
			c.setRegisterState(&RegisterState{
				Phase:             PhaseFailed,
				Service:           c.NacosCfg.ServiceName,
				Port:              c.Port,
				Err:               ErrReadyFatal,
				Since:             time.Now(),
				ReadyWaitExceeded: true,
			})
			app.Fail(ErrReadyFatal)
			return false
		}
		// 超时后继续等就绪信号：慢启动不应被误判为失败，
		// 就绪后仍照常注册（只是已经告警过）。
		select {
		case <-ctx.Done():
			c.markShutdownWithoutReady()
			return false
		case <-app.ReadyChan():
			return true
		}
	}
}

// registerAndWire 执行注册并挂载反注册 closer（状态机迁移随迁）：
// 注册失败区分「关停窗口 in-flight 取消」（#136 abandonRegister）与「明确
// 失败」（PhaseFailed + #77 Required fail-fast）；注册成功后挂
// nns-deregister（落 close 阶段且按注册逆序释放：userFn 先注册 server 资源
// → Ready → 此处后注册 ⇒ 关停序为 nns-deregister → server.Shutdown，
// 对齐 go-kratos Stop）。
func (c *NNSComponent) registerAndWire(ctx context.Context, app *bootstrap.App, nnsc nnsClient) {
	if err := nnsc.Register(ctx, c.Port, registry.WithMetadata(version.Metadata())); err != nil {
		// #136：ctx 取消（关停窗口 in-flight）≠ 明确失败——请求可能
		// 已到达服务端并生效（gRPC 取消不回滚服务端状态），直接退出
		// 会留下幽灵实例（存活到 nacos ephemeral 心跳超时，流量继续
		// 打向已停止的 server）。best-effort 反注册 + 独立状态面。
		if errors.Is(err, context.Canceled) {
			c.abandonRegister(nnsc, err)
			return
		}
		logger.Error("nns register failed (deferred) — service will NOT be discoverable", "err", err)
		c.setRegisterState(&RegisterState{
			Phase:   PhaseFailed,
			Service: c.NacosCfg.ServiceName,
			Port:    c.Port,
			Err:     err,
			Since:   time.Now(),
		})
		// Required fail-fast 贯穿到注册失败（nns#77）：注册是 Ready
		// 之后的延迟动作，Init 阶段的 fail-fast 管不到这里——强依赖
		// 服务「未注册但还在跑」等于对集群不可用，与初始化失败同罪。
		// 出口为 App.Fail（#133）：错误经 Bootstrapper.Run 返回值上抛
		// （退出码非 0），且 userFn 不监听 ctx（裸 h.Spin()）时也能
		// 解除 Run 阻塞——RequestShutdown 的纯取消语义在两种形态下
		// 都失效（Run 永不返回 / 返回 nil 退出码 0）。
		if c.Required {
			logger.Error("nns register failed on required component, failing fast",
				"service", c.NacosCfg.ServiceName, "err", err)
			app.Fail(fmt.Errorf("nns: register failed on required component %s: %w",
				c.NacosCfg.ServiceName, err))
		}
		return
	}
	app.AddResource("nns-deregister", func(ctx context.Context) error {
		return nnsc.Deregister(ctx, c.Port)
	})
	c.setRegisterState(&RegisterState{
		Phase:   PhaseRegistered,
		Service: c.NacosCfg.ServiceName,
		Port:    c.Port,
		Since:   time.Now(),
	})
	logger.Info("registered (deferred)", "service", c.NacosCfg.ServiceName, "port", c.Port)
}

// abandonRegister 处理注册请求被 ctx 取消的场景（#136）：关停窗口内
// in-flight 的 nnsc.Register 因根 ctx 取消拿到 context.Canceled，但请求
// 可能已到达服务端并生效（gRPC 取消不回滚服务端状态）——此时反注册
// closer 尚未挂上（只在注册成功后挂），直接退出会留下幽灵实例。
//
// 处置：用独立短超时 ctx best-effort 调一次 nnsc.Deregister（不重试、
// 不拖住关停），状态落 PhaseAbandoned（与 PhaseFailed「明确失败」和
// 「从未注册」可区分）；反注册失败时打 ERROR 留痕并记入
// RegisterState.DeregisterErr，供巡检核查幽灵实例。
func (c *NNSComponent) abandonRegister(nnsc nnsClient, regErr error) {
	logger.Error("nns register cancelled during shutdown — registration may have reached the server, best-effort deregistering",
		"service", c.NacosCfg.ServiceName, "err", regErr)

	deregCtx, cancel := context.WithTimeout(context.Background(), abandonedDeregisterTimeout)
	deregErr := nnsc.Deregister(deregCtx, c.Port)
	cancel()
	if deregErr != nil {
		logger.Error("nns best-effort deregister failed after cancelled register — ghost instance possible until nacos ephemeral timeout",
			"service", c.NacosCfg.ServiceName, "err", deregErr)
	}

	c.setRegisterState(&RegisterState{
		Phase:         PhaseAbandoned,
		Service:       c.NacosCfg.ServiceName,
		Port:          c.Port,
		Err:           regErr,
		DeregisterErr: deregErr,
		Since:         time.Now(),
	})
}
