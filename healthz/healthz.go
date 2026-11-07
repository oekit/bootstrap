// Package healthz 提供 ekit 应用健康检查 helper,让业务注册多个 check
// 项(DB / Redis / 上游依赖等),/healthz endpoint 一次调起,全部通过返 200,
// 任一硬失败返 503;软依赖降级(Degraded)返 200 + status "degraded"。
//
// 集成方式(hertz):
//
//	import "github.com/oekit/bootstrap/healthz"
//
//	healthz.RegisterCheck("mysql", func(ctx context.Context) error {
//	    return db.PingContext(ctx)
//	})
//	healthz.RegisterCheck("redis", func(ctx context.Context) error {
//	    return redisClient.Ping(ctx).Err()
//	})
//
//	h.GET("/healthz", func(ctx context.Context, c *app.RequestContext) {
//	    status, body := healthz.Handle(ctx) // 传 ctx（RequestContext 恰好实现
//	    // context.Context，传 c 能编译但超时语义错）；body 已是 JSON 字符串，
//	    // 用 c.Data 直写，勿用 c.JSON（会二次编码）
//	    c.Data(status, "application/json; charset=utf-8", []byte(body))
//	})
//
// 业务使用 liveness/readiness 分离(可选):
//   - /healthz  — 包含所有 check(默认;所有依赖就绪才返 200)
//   - /livez    — 只检查进程存活,永远 200(给 K8s livenessProbe)
//   - /readyz   — 等同 /healthz(给 K8s readinessProbe)
//
// # 三态与软依赖降级
//
// check 返回 Degraded(err) 表示软依赖降级:该项记入 Result.Degraded、整体
// status = StatusDegraded,但 HTTP 仍 200(不触发 503)——软依赖(缓存、旁路
// 服务)不可用不应让就绪探针失败导致实例被摘流,但保持可见供告警识别。
// 硬失败(普通 error)仍 503 + StatusUnhealthy。优先级:硬失败 > 降级 > ok。
//
// # 与 hertz/health 的规范与差异
//
// github.com/oekit/hertz/health 是 HTTP 服务健康端点的**规范形态**;本包是
// **传输无关的极简形态**——裸 JSON 有意为之(不假设调用方运行在 ekit
// middleware/信封栈上),适合非 hertz 传输(gRPC/kitex、CLI、定时任务)或
// 只需要 (statusCode, body) 二元组的场景。两者三态语义(ok / degraded /
// 硬失败)一致,差异速查:
//
//   - 响应形状:hertz/health 是 ekit 信封
//     {code,data:{status,checks:[{name,status,error}]},message,trace_id};
//     本包是裸 JSON {status,closed,checks:{name:错误},degraded:{name:原因}},
//     checks/degraded 是 map 且默认不含通过项
//   - 整体失败态命名:hertz/health 是 data.status = "error";本包是
//     status = "unhealthy"
//   - 默认总超时:hertz/health 3s(WithCheckTimeout 调整);本包 5s
//     (SetCheckTimeout 调整)
//   - 超时文案:hertz/health 未完成的 check 记 status=error + "timeout"
//     (默认脱敏,WithVerboseErrors 透出详情);本包记
//     "check timeout after 5s"(含总超时时长)
//   - 注册形态:hertz/health 每个 handler 独立 Options;本包是包级
//     singleton(全应用一个 registry)
//
// 设计:无外部依赖,业务按需注册;包级 singleton(全应用共享一个 registry)。
package healthz

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oekit/concurrency/async"
)

// Check 是单个健康检查项。返回 nil 表示通过;返回普通 error 表示硬失败
// (触发 503);返回 Degraded(err) 表示软依赖降级(HTTP 仍 200,记入
// Result.Degraded,见 Degraded)。
type Check func(ctx context.Context) error

// Result.Status 的取值。建议消费方用常量比较,不要硬编码字面量——
// 裸字符串比较一旦拼错(如 "degrade")是静默失效,恒不命中。
const (
	// StatusOK 全部 check 通过(或降级项也没有)。
	StatusOK = "ok"
	// StatusDegraded 存在软依赖降级项且无硬失败:HTTP 仍 200,供告警识别。
	StatusDegraded = "degraded"
	// StatusUnhealthy 存在硬失败项(或 registry 已 MarkClosed):503。
	StatusUnhealthy = "unhealthy"
)

// DegradedError 表示软依赖降级:不影响就绪(HTTP 仍 200),该 check 记入
// Result.Degraded、整体 status = StatusDegraded,供告警系统识别。与硬失败
// (普通 error → 503 + StatusUnhealthy)区分:软依赖(缓存、旁路服务)不可用
// 应保持可见,但不应让就绪探针失败导致实例被摘流。
// 语义与 hertz/health.DegradedError 三态对齐(oekit#140)。
type DegradedError struct {
	// Err 是导致降级的底层错误,经 Unwrap 暴露,可被 errors.Is/As 链识别。
	Err error
}

// Error 实现 error 接口。
func (e *DegradedError) Error() string {
	if e.Err == nil {
		return "degraded"
	}
	return e.Err.Error()
}

// Unwrap 返回底层错误,支持 errors.Is/As 链式判定。
func (e *DegradedError) Unwrap() error { return e.Err }

// Degraded 将错误标记为软依赖降级。
// 在 check 中返回 Degraded(err),Handle 仍返 200、Result.Status =
// StatusDegraded,该项的降级原因记入 Result.Degraded——不触发 503 摘流。
//
// 用法:
//
//	healthz.RegisterCheck("cache", func(ctx context.Context) error {
//	    if err := cache.Ping(ctx); err != nil {
//	        return healthz.Degraded(err) // 软依赖:降级可见但不摘流
//	    }
//	    return nil
//	})
func Degraded(err error) error { return &DegradedError{Err: err} }

// defaultCheckTimeout 是单次 Handle 允许的最长耗时;超时的 check 记为
// failure("check timeout")。K8s probe 有自己的 timeoutSeconds,健康端点
// 先行返回才能避免 kubelet 掐断连接后 goroutine 堆积;设为 0 关闭。
// 经 SetCheckTimeout 调整。
const defaultCheckTimeout = 5 * time.Second

var checkTimeoutNanos atomic.Int64

func init() { checkTimeoutNanos.Store(int64(defaultCheckTimeout)) }

// SetCheckTimeout 调整单次健康检查的总超时(含并发执行的所有 check)。
// d <= 0 关闭超时控制(不推荐:挂死的 check 会无限阻塞探测请求)。
func SetCheckTimeout(d time.Duration) { checkTimeoutNanos.Store(int64(d)) }

type checkEntry struct {
	name  string
	check Check
}

type registry struct {
	mu     sync.RWMutex
	checks map[string]checkEntry
	closed atomic.Bool
}

var global = &registry{checks: map[string]checkEntry{}}

// RegisterCheck 注册一个 check(同名覆盖)。
func RegisterCheck(name string, check Check) {
	global.mu.Lock()
	defer global.mu.Unlock()
	global.checks[name] = checkEntry{name: name, check: check}
}

// DeregisterCheck 删除一个 check(测试用,业务不常用)。
func DeregisterCheck(name string) {
	global.mu.Lock()
	defer global.mu.Unlock()
	delete(global.checks, name)
}

// MarkClosed 把 registry 标记为 closed(应用进入关闭流程),
// Handle 返 503 + {"status":"unhealthy","closed":true}。常通过
// lifecycle.App.AddResource 在关闭序列里调。
func MarkClosed() {
	global.closed.Store(true)
}

// ResetForTest 清空 registry + 重置 closed 标志(仅供 test 使用)。
func ResetForTest() {
	global.mu.Lock()
	defer global.mu.Unlock()
	global.checks = map[string]checkEntry{}
	global.closed.Store(false)
}

// Result 是 Handle 的输出(可在 HTTP handler 里转 JSON)。
type Result struct {
	Status string `json:"status"` // StatusOK / StatusDegraded / StatusUnhealthy
	Closed bool   `json:"closed"`
	// Checks 是硬失败项:check name → 错误信息。非空时 503。
	Checks map[string]string `json:"checks,omitempty"`
	// Degraded 是软依赖降级项:check name → 降级原因。不触发 503
	// (HTTP 仍 200,整体 status = StatusDegraded 供告警识别,oekit#140)。
	Degraded map[string]string `json:"degraded,omitempty"`
}

// Handle 并发执行所有注册的 check。返回 (statusCode, jsonBody) 供 HTTP handler 用。
//   - 全部 check 通过 + 未 closed → 200 + "ok"
//   - 任一 check 硬失败 → 503 + 失败详情
//   - 无硬失败但存在 Degraded 降级项 → 200 + status "degraded" + 降级详情
//   - MarkClosed 已调 → 503 + closed:true
//
// check 用 errgroup-like 模式并发跑,总耗时 = 最慢 check(而非总和),
// 适合多个外部依赖并行健康检查。整次 Handle 受总超时约束
// (默认 5s,SetCheckTimeout 调整/关闭):超时的 check 统一记为
// "check timeout",并且每个 check 的 ctx 注入 deadline,支持 ctx 的
// 依赖(DB Ping 等)能第一时间中止 — 挂死的 check 不再无限阻塞
// liveness/readiness 探测(2026-09-05 复审修复)。
func Handle(ctx context.Context) (int, string) {
	if global.closed.Load() {
		body, _ := json.Marshal(Result{Status: StatusUnhealthy, Closed: true})
		return 503, string(body)
	}

	global.mu.RLock()
	entries := make([]checkEntry, 0, len(global.checks))
	for _, e := range global.checks {
		entries = append(entries, e)
	}
	global.mu.RUnlock()

	if len(entries) == 0 {
		body, _ := json.Marshal(Result{Status: StatusOK})
		return 200, string(body)
	}

	// 排序让输出稳定(便于日志 diff)
	sort.Slice(entries, func(i, j int) bool { return entries[i].name < entries[j].name })

	// 总超时同时注入 check ctx(deadline 感知的依赖能提前中止)。
	//
	// **用 WithTimeoutCause 而非 WithTimeout**：cause 是「这次超时是**我们**
	// 设的总超时掐断的」这一事实的权威凭据。collect 侧据此判定一个
	// DeadlineExceeded 到底该记成「总超时」还是「check 自己的错」。
	runCtx := ctx
	timeout := time.Duration(checkTimeoutNanos.Load())
	if timeout > 0 {
		var cancel context.CancelFunc
		runCtx, cancel = context.WithTimeoutCause(ctx, timeout, errTotalTimeout)
		defer cancel()
	}

	type result struct {
		name string
		err  error
	}
	ch := make(chan result, len(entries))
	for _, e := range entries {
		// async.GoSafe:满足"禁止裸 go func"的项目约定;check 自身的 panic
		// 已由 safeRun 收编,这里兜底 goroutine 出口
		e := e
		async.GoSafe(func() {
			ch <- result{name: e.name, err: safeRun(runCtx, e.check)}
		})
	}

	var deadline <-chan time.Time
	if timeout > 0 {
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		deadline = timer.C
	}

	failures := map[string]string{}
	degraded := map[string]string{}
	returned := make(map[string]bool, len(entries))
collect:
	for done := 0; done < len(entries); done++ {
		select {
		case r := <-ch:
			returned[r.name] = true
			if r.err != nil {
				// DegradedError 的降级语义优先:不改判为硬失败/超时,
				// 记入 degraded map(与 hertz/health 的三态判定一致,oekit#140)
				var deg *DegradedError
				if errors.As(r.err, &deg) {
					degraded[r.name] = deg.Error()
				} else {
					failures[r.name] = failureText(r.err, runCtx, timeout)
				}
			}
		case <-deadline:
			break collect
		}
	}
	// 超时未返回的 check:goroutine 稍后写入 buffered ch(不泄漏 goroutine
	// 发送),结果记为 timeout
	if len(returned) < len(entries) {
		for _, e := range entries {
			if !returned[e.name] {
				failures[e.name] = fmt.Sprintf("check timeout after %s", timeout)
			}
		}
	}

	// 汇总优先级:硬失败 > 降级 > ok。降级不摘流(HTTP 200),硬失败 503;
	// 硬失败与降级并存时摘流优先,但降级项仍随 body 可见。
	if len(failures) > 0 {
		body, _ := json.Marshal(Result{Status: StatusUnhealthy, Checks: failures, Degraded: degraded})
		return 503, string(body)
	}
	if len(degraded) > 0 {
		body, _ := json.Marshal(Result{Status: StatusDegraded, Degraded: degraded})
		return 200, string(body)
	}
	body, _ := json.Marshal(Result{Status: StatusOK})
	return 200, string(body)
}

// errTotalTimeout 是「本次 Handle 的总超时到点」的 cause 标记。
// 见 failureText 的说明：它是「这次超时由我们掐断」的权威凭据。
var errTotalTimeout = errors.New("healthz total timeout")

// failureText 把 check 返回的 error 归一成对外文案。
//
// **为什么需要归一（2026-10-29 修）**：一个「挂死但 ctx-aware」的 check 在
// 总超时到点时，其 ctx 会被 cancel，它返回的 err 就是 context.DeadlineExceeded
// （对外文案 "context deadline exceeded"）。而 collect 侧的定时器也在**同一
// 时刻**到期——于是「谁先被 select 选中」决定了这同一次超时被记成
// "check timeout after 50ms" 还是 "context deadline exceeded"。
//
// 后果是同一份代码在不同机器/负载下产出**不同的 body**：CI 上偶发红
// （bootstrap#48：body = {"slow":"context deadline exceeded"}，
// 而测试要求 "check timeout"），本地却恒绿。文档承诺的是「超时的 check
// 统一记 check timeout」——那就必须有唯一判据，不能让两个定时器赛跑。
//
// **判据不能是「error 长什么样」**：check 内部自己的依赖超时（DB 层抛的
// context.DeadlineExceeded）与本次总超时是两回事，前者该原样上报。
// 因此判据取「**是 cause 里的那个总超时标记**」——只有 runCtx 被
// errTotalTimeout 掐断时，DeadlineExceeded 才归一为总超时文案。
func failureText(err error, runCtx context.Context, timeout time.Duration) string {
	if errors.Is(err, context.DeadlineExceeded) && errors.Is(context.Cause(runCtx), errTotalTimeout) {
		return fmt.Sprintf("check timeout after %s", timeout)
	}
	return err.Error()
}

// safeRun 调 check 捕获 panic(健康检查不应让中间件崩溃)。
func safeRun(ctx context.Context, check Check) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("check panic: %v", r)
		}
	}()
	return check(ctx)
}
