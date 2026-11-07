package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"github.com/oekit/concurrency/async"
)

// ErrManagerClosed 在 WorkerManager 已 Close 后注册任务（AddTicker/AddOnce）
// 时返回（#103）：任务不会执行，注册被显式拒绝并打 Warn 日志留痕——
// 此前是无返回值的静默丢弃，组件 Init 撞上关停竞态窗口时任务永不执行
// 且排障无任何线索。
var ErrManagerClosed = errors.New("worker: manager closed")

// ErrInvalidInterval 在 AddTicker 的 interval <= 0 时返回（#101）：
// 注册被拒绝（fail-fast）。time.NewTimer/NewTicker 对非正 interval panic，
// 且 panic 发生在内层 recover（只包住 fn）之外，被 GoSafe 出口 recover
// 兜住后表现为：wg 计数平衡、调用方无报错、任务从未运行且无日志。
var ErrInvalidInterval = errors.New("worker: ticker interval must be > 0")

// WorkerManager 管理一组周期性后台任务。
//
// 用法：
//
//	wm := bootstrap.NewWorkerManager(ctx)
//	wm.AddTicker("timeout-checker", 60*time.Second, func(ctx context.Context) {
//	    buildSvc.CheckBuildTimeout(ctx)
//	    deploySvc.CheckDeployTimeout(ctx)
//	})
//	wm.AddTicker("metrics-reporter", 30*time.Second, func(ctx context.Context) {
//	    common.ReportQueueMetrics(ctx)
//	})
//	// ctx 取消时所有 worker 自动退出
type WorkerManager struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	mu     sync.Mutex
	closed bool
}

// NewWorkerManager 创建后台任务管理器。
// ctx 取消或 Close() 调用时所有任务自动退出。
func NewWorkerManager(ctx context.Context) *WorkerManager {
	ctx, cancel := context.WithCancel(ctx)
	return &WorkerManager{ctx: ctx, cancel: cancel}
}

// AddTicker 注册一个周期性任务，按 interval 间隔执行 fn。
// ctx 取消时自动退出。返回 error（#101/#103）：注册被拒时可感知，库侧同步打日志留痕。
//
// 语义：
//   - interval 必须为正：非正值直接拒绝注册并返回 ErrInvalidInterval（#101）。
//   - 间隔是**完成驱动**的（#98）：下一轮在上一轮 fn 返回后再等 interval。
//     fn 耗时超过 interval 时堆积的滴答被丢弃，不会背靠背补跑（旧实现的
//     ticker 通道缓冲 1，fn 返回时压着一枚堆积滴答，会立即再执行一轮——
//     24h 间隔的 gc 类慢任务撞上一次超时运行，结束后立刻又跑一轮）。
//   - 已 Close 后注册被拒绝并返回 ErrManagerClosed（#103）。
//   - fn panic 被就地收编为 ERROR 日志，后续 tick 继续执行。
func (w *WorkerManager) AddTicker(name string, interval time.Duration, fn func(ctx context.Context)) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		logger.Warn("worker manager closed, ticker registration dropped", "task", name)
		return fmt.Errorf("%w: ticker %q registration dropped", ErrManagerClosed, name)
	}
	if interval <= 0 {
		logger.Error("worker ticker registration rejected: interval must be > 0",
			"task", name, "interval", interval.String())
		return fmt.Errorf("%w: ticker %q got %s", ErrInvalidInterval, name, interval)
	}

	w.wg.Add(1)
	async.GoSafe(func() {
		defer w.wg.Done()
		timer := time.NewTimer(interval)
		defer timer.Stop()
		for {
			select {
			case <-w.ctx.Done():
				return
			case <-timer.C:
				// panic 就地收编：GoSafe 的 recover 在 goroutine 出口，
				// 裸调 fn 一次 panic 就永久打死这个周期任务。收编必须留痕，
				// 否则任务每轮失败却无任何可观测信号。
				func() {
					defer func() {
						if r := recover(); r != nil {
							logger.Error("background task panicked",
								"task", name,
								"panic", r,
								"stack", string(debug.Stack()))
						}
					}()
					fn(w.ctx)
				}()
				// 完成驱动节拍（#98）：fn 返回后重新起表，把「间隔」语义
				// 从「滴答节拍」改为「上次完成至今」。此时 timer 已触发且
				// 通道已排空，Reset 语义安全；慢任务堆积的滴答直接丢弃。
				timer.Reset(interval)
			}
		}
	})
	return nil
}

// AddOnce 注册一个一次性任务，在后台执行。
// 已 Close 后注册被拒绝并返回 ErrManagerClosed、打 Warn 日志（#103）——
// 不再静默吞掉。
func (w *WorkerManager) AddOnce(name string, fn func(ctx context.Context)) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		logger.Warn("worker manager closed, once task registration dropped", "task", name)
		return fmt.Errorf("%w: once task %q registration dropped", ErrManagerClosed, name)
	}

	w.wg.Add(1)
	async.GoSafe(func() {
		defer w.wg.Done()
		fn(w.ctx)
	})
	return nil
}

// Close 取消 context、阻止新任务注册并**无限期**等待所有已启动的任务退出。
// 多次调用是安全的。fn 不感知 ctx（内嵌网络 IO/LLM 调用未传 ctx）时本方法
// 会永久阻塞——需要停机兜底请改用 CloseTimeout（#98）。
func (w *WorkerManager) Close() {
	w.markClosed()
	w.cancel()
	w.wg.Wait()
}

// CloseTimeout 是带超时的 Close（#98）：最多等待 d，超时后放弃等待仍在
// 运行的任务、打 ERROR 日志留痕并返回——停机序列不再被不感知 ctx 的 fn
// 无限期挂死（调用方此前没有任何超时兜底可加，wg.Wait 裸暴露）。
// 被放弃的任务随进程退出回收；内部 wg.Wait 的 goroutine 有意泄漏
// （sync.WaitGroup 的等待无法取消），与 App.timedWait 同一权衡。
// d <= 0 等同 Close（无限等待）。多次调用是安全的（与 Close 混用亦然）。
func (w *WorkerManager) CloseTimeout(d time.Duration) {
	w.markClosed()
	w.cancel()
	if d <= 0 {
		w.wg.Wait()
		return
	}

	done := make(chan struct{})
	async.GoSafe(func() {
		w.wg.Wait()
		close(done)
	})

	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
		logger.Error("worker manager close timeout, abandoning still-running tasks",
			"timeout", d.String())
	}
}

// markClosed 置位 closed，阻止后续注册（与 AddTicker/AddOnce 的检查互斥）。
func (w *WorkerManager) markClosed() {
	w.mu.Lock()
	w.closed = true
	w.mu.Unlock()
}

// Wait 等待所有任务退出。
func (w *WorkerManager) Wait() {
	w.wg.Wait()
}
