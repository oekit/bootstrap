package bootstrap

import (
	"context"
	"runtime/debug"
	"sync"
	"time"

	"github.com/oekit/concurrency/async"
)

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
// ctx 取消时自动退出。
func (w *WorkerManager) AddTicker(name string, interval time.Duration, fn func(ctx context.Context)) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return
	}

	w.wg.Add(1)
	async.GoSafe(func() {
		defer w.wg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-w.ctx.Done():
				return
			case <-ticker.C:
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
			}
		}
	})
}

// AddOnce 注册一个一次性任务，在后台执行。
func (w *WorkerManager) AddOnce(name string, fn func(ctx context.Context)) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return
	}

	w.wg.Add(1)
	async.GoSafe(func() {
		defer w.wg.Done()
		fn(w.ctx)
	})
}

// Close 取消 context、阻止新任务注册并等待所有已启动的任务退出。
// 多次调用是安全的。
func (w *WorkerManager) Close() {
	w.mu.Lock()
	w.closed = true
	w.mu.Unlock()
	w.cancel() // 取消 context 使 ticker 任务退出
	w.wg.Wait()
}

// Wait 等待所有任务退出。
func (w *WorkerManager) Wait() {
	w.wg.Wait()
}
