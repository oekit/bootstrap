package bootstrap

import (
	"sync"
	"time"

	"github.com/oekit/concurrency/async"
)

// waitOutcome 描述 waitWithTimeout 的等待结果。
type waitOutcome int

const (
	// waitDrained：wg 计数清零，等待成功。
	waitDrained waitOutcome = iota
	// waitTimedOut：超时到点仍未清零，放弃等待。
	waitTimedOut
	// waitAborted：中止信号源关闭（如 Shutdown ctx 取消），放弃等待。
	waitAborted
)

// waitWithTimeout 是「wg.Wait 带超时放弃」的单源实现（#179）：App.timedWait
// 与 WorkerManager.CloseTimeout 此前各写一份「done channel + GoSafe 里
// wg.Wait + timer + select」骨架。等待 wg 计数清零——d > 0 时最多等 d
// （到点未清零返回 waitTimedOut）；d <= 0 无限等待；abort 非 nil 时其关闭
// 同样中止等待（返回 waitAborted）。放弃路径的日志口径由调用方负责。
//
// 「带超时放弃」的固有代价（权衡注释单源，此前两处各写一份）：内部 wg.Wait
// goroutine 有意泄漏——sync.WaitGroup 的等待无法取消，进程退出时自动回收。
// 可接受的原因：仍在运行的 goroutine 本身就是拒绝退出的，泄漏的只是等待者。
//
// abort 用 <-chan struct{} 而非 context.Context：WorkerManager 没有 ctx
// （传 nil 即「无中止源」——nil channel 在 select 中永不就绪）；
// App.timedWait 传 ctx.Done()。
//
// d <= 0 且 abort == nil 时退化为裸 wg.Wait（纯死等，不产生等待者 goroutine）。
func waitWithTimeout(wg *sync.WaitGroup, abort <-chan struct{}, d time.Duration) waitOutcome {
	if d <= 0 && abort == nil {
		wg.Wait()
		return waitDrained
	}
	done := make(chan struct{})
	async.GoSafe(func() {
		wg.Wait()
		close(done)
	})
	var timeout <-chan time.Time
	if d > 0 {
		timer := time.NewTimer(d)
		defer timer.Stop()
		timeout = timer.C
	}
	select {
	case <-done:
		return waitDrained
	case <-timeout:
		return waitTimedOut
	case <-abort:
		return waitAborted
	}
}
