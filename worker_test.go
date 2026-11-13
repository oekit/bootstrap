package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestWorkerManager_TickerPanicDoesNotKillLoop 回归：ticker 回调裸调时
// 一次 panic 就永久打死该周期任务（GoSafe 的 recover 在 goroutine 出口）。
// 钉住：panic 后后续 tick 继续执行。
func TestWorkerManager_TickerPanicDoesNotKillLoop(t *testing.T) {
	wm := NewWorkerManager(context.Background())
	defer wm.Close()

	var runs atomic.Int32
	wm.AddTicker("panicky", 10*time.Millisecond, func(ctx context.Context) {
		if runs.Add(1) == 1 {
			panic("first tick boom")
		}
	})

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && runs.Load() < 2 {
		time.Sleep(10 * time.Millisecond)
	}
	if got := runs.Load(); got < 2 {
		t.Fatalf("ticker died after first panic: runs=%d", got)
	}
}

// TestWorkerManager_AddTickerRejectsNonPositiveInterval 钉住 #101：interval
// 非正必须在注册入口被拒绝。time.NewTimer/NewTicker 对非正 interval panic，
// 且 panic 位置在内层 recover（只包住 fn）之外、被 GoSafe 出口 recover 兜住，
// 旧实现表现为：wg 计数平衡、调用方无报错、任务从未运行且无日志。
func TestWorkerManager_AddTickerRejectsNonPositiveInterval(t *testing.T) {
	wm := NewWorkerManager(context.Background())
	defer wm.Close()

	for _, d := range []time.Duration{0, -time.Second} {
		err := wm.AddTicker("bad", d, func(context.Context) {})
		if err == nil {
			t.Fatalf("interval=%v 必须被拒绝注册（#101）", d)
		}
		if !errors.Is(err, ErrInvalidInterval) {
			t.Fatalf("interval=%v 应返回 ErrInvalidInterval, got %v", d, err)
		}
	}
}

// TestWorkerManager_AddAfterCloseReturnsError 钉住 #103：Close 后 AddTicker/
// AddOnce 的注册从「静默吞掉（无返回值、无日志）」改为显式拒绝——返回
// ErrManagerClosed 并打 Warn 留痕。组件 Init 恰好在关停竞态窗口注册时，
// 不再是「任务永不执行且无任何线索」。
func TestWorkerManager_AddAfterCloseReturnsError(t *testing.T) {
	wm := NewWorkerManager(context.Background())
	wm.Close()

	var ran atomic.Bool
	if err := wm.AddTicker("late", time.Second, func(context.Context) { ran.Store(true) }); !errors.Is(err, ErrManagerClosed) {
		t.Fatalf("AddTicker after Close = %v, want ErrManagerClosed（#103）", err)
	}
	if err := wm.AddOnce("late-once", func(context.Context) { ran.Store(true) }); !errors.Is(err, ErrManagerClosed) {
		t.Fatalf("AddOnce after Close = %v, want ErrManagerClosed（#103）", err)
	}

	time.Sleep(20 * time.Millisecond)
	if ran.Load() {
		t.Fatal("Close 后注册的任务不应执行（#103）")
	}
}

// TestWorkerManager_SlowTaskNoBackToBack 钉住 #98 缺口 1：fn 耗时超过
// interval 时不得背靠背补跑。旧实现 ticker 通道缓冲 1，fn 阻塞期间堆积
// 恰好一枚滴答，fn 返回后 select 立即选中它——慢任务周期性背靠背空转
// （24h 间隔的 gc 类任务跑超时后马上又来一轮）。完成驱动节拍（fn 返回后
// 重新起表）后，下一轮距上次完成至少一个 interval。
func TestWorkerManager_SlowTaskNoBackToBack(t *testing.T) {
	wm := NewWorkerManager(context.Background())
	defer wm.Close()

	const interval = 80 * time.Millisecond
	started := make(chan struct{}, 4)
	fnReturned := make(chan struct{}, 4)
	var runs atomic.Int32
	release := make(chan struct{})
	if err := wm.AddTicker("slow", interval, func(context.Context) {
		runs.Add(1)
		started <- struct{}{}
		<-release // 把 fn 挂起远超 interval，期间堆积恰好 1 枚滴答
		fnReturned <- struct{}{}
	}); err != nil {
		t.Fatalf("AddTicker: %v", err)
	}

	<-started // 第一轮开始
	time.Sleep(3 * interval)
	close(release) // 放行全部轮次
	<-fnReturned   // 第一轮返回（旧实现此刻会立即背靠背补跑堆积滴答）

	// 完成驱动：fn 返回后半个 interval 内不得开始新的一轮。
	// 旧实现的堆积滴答在 fn 返回后 µs 级被 select 选中，判别窗口充足。
	time.Sleep(interval / 2)
	if got := runs.Load(); got != 1 {
		t.Fatalf("fn 返回后半个 interval 内出现背靠背执行: runs=%d, want 1（#98）", got)
	}

	// 循环仍活着：下一轮应在 fn 返回 + interval 附近出现。
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && runs.Load() < 2 {
		time.Sleep(5 * time.Millisecond)
	}
	if got := runs.Load(); got < 2 {
		t.Fatal("完成驱动节拍下后续 tick 未再执行——周期任务死了（#98）")
	}
}

// TestWorkerManager_CloseTimeout 钉住 #98 缺口 2：fn 不感知 ctx 时 Close 死等
// 无超时、停机序列永久挂死。CloseTimeout(d) 超时后放弃等待并 ERROR 留痕。
func TestWorkerManager_CloseTimeout(t *testing.T) {
	wm := NewWorkerManager(context.Background())

	release := make(chan struct{})
	defer close(release) // 收尾放行被放弃的任务，避免跨用例泄漏
	if err := wm.AddOnce("ignores-ctx", func(context.Context) {
		<-release // 模拟不感知 ctx 的网络 IO / LLM 调用
	}); err != nil {
		t.Fatalf("AddOnce: %v", err)
	}

	start := time.Now()
	wm.CloseTimeout(50 * time.Millisecond)
	elapsed := time.Since(start)

	if elapsed < 40*time.Millisecond {
		t.Fatalf("CloseTimeout 未等满预算即返回: elapsed=%v（#98）", elapsed)
	}
	if elapsed > 3*time.Second {
		t.Fatalf("CloseTimeout 仍在死等: elapsed=%v（#98）", elapsed)
	}
}

// TestWorkerManager_CloseTimeout_NonPositiveWaitsLikeClose 钉住
// CloseTimeout 的 d<=0 分支：等同 Close，等待所有任务退出——由 ctx-aware
// 任务在 cancel 后退出验证（Close 本身的无限等待语义不变，向后兼容）。
func TestWorkerManager_CloseTimeout_NonPositiveWaitsLikeClose(t *testing.T) {
	wm := NewWorkerManager(context.Background())
	if err := wm.AddTicker("ctx-aware", time.Hour, func(ctx context.Context) {
		<-ctx.Done()
	}); err != nil {
		t.Fatalf("AddTicker: %v", err)
	}

	start := time.Now()
	wm.CloseTimeout(0) // d<=0 → 无限等待路径
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("d<=0 应等同 Close（等 ctx-aware 任务退出）: elapsed=%v", elapsed)
	}
}

// TestWorkerManager_Wait_ClosesRegistrationGate 回归 #183：Wait 与注册并发
// 时不得触碰 sync.WaitGroup「计数为 0 且 Wait 进行中发起正 delta Add」的
// 未定义行为。旧实现（裸 wg.Wait()，不关闸）在本测试下确定性红：Wait 进入
// 等待后 AddOnce 始终成功（2s 内无一次被拒）；新实现 Wait = markClosed +
// wg.Wait，闸关闭后注册被拒绝（ErrManagerClosed）。
func TestWorkerManager_Wait_ClosesRegistrationGate(t *testing.T) {
	wm := NewWorkerManager(context.Background())

	// 先注册一个阻塞任务（计数 ≥1），随后在另一 goroutine 里 Wait——Wait
	// 真正进入等待状态（而非计数为 0 的即时返回路径），与真实缺陷场景一致。
	release := make(chan struct{})
	if err := wm.AddOnce("blocker", func(_ context.Context) { <-release }); err != nil {
		t.Fatalf("AddOnce: %v", err)
	}
	waitDone := make(chan struct{})
	go func() {
		wm.Wait()
		close(waitDone)
	}()

	// 轮询注册直到被拒（给 Wait 的 markClosed 留出调度窗口）：新实现下
	// 首次成功后的几次尝试内必被拒；旧实现下 2s 内一次都不会被拒。
	deadline := time.Now().Add(2 * time.Second)
	rejected := false
	for {
		if err := wm.AddOnce("late", func(_ context.Context) {}); err != nil {
			rejected = true
			break
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if !rejected {
		t.Fatal("Wait() 并发期间的注册始终未被拒绝——旧实现旁路（#183）：Wait 进行中发起正 delta Add，违反 WaitGroup 铁律")
	}

	// 闸关了，存量任务仍要等完：放开阻塞任务，Wait 必须随之返回。
	close(release)
	select {
	case <-waitDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Wait 未随存量任务退出而返回")
	}
}

// TestWorkerManager_Wait_ConcurrentWithAdd_RaceSmoke #183 的 -race 冒烟：
// 多 goroutine 并发 Wait 与 AddOnce——新实现下注册闸（mu 保护）把 Add 与
// Wait 的相遇全序化（先 markClosed 后进入 Wait，Add 要么在闸关前完成、
// 要么被拒），无 WaitGroup 正增量违规、无数据竞争、不漏等。配合 -race 运行
// （make ci / GOWORK=off go test -race）。注意本测试不区分新旧实现的终止性
// （旧实现下同样会终止但带 UB），确定性红由
// TestWorkerManager_Wait_ClosesRegistrationGate 承担。
func TestWorkerManager_Wait_ConcurrentWithAdd_RaceSmoke(t *testing.T) {
	wm := NewWorkerManager(context.Background())

	const adders = 8
	const attempts = 50
	var join sync.WaitGroup
	for i := 0; i < adders; i++ {
		join.Go(func() { // Go 1.26 地板：sync.WaitGroup.Go
			for j := 0; j < attempts; j++ {
				_ = wm.AddOnce(fmt.Sprintf("t-%d-%d", i, j), func(_ context.Context) {})
			}
		})
	}
	for i := 0; i < 4; i++ {
		join.Go(func() { wm.Wait() })
	}

	done := make(chan struct{})
	go func() {
		join.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("并发 Wait/Add 未在时限内收敛：疑似 WaitGroup 违规死锁（#183）")
	}
}

// TestWorkerManager_Wait_NeverReturnsWithTicker 钉住 #210（锚点测试）：
// 注册过 AddTicker 后 Wait 永不返回——ticker goroutine 唯一退出路径是
// ctx 取消，而 Wait 不取消 context（此前 godoc 宣称「等待所有任务退出」，
// 对周期任务不成立，且无任何警告）。修的是 godoc 如实点破 + WaitTimeout
// 出口，Wait 本身语义不变（仅 AddOnce 有限任务时保证返回，#183 用例钉住）。
// 收尾用 Close 取消 ctx：挂起的 Wait 随 ticker 退出、wg 清零而返回。
func TestWorkerManager_Wait_NeverReturnsWithTicker(t *testing.T) {
	wm := NewWorkerManager(context.Background())

	if err := wm.AddTicker("periodic", 20*time.Millisecond, func(context.Context) {}); err != nil {
		t.Fatalf("AddTicker: %v", err)
	}

	waitDone := make(chan struct{})
	go func() {
		wm.Wait()
		close(waitDone)
	}()

	select {
	case <-waitDone:
		t.Fatal("注册过 AddTicker 后 Wait 不应返回（ticker goroutine 只随 ctx 取消退出，Wait 不取消 context——#210）")
	case <-time.After(300 * time.Millisecond):
		// 期望路径：300ms 窗口内 Wait 仍挂起。
	}

	// Close 取消 ctx ⇒ ticker 退出 ⇒ wg 清零 ⇒ 挂起的 Wait 随之返回。
	wm.Close()
	select {
	case <-waitDone:
	case <-time.After(3 * time.Second):
		t.Fatal("Close 取消 ctx 后挂起的 Wait 未随任务退出而返回（#210）")
	}
}

// TestWorkerManager_WaitTimeout_TimesOutWithTicker 钉住 #210 的超时面：
// 注册过 AddTicker 且不取消时，WaitTimeout(d) 必须在预算到点返回 false
// （不再有「等存量自然结束」的无限死等裸暴露）；CloseTimeout 的 d<=0 兜底
// 对 WaitTimeout 不需要——d<=0 等同 Wait，语义见 WaitTimeout godoc。
func TestWorkerManager_WaitTimeout_TimesOutWithTicker(t *testing.T) {
	wm := NewWorkerManager(context.Background())
	defer wm.Close() // 超时放弃的只是等待者；任务由 Close 收尾取消

	if err := wm.AddTicker("periodic", 20*time.Millisecond, func(context.Context) {}); err != nil {
		t.Fatalf("AddTicker: %v", err)
	}

	start := time.Now()
	if wm.WaitTimeout(80 * time.Millisecond) {
		t.Fatal("注册过 AddTicker 且未取消 ctx 时 WaitTimeout 应返回 false（#210）")
	}
	elapsed := time.Since(start)
	if elapsed < 60*time.Millisecond {
		t.Fatalf("WaitTimeout 未等满预算即返回: elapsed=%v（#210）", elapsed)
	}
	if elapsed > 3*time.Second {
		t.Fatalf("WaitTimeout 仍在死等: elapsed=%v（#210）", elapsed)
	}
}

// TestWorkerManager_WaitTimeout_DrainsFiniteTasks 钉住 #210 的 drain 面：
// 有限任务（AddOnce）退出后 WaitTimeout 返回 true；d<=0 走等同 Wait 的
// 无限等待路径（waitWithTimeout 退化为裸 wg.Wait）。
func TestWorkerManager_WaitTimeout_DrainsFiniteTasks(t *testing.T) {
	wm := NewWorkerManager(context.Background())
	defer wm.Close()

	// 正预算：任务退出（wg 清零）后返回 true。
	release := make(chan struct{})
	if err := wm.AddOnce("finite", func(context.Context) { <-release }); err != nil {
		t.Fatalf("AddOnce: %v", err)
	}
	go func() {
		time.Sleep(50 * time.Millisecond)
		close(release)
	}()
	if !wm.WaitTimeout(3 * time.Second) {
		t.Fatal("有限任务退出后 WaitTimeout 应返回 true（drain 成功，#210）")
	}

	// d<=0：等同 Wait 的无限等待路径——计数已清零，立即返回 true。
	if !wm.WaitTimeout(0) {
		t.Fatal("d<=0 应走无限等待路径并在计数清零后立即返回 true（#210）")
	}
}
