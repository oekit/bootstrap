package healthz

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestCollectResults_ResultReadyAtDeadlineNotLost 钉住 #254：deadline 与结果
// 通道同时就绪时 Go select 按均匀伪随机选一个能推进的分支——超时分支被选中
// 时，结果已在通道里的 check 不得被漏记成 "check timeout"。漏记的后果是
// 已完成且成功的 check 被误杀成 503（readiness 假摘流）、Degraded 被升级成
// 硬失败、同一份代码在不同调度下产出不确定 body（bootstrap#48 家族残留）。
//
// 构造「两侧同时就绪」：deadline 用已关闭的通道（接收恒就绪），ch 预置一条
// 已完成结果、另一 check 永不返回——select 每轮在 ch 与 deadline 间随机二选
// 一，修复前只要有一轮选中 deadline，预置结果即被丢弃并补记 timeout。多轮
// （每轮重新预置）把约 50% 的翻转概率变成必然撞上的回归：单轮绿不等于修对。
func TestCollectResults_ResultReadyAtDeadlineNotLost(t *testing.T) {
	entries := []checkEntry{{name: "done"}, {name: "hang"}}
	fired := make(chan time.Time)
	close(fired) // 已到点：接收恒就绪（等价于 Handle 里到点的 timer.C）

	const rounds = 100
	for i := range rounds {
		ch := make(chan checkResult, len(entries))
		ch <- checkResult{name: "done"} // 已完成的 check，结果已在通道里
		// "hang" 永不返回

		failures, degraded := collectResults(entries, ch, fired, 5*time.Second, context.Background())
		if msg, ok := failures["done"]; ok {
			t.Fatalf("round %d: 结果已在通道里的 check 被记成 timeout（#254）: %s", i, msg)
		}
		if _, ok := failures["hang"]; !ok {
			t.Fatalf("round %d: 从未返回的 check 必须记 timeout, failures=%v", i, failures)
		}
		if len(degraded) != 0 {
			t.Fatalf("round %d: degraded 应为空, got %v", i, degraded)
		}
	}
}

// TestCollectResults_DeadlineGone_RecordRealResults 钉住收集段的另一半：
// 超时退出后的排空只收「已就绪」的结果，排空之后仍未到达的（真超时）照旧
// 记 "check timeout"——排空不得把迟到结果也算成成功，也不得阻塞等待迟到者。
func TestCollectResults_DeadlineGone_RecordRealResults(t *testing.T) {
	entries := []checkEntry{{name: "early"}, {name: "late"}}
	fired := make(chan time.Time)
	close(fired)

	ch := make(chan checkResult, len(entries))
	ch <- checkResult{name: "early", err: errors.New("boom")} // 排空前就绪：记真实结果
	// "late" 的结果永不入队（check goroutine 迟到/挂死）

	failures, _ := collectResults(entries, ch, fired, time.Millisecond, context.Background())
	if msg, ok := failures["early"]; !ok || msg != "boom" {
		t.Fatalf("已就绪的结果必须原样上报（不得被排空丢弃或改判 timeout）, failures=%v", failures)
	}
	if _, ok := failures["late"]; !ok {
		t.Fatalf("排空后仍未就绪的 check 必须记 timeout, failures=%v", failures)
	}
}
