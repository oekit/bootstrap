package nacos

import (
	"testing"
	"time"

	"github.com/oekit/bootstrap"
)

// TestWithReadyTimeout_ZeroNormalizesToDefault 钉住 #203：WithReadyTimeout(0)
// 此前静默关闭看门狗——godoc 只声明负值为关闭哨兵，0 未定义而实现把 0 当关闭：
// 配置透传 WithReadyTimeout(cfg.GetDuration("ready_timeout")) 在配置未填时
// 把 #33 补的「服务不会注册到注册中心」ERROR 可观测性无声掐掉（零信号）。
// 修复后 0 = 未配置回填默认 30s；负值仍是唯一的关闭哨兵。
func TestWithReadyTimeout_ZeroNormalizesToDefault(t *testing.T) {
	orig := readyTimeoutNanos.Load()
	t.Cleanup(func() { readyTimeoutNanos.Store(orig) })

	t.Run("zero 回填默认 30s", func(t *testing.T) {
		readyTimeoutNanos.Store(int64(50 * time.Millisecond)) // 任意旧值，证明 0 真的被归一
		WithReadyTimeout(0)(&bootstrap.Bootstrapper{})
		got := time.Duration(readyTimeoutNanos.Load())
		if got != defaultReadyTimeout {
			t.Fatalf("WithReadyTimeout(0) 应回填默认 %v（0 = 未配置），got %v——配置零值透传会静默关掉看门狗（#203）",
				defaultReadyTimeout, got)
		}
	})

	t.Run("负值仍是关闭哨兵", func(t *testing.T) {
		WithReadyTimeout(-time.Second)(&bootstrap.Bootstrapper{})
		got := time.Duration(readyTimeoutNanos.Load())
		if got != -time.Second {
			t.Fatalf("负值必须原样存入（关闭看门狗），got %v", got)
		}
	})

	t.Run("显式非零保持", func(t *testing.T) {
		WithReadyTimeout(1234 * time.Millisecond)(&bootstrap.Bootstrapper{})
		got := time.Duration(readyTimeoutNanos.Load())
		if got != 1234*time.Millisecond {
			t.Fatalf("显式非零必须原样存入, got %v", got)
		}
	})

	t.Run("后写覆盖先写仍成立", func(t *testing.T) {
		WithReadyTimeout(2 * time.Second)(&bootstrap.Bootstrapper{})
		WithReadyTimeout(0)(&bootstrap.Bootstrapper{}) // 0 归一为 30s，仍是「后写」
		got := time.Duration(readyTimeoutNanos.Load())
		if got != defaultReadyTimeout {
			t.Fatalf("0 归一后的取值应参与后写覆盖（= %v）, got %v", defaultReadyTimeout, got)
		}
	})
}
