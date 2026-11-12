package nacos

import (
	"os"
	"strings"
	"testing"
)

// withReadyFatalDoc 截取 nns.go 中 WithReadyFatal 的 godoc 块（自上一个
// 非注释顶层行到 func 声明之间的连续注释）。
func withReadyFatalDoc(t *testing.T, src string) string {
	t.Helper()
	idx := strings.Index(src, "func WithReadyFatal()")
	if idx < 0 {
		t.Fatal("WithReadyFatal not found in nns.go")
	}
	lines := strings.Split(src[:idx], "\n")
	start := 0
	for i := len(lines) - 1; i >= 0; i-- {
		s := strings.TrimSpace(lines[i])
		if s != "" && !strings.HasPrefix(s, "//") {
			start = i + 1
			break
		}
	}
	return strings.Join(lines[start:], "\n")
}

// TestWithReadyFatalGodoc_AuthoritativeScope 钉住 #185：WithReadyFatal 的
// godoc 不得宣称「须在 WithNNS/WithRequiredNNS 之前应用」——实现是包级
// atomic（选项应用全在 bootstrap.New 阶段、消费在注册 goroutine 运行期），
// 顺序无关、后写覆盖先写，与姊妹选项 WithReadyTimeout 已纠正的口径一致；
// 此前两段相邻文档自相矛盾。权威表述必须写明进程级全局作用域与
// per-Bootstrapper 替代（NNSComponent.ReadyFatal）。
func TestWithReadyFatalGodoc_AuthoritativeScope(t *testing.T) {
	data, err := os.ReadFile("nns.go")
	if err != nil {
		t.Fatalf("read nns.go: %v", err)
	}
	doc := withReadyFatalDoc(t, string(data))
	if strings.Contains(doc, "须在 WithNNS/WithRequiredNNS 之前应用") {
		t.Error("WithReadyFatal godoc 仍宣称顺序依赖（#185）：实现为包级 atomic，顺序无关")
	}
	if !strings.Contains(doc, "进程级全局") {
		t.Error("WithReadyFatal godoc 应写明进程级全局作用域（#185，与 WithReadyTimeout 同款权威表述）")
	}
	if !strings.Contains(doc, "NNSComponent.ReadyFatal") {
		t.Error("WithReadyFatal godoc 应指路 per-Bootstrapper 作用域的替代：组件字段 NNSComponent.ReadyFatal（#185）")
	}
}
