package bootstrap

import (
	"os"
	"strings"
	"testing"
)

// TestDocNoStaleShutdownPackageRefs 钉住 #184：doc.go 与 README 的示例/叙述
// 必须引用真实存在的包——shutdown.Manager/shutdown.New 在当前模块图中不存在
// （优雅关停自 logx/shutdown 迁入 core/lifecycle），用户照抄包文档/README 拼
// attach 模式示例会在编译期失败。历史迁移叙述（如 README 矩阵表的
// 「自 logx/shutdown 迁入」）不受影响——禁的是「把 shutdown 当可引用包」的
// 用法面（shutdown.Manager / shutdown.New( / core/shutdown）。
func TestDocNoStaleShutdownPackageRefs(t *testing.T) {
	stalePatterns := []string{"shutdown.Manager", "shutdown.New(", "core/shutdown"}
	for _, f := range []string{"doc.go", "README.md"} {
		data, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("read %s: %v", f, err)
		}
		for i, line := range strings.Split(string(data), "\n") {
			for _, pat := range stalePatterns {
				if strings.Contains(line, pat) {
					t.Errorf("%s:%d 引用了已不存在的 shutdown 包（%q）——应改用 core/lifecycle（#184）",
						f, i+1, pat)
				}
			}
		}
	}
}
