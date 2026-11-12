package bootstrap

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestNoHardcodedErrKeyLiterals 钉住 #188：本仓非测试 Go 代码的日志错误
// 字段名必须用 logx.ErrKey 常量（值 "err"），禁止手写 "err" 字面量——
// 两矩阵落同一 Loki/ES，字段名漂移会拆散检索。CI grep 门禁的同款语义
// 固化为测试护栏：扫描仓内全部非 _test.go 的 .go 文件（含嵌套 module
// nacos/obs），行内出现 `"err",` 或 `"err")` 即失败（注释行豁免）。
func TestNoHardcodedErrKeyLiterals(t *testing.T) {
	visit := func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			name := d.Name()
			if name != "." && (strings.HasPrefix(name, ".") || name == "vendor" || name == "testdata") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for i, line := range strings.Split(string(data), "\n") {
			trimmed := strings.TrimSpace(line)
			if strings.HasPrefix(trimmed, "//") {
				continue
			}
			if strings.Contains(line, `"err",`) || strings.Contains(line, `"err")`) {
				t.Errorf("%s:%d 手写 \"err\" 字面量违反 AGENTS.md（错误字段名统一用 logx.ErrKey，#188）",
					path, i+1)
			}
		}
		return nil
	}
	if err := filepath.WalkDir(".", visit); err != nil {
		t.Fatalf("walk repo: %v", err)
	}
}
