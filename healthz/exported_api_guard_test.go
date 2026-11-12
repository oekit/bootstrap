package healthz

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

// TestNoTestOnlyExportedAPIs 钉住 #187：测试专用 API 不得进入生产导出面。
// 此前 ResetForTest 以导出符号暴露（注释自认「仅供 test 使用」），任何业务
// 代码都能调它把全应用 registry（含 MarkClosed 状态）清空，且 global 是包级
// singleton，误用影响是全局的——已改为未导出 resetForTest（本包测试同包
// 调用）。本护栏扫描包生产文件（非 _test.go）的顶层导出符号，禁止
// ForTest 后缀（及 TestOnly/TestHelper 变体）回流导出面。
func TestNoTestOnlyExportedAPIs(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("list package files: %v", err)
	}
	fset := token.NewFileSet()
	for _, e := range entries {
		f := e.Name()
		if !strings.HasSuffix(f, ".go") || strings.HasSuffix(f, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, f, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", f, err)
		}
		for _, decl := range file.Decls {
			var names []string
			switch d := decl.(type) {
			case *ast.FuncDecl:
				if d.Recv != nil {
					continue // 方法不构成包导出面入口，跳过
				}
				names = []string{d.Name.Name}
			case *ast.GenDecl:
				for _, spec := range d.Specs {
					switch s := spec.(type) {
					case *ast.TypeSpec:
						names = append(names, s.Name.Name)
					case *ast.ValueSpec:
						for _, n := range s.Names {
							names = append(names, n.Name)
						}
					}
				}
			}
			for _, n := range names {
				if ast.IsExported(n) && testOnlySuffix(n) {
					t.Errorf("%s: 导出符号 %s 是测试专用 API（ForTest/TestOnly 后缀），不得进入生产面（#187）", f, n)
				}
			}
		}
	}
}

// testOnlySuffix 报告符号名是否带测试专用后缀。
func testOnlySuffix(name string) bool {
	return strings.HasSuffix(name, "ForTest") ||
		strings.HasSuffix(name, "TestOnly") ||
		strings.HasSuffix(name, "TestHelper")
}
