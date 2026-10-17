package bootstrap

import (
	"errors"
	"testing"
)

func TestWithBeforeServe_RunsBeforeUserFn(t *testing.T) {
	var order []string
	b, err := New(
		WithServiceName("svc"),
		WithBeforeServe(func() error { order = append(order, "a"); return nil }),
		WithBeforeServe(func() error { order = append(order, "b"); return nil }),
	)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = b.Run(func(app *App) error {
			order = append(order, "user")
			return nil
		})
	}()
	<-done
	if len(order) != 3 || order[0] != "a" || order[1] != "b" || order[2] != "user" {
		t.Fatalf("order=%v", order)
	}
}

func TestWithBeforeServe_ErrorAborts(t *testing.T) {
	userRan := false
	b, _ := New(
		WithServiceName("svc"),
		WithBeforeServe(func() error { return errors.New("boom") }),
	)
	err := b.Run(func(app *App) error {
		userRan = true
		return nil
	})
	if err == nil {
		t.Fatal("want error")
	}
	if userRan {
		t.Fatal("userFn must not run after beforeServe failure")
	}
}
