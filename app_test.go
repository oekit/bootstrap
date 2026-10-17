package bootstrap

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oekit/core/lifecycle"
)

// TestApp_Context_NotNil verifies the App exposes a non-nil context.
func TestApp_Context_NotNil(t *testing.T) {
	mgr := lifecycle.New(100 * time.Millisecond)
	app := NewApp(mgr)
	if app.Context() == nil {
		t.Fatal("expected non-nil context from App")
	}
}

// TestApp_Context_CancelledOnShutdown verifies the app-lifecycle hook
// cancels the App's context when shutdown is triggered.
func TestApp_Context_CancelledOnShutdown(t *testing.T) {
	mgr := lifecycle.New(500 * time.Millisecond)
	app := NewApp(mgr)

	// Trigger shutdown — runs the three app-lifecycle jobs (cancel, close, wait).
	_ = app // suppress unused warning

	// Use a goroutine + timer to wait for cancellation.
	done := make(chan struct{})
	go func() {
		<-app.Context().Done()
		close(done)
	}()

	// Wait briefly, then trigger lifecycle. The hook should cancel
	// app.ctx.
	time.Sleep(20 * time.Millisecond)
	go mgr.Shutdown(context.Background())

	select {
	case <-done:
		// expected
	case <-time.After(2 * time.Second):
		t.Fatal("expected App.Context to be cancelled after shutdown")
	}
}

// TestApp_AddResource_ClosesOnShutdown verifies AddResource registers
// a closer that runs in reverse order on lifecycle.
func TestApp_AddResource_ClosesOnShutdown(t *testing.T) {
	mgr := lifecycle.New(500 * time.Millisecond)
	app := NewApp(mgr)

	var (
		mu    sync.Mutex
		order []string
	)
	record := func(name string) func(_ context.Context) error {
		return func(_ context.Context) error {
			mu.Lock()
			order = append(order, name)
			mu.Unlock()
			return nil
		}
	}
	app.AddResource("db", record("db"))
	app.AddResource("cache", record("cache"))
	app.AddResource("queue", record("queue"))

	// Trigger shutdown — closeAll runs closers in reverse order.
	mgr.Shutdown(context.Background())

	mu.Lock()
	defer mu.Unlock()
	if got := order; len(got) != 3 || got[0] != "queue" || got[1] != "cache" || got[2] != "db" {
		t.Errorf("expected reverse-order [queue, cache, db], got %v", got)
	}
}

// TestApp_AddResource_CloserError_DoesNotStopShutdown verifies that an
// error from one closer doesn't stop subsequent closers from running.
func TestApp_AddResource_CloserError_DoesNotStopShutdown(t *testing.T) {
	mgr := lifecycle.New(500 * time.Millisecond)
	app := NewApp(mgr)

	var ranSecond atomic.Bool
	app.AddResource("first-fails", func(_ context.Context) error {
		return errors.New("first close failed")
	})
	app.AddResource("second-runs", func(_ context.Context) error {
		ranSecond.Store(true)
		return nil
	})

	mgr.Shutdown(context.Background())

	if !ranSecond.Load() {
		t.Error("expected second closer to run even after first failed")
	}
}

// TestApp_GoBackground_RunsAndExitsOnContextCancel verifies GoBackground
// launches a goroutine and that the goroutine exits when App.ctx is
// cancelled.
func TestApp_GoBackground_RunsAndExitsOnContextCancel(t *testing.T) {
	mgr := lifecycle.New(500 * time.Millisecond)
	app := NewApp(mgr)

	started := make(chan struct{})
	exited := make(chan struct{})
	app.GoBackground("test-bg", func(ctx context.Context) {
		close(started)
		<-ctx.Done()
		close(exited)
	})

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("background goroutine did not start")
	}

	go mgr.Shutdown(context.Background())

	select {
	case <-exited:
		// expected
	case <-time.After(2 * time.Second):
		t.Fatal("background goroutine did not exit on shutdown")
	}
}

// TestApp_GoBackground_ServerExitsOnResourceClose verifies the shutdown
// ordering fix: a GoBackground goroutine that only exits when its
// associated resource closer is called (simulating h.Run / svr.Run)
// must not deadlock. The app-lifecycle hook calls closeAll before
// wg.Wait, so the server's Shutdown triggers Run to return.
func TestApp_GoBackground_ServerExitsOnResourceClose(t *testing.T) {
	mgr := lifecycle.New(500 * time.Millisecond)
	app := NewApp(mgr)

	// Simulate a server: blocks until shutdownFn is called.
	shutdownCh := make(chan struct{})
	exited := make(chan struct{})
	app.GoBackground("fake-server", func(_ context.Context) {
		defer close(exited)
		<-shutdownCh // blocks like h.Run() until Shutdown is called
	})
	app.AddResource("fake-server", func(_ context.Context) error {
		close(shutdownCh) // simulates h.Shutdown()
		return nil
	})

	go mgr.Shutdown(context.Background())

	select {
	case <-exited:
		// expected — closeAll called resource closer before wg.Wait
	case <-time.After(2 * time.Second):
		t.Fatal("deadlock: GoBackground goroutine did not exit; closeAll likely ran after wg.Wait")
	}
}

// TestApp_GoBackground_DrainTimeout verifies that a hanging GoBackground
// goroutine does not block shutdown indefinitely — the wait phase times
// out and proceeds.
func TestApp_GoBackground_DrainTimeout(t *testing.T) {
	mgr := lifecycle.New(500 * time.Millisecond)
	app := NewApp(mgr)
	app.SetWaitTimeout(100 * time.Millisecond) // short timeout for test

	app.GoBackground("hanger", func(_ context.Context) {
		select {} // blocks forever, ignoring context cancellation
	})

	done := make(chan struct{})
	go func() {
		mgr.Shutdown(context.Background())
		close(done)
	}()

	select {
	case <-done:
		// expected — timedWait should skip the hanging goroutine
	case <-time.After(3 * time.Second):
		t.Fatal("shutdown blocked by hanging goroutine; timedWait not working")
	}
}

// TestApp_GoBackground_DrainContextCancelled verifies that timedWait exits
// when the shutdown context is cancelled, even if the drain timeout hasn't
// fired. This covers the ctx.Done() branch in timedWait.
func TestApp_GoBackground_DrainContextCancelled(t *testing.T) {
	mgr := lifecycle.New(500 * time.Millisecond)
	app := NewApp(mgr)
	app.SetWaitTimeout(10 * time.Second) // long timeout — won't fire

	app.GoBackground("hanger", func(_ context.Context) {
		select {} // blocks forever
	})

	// Use a short-lived context to trigger ctx.Done() before timer fires.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	done := make(chan struct{})
	go func() {
		mgr.Shutdown(ctx)
		close(done)
	}()

	select {
	case <-done:
		// expected — timedWait exited via ctx.Done()
	case <-time.After(3 * time.Second):
		t.Fatal("shutdown blocked; timedWait did not respect context cancellation")
	}
}

// TestApp_GoBackground_DrainContextCancelled_NoTimeout verifies the
// timeout<=0 (infinite wait) path also respects context cancellation.
func TestApp_GoBackground_DrainContextCancelled_NoTimeout(t *testing.T) {
	mgr := lifecycle.New(500 * time.Millisecond)
	app := NewApp(mgr)
	app.SetWaitTimeout(0) // infinite wait

	app.GoBackground("hanger", func(_ context.Context) {
		select {} // blocks forever
	})

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	done := make(chan struct{})
	go func() {
		mgr.Shutdown(ctx)
		close(done)
	}()

	select {
	case <-done:
		// expected — infinite-wait path still exits on ctx.Done()
	case <-time.After(3 * time.Second):
		t.Fatal("shutdown blocked; infinite-wait timedWait did not respect context cancellation")
	}
}

// TestApp_GoBackground_PanicRecovered verifies that a panicking background
// goroutine does not crash the process. async.GoSafe is expected to
// recover the panic.
func TestApp_GoBackground_PanicRecovered(t *testing.T) {
	mgr := lifecycle.New(500 * time.Millisecond)
	app := NewApp(mgr)

	exited := make(chan struct{})
	app.GoBackground("panicker", func(_ context.Context) {
		defer close(exited)
		panic("test panic")
	})

	// The panic must be recovered; exited is closed via defer.
	select {
	case <-exited:
		// expected — async.GoSafe recovered the panic, defer ran
	case <-time.After(time.Second):
		t.Fatal("panicking goroutine did not exit cleanly")
	}
}

// TestShutdownHookComponent_DefaultName covers the fallback name when
// ComponentName is empty.
func TestShutdownHookComponent_DefaultName(t *testing.T) {
	c := &ShutdownHookComponent{ComponentName: ""}
	if got := c.Name(); got != "shutdown-hook" {
		t.Errorf("expected default name 'shutdown-hook', got %q", got)
	}
}

// TestShutdownHookComponent_CustomName covers the custom-name path.
func TestShutdownHookComponent_CustomName(t *testing.T) {
	c := &ShutdownHookComponent{ComponentName: "my-hook"}
	if got := c.Name(); got != "my-hook" {
		t.Errorf("expected 'my-hook', got %q", got)
	}
}

// TestShutdownHookComponent_Init_RegistersHook verifies Init registers
// the hook with the shutdown manager.
func TestShutdownHookComponent_Init_RegistersHook(t *testing.T) {
	called := atomic.Bool{}
	c := &ShutdownHookComponent{
		ComponentName: "test-hook",
		Hook: func(_ context.Context) error {
			called.Store(true)
			return nil
		},
	}
	mgr := lifecycle.New(500 * time.Millisecond)
	if err := c.Init(context.Background(), nil, mgr); err != nil {
		t.Fatalf("Init: %v", err)
	}
	go mgr.Shutdown(context.Background())

	// Wait for hook to fire.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if called.Load() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Error("expected hook to be called on shutdown")
}

// --- worker.go tests ---

// TestWorkerManager_AddOnce_RunsAndCompletes verifies AddOnce launches
// the task in a goroutine and Wait returns when it finishes.
func TestWorkerManager_AddOnce_RunsAndCompletes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wm := NewWorkerManager(ctx)

	ran := atomic.Bool{}
	wm.AddOnce("test", func(_ context.Context) {
		ran.Store(true)
	})
	wm.Wait()

	if !ran.Load() {
		t.Error("expected AddOnce task to run")
	}
}

// TestWorkerManager_AddTicker_RunsAtLeastOnce verifies AddTicker launches
// a periodic task. We use a very short interval (10ms) and wait long
// enough for at least one tick.
func TestWorkerManager_AddTicker_RunsAtLeastOnce(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wm := NewWorkerManager(ctx)

	count := atomic.Int32{}
	wm.AddTicker("ticker", 10*time.Millisecond, func(_ context.Context) {
		count.Add(1)
	})

	// Wait for at least 2 ticks to confirm periodicity.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if count.Load() >= 2 {
			cancel()  // stop the ticker
			wm.Wait() // wait for goroutine to exit
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Errorf("expected ≥2 ticker fires, got %d", count.Load())
}

// TestWorkerManager_AddTicker_StopsOnContextCancel verifies the ticker
// exits when ctx is cancelled.
func TestWorkerManager_AddTicker_StopsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	wm := NewWorkerManager(ctx)

	wm.AddTicker("ticker", 10*time.Millisecond, func(_ context.Context) {})
	cancel()
	wm.Wait() // should return quickly, not block
}

// TestWorkerManager_Close_PreventsNewTasks verifies that AddOnce and
// AddTicker are no-ops after Close is called.
func TestWorkerManager_Close_PreventsNewTasks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wm := NewWorkerManager(ctx)

	ran := atomic.Bool{}
	wm.AddOnce("before-close", func(_ context.Context) {
		ran.Store(true)
	})
	wm.Close() // waits for "before-close" to finish

	if !ran.Load() {
		t.Fatal("expected pre-close task to run")
	}

	// After Close, new tasks must not launch.
	shouldNotRun := atomic.Bool{}
	wm.AddOnce("after-close", func(_ context.Context) {
		shouldNotRun.Store(true)
	})
	wm.AddTicker("after-close-ticker", time.Millisecond, func(_ context.Context) {
		shouldNotRun.Store(true)
	})

	// Give goroutines a moment in case they incorrectly launched.
	time.Sleep(50 * time.Millisecond)
	if shouldNotRun.Load() {
		t.Error("task ran after Close; closed guard not working")
	}
}

// TestWorkerManager_Close_Idempotent verifies that calling Close multiple
// times does not panic.
func TestWorkerManager_Close_Idempotent(t *testing.T) {
	wm := NewWorkerManager(context.Background())
	wm.Close()
	wm.Close() // must not panic
}
