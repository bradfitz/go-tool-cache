package gocached

import (
	"context"
	"errors"
	"testing"
	"time"
)

// feedProbes records n probe samples of latency d.
func feedProbes(g *moverGovernor, n int, d time.Duration) {
	for range n {
		g.observe(1<<10, d)
	}
}

func TestMoverGovernorGrowsOnlyWithDemand(t *testing.T) {
	g := newMoverGovernor(8, 256)

	// Unloaded latency with no demand: learns the baseline, holds the limit.
	feedProbes(g, 20, 10*time.Millisecond)
	g.adjust()
	if st := g.stats(); st.limit != 8 || st.baseline != 10*time.Millisecond {
		t.Fatalf("after idle interval: %+v; want limit 8, baseline 10ms", st)
	}

	// Same latency but movers waited for a slot: grow additively.
	g.mu.Lock()
	g.waited = true
	g.mu.Unlock()
	feedProbes(g, 20, 11*time.Millisecond)
	g.adjust()
	if st := g.stats(); st.limit != 10 || st.increases != 1 {
		t.Fatalf("after loaded low-latency interval: %+v; want limit 10", st)
	}

	// Keep growing while latency stays near baseline; doubling takes about
	// moverIncreaseDivisor intervals.
	for range 20 {
		g.mu.Lock()
		g.waited = true
		g.mu.Unlock()
		feedProbes(g, 20, 12*time.Millisecond)
		g.adjust()
	}
	if st := g.stats(); st.limit <= 40 {
		t.Fatalf("after sustained headroom: %+v; want limit well above 40", st)
	}
}

func TestMoverGovernorBacksOffOnLatency(t *testing.T) {
	g := newMoverGovernor(8, 256)
	g.mu.Lock()
	g.limit = 64
	g.mu.Unlock()
	feedProbes(g, 20, 10*time.Millisecond)
	g.adjust() // learn baseline 10ms

	// Median latency at 3x baseline while saturated: multiplicative decrease.
	g.mu.Lock()
	g.waited = true
	g.mu.Unlock()
	feedProbes(g, 20, 30*time.Millisecond)
	g.adjust()
	if st := g.stats(); st.limit != 48 || st.decreases != 1 {
		t.Fatalf("after saturated interval: %+v; want limit 48", st)
	}

	// Saturated intervals must not drag the baseline up: it stays at the
	// unloaded 10ms so continued high latency keeps shrinking the limit.
	if st := g.stats(); st.baseline != 10*time.Millisecond {
		t.Fatalf("baseline moved under load: %+v", st)
	}
	for range 10 {
		g.mu.Lock()
		g.waited = true
		g.mu.Unlock()
		feedProbes(g, 20, 30*time.Millisecond)
		g.adjust()
	}
	if st := g.stats(); st.limit != 8 {
		t.Fatalf("after sustained saturation: %+v; want floor of 8", st)
	}
}

func TestMoverGovernorHoldsInDeadBand(t *testing.T) {
	g := newMoverGovernor(8, 256)
	g.mu.Lock()
	g.limit = 32
	g.mu.Unlock()
	feedProbes(g, 20, 10*time.Millisecond)
	g.adjust()

	// Between the thresholds (1.5x to 2x baseline) the limit holds even
	// under demand.
	g.mu.Lock()
	g.waited = true
	g.mu.Unlock()
	feedProbes(g, 20, 17*time.Millisecond)
	g.adjust()
	if st := g.stats(); st.limit != 32 {
		t.Fatalf("in dead band: %+v; want limit 32", st)
	}

	// Too few probes: hold, and don't learn a baseline from them.
	g2 := newMoverGovernor(8, 256)
	feedProbes(g2, moverMinProbes-1, time.Millisecond)
	g2.adjust()
	if st := g2.stats(); st.limit != 8 || st.baseline != 0 {
		t.Fatalf("with too few probes: %+v; want no change", st)
	}
}

func TestMoverGovernorBaselineDriftsOnlyWhenIdle(t *testing.T) {
	g := newMoverGovernor(8, 256)
	feedProbes(g, 20, 10*time.Millisecond)
	g.adjust()

	// Idle intervals at a higher latency: the baseline drifts up toward
	// the new unloaded latency.
	for range 100 {
		feedProbes(g, 20, 20*time.Millisecond)
		g.adjust()
	}
	if st := g.stats(); st.baseline < 19*time.Millisecond {
		t.Fatalf("baseline didn't drift toward new idle latency: %+v", st)
	}

	// A faster interval snaps the baseline down immediately.
	feedProbes(g, 20, 5*time.Millisecond)
	g.adjust()
	if st := g.stats(); st.baseline != 5*time.Millisecond {
		t.Fatalf("baseline didn't snap down: %+v", st)
	}
}

func TestMoverGovernorIgnoresLargeCopies(t *testing.T) {
	g := newMoverGovernor(8, 256)
	for range 20 {
		g.observe(moverProbeMaxSize+1, time.Second)
	}
	g.adjust()
	if st := g.stats(); st.baseline != 0 {
		t.Fatalf("large copies counted as probes: %+v", st)
	}
}

func TestMoverGovernorAcquireRelease(t *testing.T) {
	g := newMoverGovernor(2, 4)
	ctx := context.Background()
	if err := g.acquire(ctx); err != nil {
		t.Fatal(err)
	}
	if err := g.acquire(ctx); err != nil {
		t.Fatal(err)
	}

	// A third acquire blocks at the limit and records demand.
	done := make(chan error, 1)
	go func() { done <- g.acquire(ctx) }()
	select {
	case err := <-done:
		t.Fatalf("acquire over limit returned early: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	g.release()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("acquire didn't proceed after release")
	}
	g.mu.Lock()
	waited := g.waited
	g.mu.Unlock()
	if !waited {
		t.Fatal("blocked acquire didn't record demand")
	}

	// Raising the limit admits a waiter without a release.
	go func() { done <- g.acquire(ctx) }()
	select {
	case err := <-done:
		t.Fatalf("acquire over limit returned early: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	g.mu.Lock()
	g.limit = 3
	g.cond.Broadcast()
	g.mu.Unlock()
	if err := <-done; err != nil {
		t.Fatal(err)
	}

	// A blocked acquire aborts when its context is canceled.
	cctx, cancel := context.WithCancel(ctx)
	go func() { done <- g.acquire(cctx) }()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled acquire = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("canceled acquire didn't return")
	}
}
