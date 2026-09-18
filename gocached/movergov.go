package gocached

import (
	"context"
	"slices"
	"sync"
	"time"
)

// Defaults and tuning parameters for [moverGovernor].
const (
	// defaultMinPutMovers and defaultMaxPutMovers bound the adaptive number
	// of concurrent spooled-blob copies into the main blob directory. The
	// minimum is the floor the governor never backs off below (and the
	// starting point); the maximum bounds goroutines, open files, and the
	// concurrency inflicted on the main directory's filesystem.
	defaultMinPutMovers = 8
	defaultMaxPutMovers = 256

	// moverProbeMaxSize is the largest stored blob size whose copy latency
	// counts as a probe of the main directory's round-trip latency. Below
	// this the transfer time is negligible next to the fixed per-copy
	// round trips (intent create, temp create, write, rename), so the
	// latency reflects how loaded the filesystem is rather than how big
	// the blob was. In practice most spooled blobs are this small.
	moverProbeMaxSize = 64 << 10

	// moverAdjustInterval is how often the governor re-evaluates the limit.
	moverAdjustInterval = 2 * time.Second

	// moverMinProbes is the fewest probe samples an interval needs before
	// the governor acts on it; with fewer it holds the current limit.
	moverMinProbes = 5

	// moverIncreaseBelow and moverDecreaseAbove are the ratios of the
	// interval's median probe latency to the unloaded baseline that
	// trigger growth (when there is also demand) and shrinkage. Between
	// them the limit holds.
	moverIncreaseBelow = 1.5
	moverDecreaseAbove = 2.0

	// moverIncreaseDivisor sets additive growth: the limit grows by
	// limit/moverIncreaseDivisor (at least 1) per interval, so it doubles
	// in about moverIncreaseDivisor intervals under sustained headroom.
	moverIncreaseDivisor = 4

	// moverDecreaseNumer/Denom set multiplicative backoff: the limit is
	// scaled by moverDecreaseNumer/moverDecreaseDenom when latency shows
	// the filesystem is saturating.
	moverDecreaseNumer = 3
	moverDecreaseDenom = 4

	// moverBaselineDrift is how much of the gap between the baseline and an
	// unloaded interval's median the baseline absorbs per interval. It lets
	// the baseline re-learn a slower filesystem, but only from intervals
	// with no demand (no mover waited for a slot), so sustained saturation
	// can't teach the governor that loaded latency is normal.
	moverBaselineDrift = 0.05
)

// moverGovernor adaptively sets how many spooled-blob copies into the main
// blob directory may run concurrently, using an AIMD control loop driven by
// the latency of small copies.
//
// The main directory may be a network filesystem whose throughput for the
// small, round-trip-bound copies that dominate the put queue scales with
// concurrency until the filesystem saturates. Rather than guess a fixed
// mover count, the governor grows the limit while small-copy latency stays
// near its unloaded baseline and there is demand (movers waiting for a
// slot), and backs off multiplicatively when latency climbs well above the
// baseline. Without demand it holds, since there is nothing to learn from
// an idle filesystem except its baseline latency.
type moverGovernor struct {
	minLimit int
	maxLimit int

	mu     sync.Mutex
	cond   *sync.Cond // signaled when a slot frees or the limit grows
	limit  int        // current maximum concurrent copies
	active int        // copies currently running
	waited bool       // whether any acquire had to wait this interval

	probes   []time.Duration // probe copy latencies observed this interval
	baseline time.Duration   // estimate of unloaded probe latency; 0 until learned
	lastMed  time.Duration   // median probe latency of the last evaluated interval

	increases, decreases int // adjustment counts for metrics
}

func newMoverGovernor(minLimit, maxLimit int) *moverGovernor {
	if minLimit <= 0 {
		minLimit = defaultMinPutMovers
	}
	if maxLimit < minLimit {
		maxLimit = max(minLimit, defaultMaxPutMovers)
	}
	g := &moverGovernor{
		minLimit: minLimit,
		maxLimit: maxLimit,
		limit:    minLimit,
	}
	g.cond = sync.NewCond(&g.mu)
	return g
}

// acquire blocks until a copy slot is available under the current limit or
// ctx is done. Each successful acquire must be paired with a release.
func (g *moverGovernor) acquire(ctx context.Context) error {
	// Wake waiters on shutdown, since cond.Wait can't select on ctx.
	stop := context.AfterFunc(ctx, func() {
		g.mu.Lock()
		g.cond.Broadcast()
		g.mu.Unlock()
	})
	defer stop()

	g.mu.Lock()
	defer g.mu.Unlock()
	for g.active >= g.limit {
		if err := ctx.Err(); err != nil {
			return err
		}
		g.waited = true
		g.cond.Wait()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	g.active++
	return nil
}

// release returns a slot taken by acquire.
func (g *moverGovernor) release() {
	g.mu.Lock()
	g.active--
	g.mu.Unlock()
	g.cond.Signal()
}

// observe records a completed copy of a blob of storedSize bytes that took
// d. Only copies small enough to be latency probes affect the limit.
func (g *moverGovernor) observe(storedSize int64, d time.Duration) {
	if storedSize > moverProbeMaxSize {
		return
	}
	g.mu.Lock()
	g.probes = append(g.probes, d)
	g.mu.Unlock()
}

// adjust evaluates the interval's probe samples and demand and moves the
// limit accordingly. It is called once per moverAdjustInterval by the
// governor loop, and directly by tests.
func (g *moverGovernor) adjust() {
	g.mu.Lock()
	defer g.mu.Unlock()

	probes := g.probes
	g.probes = nil
	waited := g.waited
	g.waited = false

	if len(probes) < moverMinProbes {
		return
	}
	slices.Sort(probes)
	med := probes[len(probes)/2]
	g.lastMed = med

	if g.baseline == 0 || med < g.baseline {
		// First estimate, or the filesystem is faster than we thought:
		// adopt the new unloaded latency immediately.
		g.baseline = med
	} else if !waited {
		// No demand this interval, so the latency was unloaded: let the
		// baseline drift toward it to track a filesystem that slowed down.
		g.baseline += time.Duration(float64(med-g.baseline) * moverBaselineDrift)
	}

	ratio := float64(med) / float64(g.baseline)
	switch {
	case ratio > moverDecreaseAbove:
		newLimit := max(g.minLimit, g.limit*moverDecreaseNumer/moverDecreaseDenom)
		if newLimit < g.limit {
			g.limit = newLimit
			g.decreases++
		}
	case ratio < moverIncreaseBelow && waited:
		newLimit := min(g.maxLimit, g.limit+max(1, g.limit/moverIncreaseDivisor))
		if newLimit > g.limit {
			g.limit = newLimit
			g.increases++
			g.cond.Broadcast()
		}
	}
}

// run calls adjust every moverAdjustInterval until ctx is done.
func (g *moverGovernor) run(ctx context.Context) {
	t := time.NewTicker(moverAdjustInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			g.adjust()
		}
	}
}

// moverStats is a snapshot of the governor's state for metrics.
type moverStats struct {
	limit, active        int
	baseline, lastMedian time.Duration
	increases, decreases int
}

func (g *moverGovernor) stats() moverStats {
	g.mu.Lock()
	defer g.mu.Unlock()
	return moverStats{
		limit:      g.limit,
		active:     g.active,
		baseline:   g.baseline,
		lastMedian: g.lastMed,
		increases:  g.increases,
		decreases:  g.decreases,
	}
}
