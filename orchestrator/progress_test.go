package orchestrator

import (
	"sync"
	"sync/atomic"
	"testing"
)

func absF32(v float32) float32 {
	if v < 0 {
		return -v
	}
	return v
}

// TEST1126: map_progress is deterministic — same inputs always produce same output
func Test1126_map_progress_deterministic(t *testing.T) {
	for i := 0; i < 100; i++ {
		p := float32(i) / 100.0
		a := MapProgress(p, 0.1, 0.8)
		b := MapProgress(p, 0.1, 0.8)
		if a != b {
			t.Fatalf("map_progress must be deterministic for p=%v: %v != %v", p, a, b)
		}
	}
}

// TEST910: map_progress output is monotonic for monotonically increasing input
func Test910_map_progress_monotonic(t *testing.T) {
	prev := MapProgress(0.0, 0.1, 0.7)
	for i := 1; i <= 100; i++ {
		p := float32(i) / 100.0
		curr := MapProgress(p, 0.1, 0.7)
		if curr < prev {
			t.Fatalf("map_progress must be monotonic: p=%v, prev=%v, curr=%v", p, prev, curr)
		}
		prev = curr
	}
}

// TEST911: map_progress output is bounded within [base, base+weight]
func Test911_map_progress_bounded(t *testing.T) {
	var base float32 = 0.15
	var weight float32 = 0.55
	for i := -10; i <= 110; i++ {
		p := float32(i) / 100.0
		result := MapProgress(p, base, weight)
		if result < base || result > base+weight {
			t.Fatalf("map_progress(%v, %v, %v) = %v must be in [%v, %v]",
				p, base, weight, result, base, base+weight)
		}
	}
}

// TEST912: ProgressMapper correctly maps through a CapProgressFn
func Test912_progress_mapper_reports_through_parent(t *testing.T) {
	type report struct {
		p   float32
		msg string
	}
	var mu sync.Mutex
	var reported []report
	parent := CapProgressFn(func(p float32, _capUrn string, msg string) {
		mu.Lock()
		reported = append(reported, report{p, msg})
		mu.Unlock()
	})

	mapper := NewProgressMapper(parent, 0.2, 0.6)
	mapper.Report(0.0, "", "start")
	mapper.Report(0.5, "", "half")
	mapper.Report(1.0, "", "done")

	if len(reported) != 3 {
		t.Fatalf("expected 3 reports, got %d", len(reported))
	}
	if absF32(reported[0].p-0.2) >= 0.001 {
		t.Errorf("0%% maps to base=0.2, got %v", reported[0].p)
	}
	if absF32(reported[1].p-0.5) >= 0.001 {
		t.Errorf("50%% maps to 0.5, got %v", reported[1].p)
	}
	if absF32(reported[2].p-0.8) >= 0.001 {
		t.Errorf("100%% maps to base+weight=0.8, got %v", reported[2].p)
	}
}

// TEST913: ProgressMapper.as_cap_progress_fn produces same mapping
func Test913_progress_mapper_as_cap_progress_fn(t *testing.T) {
	var mu sync.Mutex
	var reported []float32
	parent := CapProgressFn(func(p float32, _capUrn string, _msg string) {
		mu.Lock()
		reported = append(reported, p)
		mu.Unlock()
	})

	mapper := NewProgressMapper(parent, 0.1, 0.3)
	pfn := mapper.AsCapProgressFn()

	pfn(0.0, "", "a")
	pfn(0.5, "", "b")
	pfn(1.0, "", "c")

	if len(reported) != 3 {
		t.Fatalf("expected 3 reports, got %d", len(reported))
	}
	if absF32(reported[0]-0.1) >= 0.001 {
		t.Errorf("expected 0.1, got %v", reported[0])
	}
	if absF32(reported[1]-0.25) >= 0.001 {
		t.Errorf("expected 0.25, got %v", reported[1])
	}
	if absF32(reported[2]-0.4) >= 0.001 {
		t.Errorf("expected 0.4, got %v", reported[2])
	}
}

// TEST914: ProgressMapper.sub_mapper chains correctly
func Test914_progress_mapper_sub_mapper(t *testing.T) {
	var mu sync.Mutex
	var reported []float32
	parent := CapProgressFn(func(p float32, _capUrn string, _msg string) {
		mu.Lock()
		reported = append(reported, p)
		mu.Unlock()
	})

	// Parent maps [0, 1] to [0.2, 0.8] (base=0.2, weight=0.6)
	mapper := NewProgressMapper(parent, 0.2, 0.6)

	// Sub-mapper maps [0, 1] to the second half of parent's range
	// sub_base=0.5, sub_weight=0.5 → [0.2 + 0.5*0.6, 0.2 + (0.5+0.5)*0.6] = [0.5, 0.8]
	sub := mapper.SubMapper(0.5, 0.5)
	sub.Report(0.0, "", "sub_start")
	sub.Report(1.0, "", "sub_end")

	if len(reported) != 2 {
		t.Fatalf("expected 2 reports, got %d", len(reported))
	}
	if absF32(reported[0]-0.5) >= 0.001 {
		t.Errorf("sub 0%% maps to 0.5, got %v", reported[0])
	}
	if absF32(reported[1]-0.8) >= 0.001 {
		t.Errorf("sub 100%% maps to 0.8, got %v", reported[1])
	}
}

// TEST915: Per-group subdivision produces monotonic, bounded progress for N groups
//
// Uses pre-computed boundaries (same pattern as production code) to guarantee
// monotonicity regardless of f32 rounding.
func Test915_per_group_subdivision_monotonic_bounded(t *testing.T) {
	var mu sync.Mutex
	var allProgress []float32
	parent := CapProgressFn(func(p float32, _capUrn string, _msg string) {
		mu.Lock()
		allProgress = append(allProgress, p)
		mu.Unlock()
	})

	nGroups := 5
	boundaries := make([]float32, nGroups+1)
	for i := 0; i <= nGroups; i++ {
		boundaries[i] = float32(i) / float32(nGroups)
	}

	for i := 0; i < nGroups; i++ {
		base := boundaries[i]
		weight := boundaries[i+1] - base
		mapper := NewProgressMapper(parent, base, weight)

		// Each group reports 0%, 50%, 100%
		mapper.Report(0.0, "", "start")
		mapper.Report(0.5, "", "half")
		mapper.Report(1.0, "", "done")
	}

	if len(allProgress) != 15 { // 5 groups * 3 reports
		t.Fatalf("expected 15 reports, got %d", len(allProgress))
	}

	// Verify monotonicity
	for i := 1; i < len(allProgress); i++ {
		if allProgress[i] < allProgress[i-1] {
			t.Fatalf("monotonic violation at index %d: %v < %v", i, allProgress[i], allProgress[i-1])
		}
	}

	// Verify bounded [0.0, 1.0]
	for i, p := range allProgress {
		if p < 0.0 || p > 1.0 {
			t.Fatalf("Progress[%d]=%v must be in [0.0, 1.0]", i, p)
		}
	}

	// First should be 0.0 (group 0, 0%)
	if absF32(allProgress[0]-0.0) >= 0.001 {
		t.Errorf("first should be 0.0, got %v", allProgress[0])
	}
	// Last should be 1.0 (group 4, 100%)
	if absF32(allProgress[14]-1.0) >= 0.001 {
		t.Errorf("last should be 1.0, got %v", allProgress[14])
	}
}

// TEST917: High-frequency progress emission does not violate bounds
// (Regression test for the deadlock scenario — verifies computation stays bounded)
func Test917_high_frequency_progress_bounded(t *testing.T) {
	var count atomic.Uint32
	var mu sync.Mutex
	maxVal := float32(-3.4e38)
	minVal := float32(3.4e38)

	parent := CapProgressFn(func(p float32, _capUrn string, _msg string) {
		count.Add(1)
		mu.Lock()
		if p > maxVal {
			maxVal = p
		}
		if p < minVal {
			minVal = p
		}
		mu.Unlock()
	})

	mapper := NewProgressMapper(parent, 0.1, 0.8)

	// Simulate 100,000 rapid progress updates (like model download without throttle)
	for i := 0; i < 100000; i++ {
		p := float32(i) / 100000.0
		mapper.Report(p, "", "downloading")
	}

	if count.Load() != 100000 {
		t.Fatalf("expected 100000 reports, got %d", count.Load())
	}
	if minVal < 0.1 {
		t.Errorf("min %v must be >= base 0.1", minVal)
	}
	if maxVal > 0.9 {
		t.Errorf("max %v must be <= base+weight 0.9", maxVal)
	}
}
