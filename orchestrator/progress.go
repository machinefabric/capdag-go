package orchestrator

// Progress mapping — ported from Rust's orchestrator/executor.rs.
//
// This is the single progress-mapping computation used everywhere progress is
// subdivided: DAG per-group subdivision, ForEach per-item subdivision, peer-call
// range delegation, and cartridge frame-to-callback mapping. No ad-hoc
// derivations elsewhere.

// CapProgressFn is a callback for reporting per-cap progress.
// Parameters: (progress 0.0–1.0, cap URN string, human-readable message).
// Mirrors Rust's CapProgressFn type alias.
type CapProgressFn func(progress float32, capUrn string, msg string)

// clampUnit clamps a value to [0.0, 1.0]. Mirrors Rust's f32::clamp(0.0, 1.0).
func clampUnit(v float32) float32 {
	if v < 0.0 {
		return 0.0
	}
	if v > 1.0 {
		return 1.0
	}
	return v
}

// MapProgress maps child progress [0.0, 1.0] into a parent range
// [base, base + weight].
//
// This is the canonical progress mapping formula. The child value is clamped to
// [0.0, 1.0] before mapping, so the result is always within [base, base+weight].
// Mirrors Rust's map_progress.
func MapProgress(childProgress, base, weight float32) float32 {
	return base + clampUnit(childProgress)*weight
}

// ProgressMapper wraps a CapProgressFn with a progress range subdivision.
// Mirrors Rust's ProgressMapper.
type ProgressMapper struct {
	base   float32
	weight float32
	parent CapProgressFn
}

// NewProgressMapper creates a mapper that maps child [0.0, 1.0] into parent
// [base, base + weight]. Mirrors Rust's ProgressMapper::new.
func NewProgressMapper(parent CapProgressFn, base, weight float32) *ProgressMapper {
	return &ProgressMapper{base: base, weight: weight, parent: parent}
}

// Report reports child progress. The value is clamped to [0.0, 1.0] and mapped
// into the parent range before being forwarded to the parent callback.
// Mirrors Rust's ProgressMapper::report.
func (m *ProgressMapper) Report(childProgress float32, capUrn string, msg string) {
	overall := MapProgress(childProgress, m.base, m.weight)
	m.parent(overall, capUrn, msg)
}

// AsCapProgressFn returns a CapProgressFn that forwards through this mapper.
// Mirrors Rust's ProgressMapper::as_cap_progress_fn.
func (m *ProgressMapper) AsCapProgressFn() CapProgressFn {
	return func(p float32, capUrn string, msg string) {
		m.Report(p, capUrn, msg)
	}
}

// SubMapper creates a sub-mapper that maps a child range within this mapper's
// range. For example, if this mapper maps to [0.2, 0.8] (base=0.2, weight=0.6),
// a sub-mapper with subBase=0.5, subWeight=0.5 maps to [0.5, 0.8] in the
// parent's coordinate space. Mirrors Rust's ProgressMapper::sub_mapper.
func (m *ProgressMapper) SubMapper(subBase, subWeight float32) *ProgressMapper {
	return &ProgressMapper{
		base:   m.base + subBase*m.weight,
		weight: subWeight * m.weight,
		parent: m.parent,
	}
}
