
// Package sampler aggregates spans by trace ID across requests, decides
// keep/drop at a fixed deadline, and caches decisions for late spans.
package sampler

import (
	"encoding/hex"
	"errors"
	"fmt"
	"hash/fnv"
	"sync"
	"time"

	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"

	"trace-tail-sampler/internal/policy"
)

// ErrCapacity is returned when a batch cannot be accepted because the
// in-flight trace limit or per-trace span limit would be exceeded.
var ErrCapacity = errors.New("capacity exceeded")

// SpanEnvelope keeps a span together with its resource and scope so no
// attributes are lost.
type SpanEnvelope struct {
	Resource *resourcev1.Resource
	Scope    *commonv1.InstrumentationScope
	Span     *tracev1.Span
}

// KeptRecord is emitted for kept traces (and late-span appends).
type KeptRecord struct {
	TraceID       string
	Reason        string
	PolicyVersion uint64
	LateAppend    bool
	Spans         []*SpanEnvelope
}

// DecisionSink receives kept records. Implementations must be safe for
// concurrent use.
type DecisionSink interface {
	AppendKept(KeptRecord)
}

// Counters exposes operational statistics.
type Counters struct {
	ReceivedSpans     uint64 `json:"received_spans"`
	DuplicateSpans    uint64 `json:"duplicate_spans"`
	TracesKept        uint64 `json:"traces_kept"`
	TracesDropped     uint64 `json:"traces_dropped"`
	LateSpansAppended uint64 `json:"late_spans_appended"`
	LateSpansIgnored  uint64 `json:"late_spans_ignored"`
	RejectedBatches   uint64 `json:"rejected_batches"`
	InvalidBatches    uint64 `json:"invalid_batches"`
	InflightTraces    int    `json:"inflight_traces"`
	CachedDecisions   int    `json:"cached_decisions"`
}

type traceState struct {
	spans    map[string]*SpanEnvelope // keyed by hex span ID; first wins
	order    []string                 // span IDs in first-seen order
	deadline time.Time
	policy   policy.Policy
}

type decisionEntry struct {
	keep      bool
	reason    string
	version   uint64
	expires   time.Time
	keptIDs   map[string]bool // span IDs already written (kept traces only)
	keptOrder []string
	spans     map[string]*SpanEnvelope
}

// Config tunes the sampler limits.
type Config struct {
	MaxTraces        int
	MaxSpansPerTrace int
	DecisionCacheTTL time.Duration
	SweepInterval    time.Duration
}

// Sampler is the tail-sampling engine.
type Sampler struct {
	mu       sync.Mutex
	cfg      Config
	policies *policy.Manager
	sink     DecisionSink
	now      func() time.Time

	inflight map[string]*traceState
	decided  map[string]*decisionEntry
	stats    Counters

	stopCh chan struct{}
	doneCh chan struct{}
}

func New(cfg Config, policies *policy.Manager, sink DecisionSink) *Sampler {
	if cfg.SweepInterval <= 0 {
		cfg.SweepInterval = 50 * time.Millisecond
	}
	s := &Sampler{
		cfg:      cfg,
		policies: policies,
		sink:     sink,
		now:      time.Now,
		inflight: make(map[string]*traceState),
		decided:  make(map[string]*decisionEntry),
		stopCh:   make(chan struct{}),
		doneCh:   make(chan struct{}),
	}
	go s.sweepLoop()
	return s
}

// Close stops the background sweeper. It does not flush undecided traces.
func (s *Sampler) Close() {
	close(s.stopCh)
	<-s.doneCh
}

func (s *Sampler) sweepLoop() {
	defer close(s.doneCh)
	t := time.NewTicker(s.cfg.SweepInterval)
	defer t.Stop()
	for {
		select {
		case <-s.stopCh:
			return
		case <-t.C:
			s.sweep()
		}
	}
}

// SweepNow forces a decision/cache sweep; used by tests.
func (s *Sampler) SweepNow() { s.sweep() }

func (s *Sampler) sweep() {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	for id, tr := range s.inflight {
		if !now.Before(tr.deadline) {
			s.decideLocked(id, tr, now)
			delete(s.inflight, id)
		}
	}
	for id, d := range s.decided {
		if !now.Before(d.expires) {
			delete(s.decided, id)
		}
	}
}

// decideLocked runs the sampling decision exactly once per trace.
func (s *Sampler) decideLocked(id string, tr *traceState, now time.Time) {
	p := tr.policy
	hasError := false
	var minStart, maxEnd uint64
	for _, sid := range tr.order {
		sp := tr.spans[sid].Span
		if sp.Status != nil && sp.Status.Code == tracev1.Status_STATUS_CODE_ERROR {
			hasError = true
		}
		if minStart == 0 || sp.StartTimeUnixNano < minStart {
			minStart = sp.StartTimeUnixNano
		}
		if sp.EndTimeUnixNano > maxEnd {
			maxEnd = sp.EndTimeUnixNano
		}
	}
	entry := &decisionEntry{version: p.Version, expires: now.Add(s.cfg.DecisionCacheTTL)}
	switch {
	case hasError:
		entry.keep, entry.reason = true, "error"
	case maxEnd-minStart >= uint64(p.SlowThreshold):
		entry.keep, entry.reason = true, "slow"
	case hashFraction(id) < p.SampleRatio:
		entry.keep, entry.reason = true, "sampled"
	default:
		entry.keep, entry.reason = false, "dropped"
	}
	if entry.keep {
		entry.spans = tr.spans
		entry.keptOrder = tr.order
		entry.keptIDs = make(map[string]bool, len(tr.order))
		for _, sid := range tr.order {
			entry.keptIDs[sid] = true
		}
		s.emitLocked(id, entry, tr.order, false)
		s.stats.TracesKept++
	} else {
		s.stats.TracesDropped++
	}
	s.decided[id] = entry
}

// emitLocked writes a kept record; caller holds s.mu.
func (s *Sampler) emitLocked(id string, d *decisionEntry, spanIDs []string, late bool) {
	rec := KeptRecord{
		TraceID:       id,
		Reason:        d.reason,
		PolicyVersion: d.version,
		LateAppend:    late,
	}
	for _, sid := range spanIDs {
		rec.Spans = append(rec.Spans, d.spans[sid])
	}
	s.sink.AppendKept(rec)
}

// hashFraction maps a hex trace ID to a stable value in [0,1).
func hashFraction(hexID string) float64 {
	raw, err := hex.DecodeString(hexID)
	if err != nil {
		return 0
	}
	h := fnv.New64a()
	_, _ = h.Write(raw)
	return float64(h.Sum64()) / float64(^uint64(0))
}

// validateSpan checks IDs and timestamps.
func validateSpan(sp *tracev1.Span) error {
	if len(sp.TraceId) != 16 || isZero(sp.TraceId) {
		return fmt.Errorf("invalid trace_id length/content")
	}
	if len(sp.SpanId) != 8 || isZero(sp.SpanId) {
		return fmt.Errorf("invalid span_id length/content")
	}
	if sp.StartTimeUnixNano == 0 {
		return fmt.Errorf("missing start time")
	}
	if sp.EndTimeUnixNano < sp.StartTimeUnixNano {
		return fmt.Errorf("end time before start time")
	}
	return nil
}

func isZero(b []byte) bool {
	for _, x := range b {
		if x != 0 {
			return false
		}
	}
	return true
}

// Ingest validates and merges a batch atomically: any validation or
// capacity failure rejects the whole batch without changing state.
func (s *Sampler) Ingest(envs []*SpanEnvelope) error {
	// Phase 1: validate every span before touching state.
	for _, e := range envs {
		if e == nil || e.Span == nil {
			s.mu.Lock()
			s.stats.InvalidBatches++
			s.mu.Unlock()
			return fmt.Errorf("nil span in batch")
		}
		if err := validateSpan(e.Span); err != nil {
			s.mu.Lock()
			s.stats.InvalidBatches++
			s.mu.Unlock()
			return err
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()

	// Phase 2: capacity pre-check (no mutation yet).
	newTraces := 0
	addPerTrace := make(map[string]int)
	seenInBatch := make(map[string]map[string]bool)
	for _, e := range envs {
		tid := hex.EncodeToString(e.Span.TraceId)
		sid := hex.EncodeToString(e.Span.SpanId)
		if seenInBatch[tid] == nil {
			seenInBatch[tid] = make(map[string]bool)
		}
		if seenInBatch[tid][sid] {
			continue // duplicate within batch: first wins, no capacity cost
		}
		seenInBatch[tid][sid] = true
		if d, ok := s.decided[tid]; ok && now.Before(d.expires) {
			if d.keep && !d.keptIDs[sid] {
				addPerTrace[tid]++ // late append to kept trace
			}
			continue
		}
		tr, ok := s.inflight[tid]
		if !ok {
			newTraces++
			addPerTrace[tid]++
			continue
		}
		if _, dup := tr.spans[sid]; !dup {
			addPerTrace[tid]++
		}
	}
	if s.cfg.MaxTraces > 0 && len(s.inflight)+newTraces > s.cfg.MaxTraces {
		s.stats.RejectedBatches++
		return fmt.Errorf("%w: in-flight trace limit %d", ErrCapacity, s.cfg.MaxTraces)
	}
	if s.cfg.MaxSpansPerTrace > 0 {
		for tid, add := range addPerTrace {
			cur := 0
			if tr, ok := s.inflight[tid]; ok {
				cur = len(tr.spans)
			} else if d, ok := s.decided[tid]; ok && d.keep {
				cur = len(d.keptIDs)
			}
			if cur+add > s.cfg.MaxSpansPerTrace {
				s.stats.RejectedBatches++
				return fmt.Errorf("%w: per-trace span limit %d for trace %s", ErrCapacity, s.cfg.MaxSpansPerTrace, tid)
			}
		}
	}

	// Phase 3: apply.
	for _, e := range envs {
		tid := hex.EncodeToString(e.Span.TraceId)
		sid := hex.EncodeToString(e.Span.SpanId)
		s.stats.ReceivedSpans++

		if d, ok := s.decided[tid]; ok && now.Before(d.expires) {
			if !d.keep {
				s.stats.LateSpansIgnored++
				continue
			}
			if d.keptIDs[sid] {
				s.stats.DuplicateSpans++
				continue
			}
			// Late span on a kept trace: append, never re-decide.
			d.keptIDs[sid] = true
			d.keptOrder = append(d.keptOrder, sid)
			d.spans[sid] = e
			s.emitLocked(tid, d, []string{sid}, true)
			s.stats.LateSpansAppended++
			continue
		}

		tr, ok := s.inflight[tid]
		if !ok {
			p := s.policies.Current()
			tr = &traceState{
				spans:    make(map[string]*SpanEnvelope),
				deadline: now.Add(p.WaitDuration),
				policy:   p,
			}
			s.inflight[tid] = tr
		}
		if _, dup := tr.spans[sid]; dup {
			s.stats.DuplicateSpans++
			continue
		}
		tr.spans[sid] = e
		tr.order = append(tr.order, sid)
	}
	return nil
}

// Stats returns a snapshot of counters.
func (s *Sampler) Stats() Counters {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := s.stats
	c.InflightTraces = len(s.inflight)
	c.CachedDecisions = len(s.decided)
	return c
}

