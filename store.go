package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"
)

// SpanRecord keeps a span together with the resource/scope it was reported
// with, preserving all attributes for later JSONL output.
type SpanRecord struct {
	Span     *tracev1.Span
	Resource *resourcev1.Resource
	Scope    *commonv1.InstrumentationScope
}

type pendingTrace struct {
	traceID    []byte
	spans      []*SpanRecord
	seen       map[string]struct{}
	policy     Policy
	firstSeen  time.Time
	deadline   time.Time
	timer      *time.Timer
}

type cachedDecision struct {
	decision    string
	reason      string
	policyVer   int64
	kept        []*SpanRecord
	seen        map[string]struct{}
	finalizedAt time.Time
}

// Counters are exported through the status endpoint.
type Counters struct {
	ReceivedBatches int64 `json:"received_batches"`
	RejectedBatches int64 `json:"rejected_batches"`
	ReceivedSpans   int64 `json:"received_spans"`
	DedupedSpans    int64 `json:"deduped_spans"`
	DecidedKeep     int64 `json:"decided_keep"`
	DecidedDrop     int64 `json:"decided_drop"`
	LateAppended    int64 `json:"late_appended"`
	ExpiredTraces   int64 `json:"expired_traces"`
	OutputFailures  int64 `json:"output_failures"`
}

// IngestResult reports what happened to one validated batch.
type IngestResult struct {
	AcceptedSpans int
	DedupedSpans  int
	NewTraces     int
}

// Store is the single synchronization point between HTTP receivers, the
// deadline timers and the cache sweeper. All maps are only touched while mu
// is held, which makes "exactly one output" and capacity checks safe.
type Store struct {
	mu        sync.Mutex
	pending   map[string]*pendingTrace
	cache     map[string]*cachedDecision
	maxTraces int
	maxSpans  int
	ttl       time.Duration
	writer    *JSONLWriter
	now       func() time.Time
	counters  Counters
}

func NewStore(maxTraces, maxSpansPerTrace int, ttl time.Duration, writer *JSONLWriter) *Store {
	return &Store{
		pending:   make(map[string]*pendingTrace),
		cache:     make(map[string]*cachedDecision),
		maxTraces: maxTraces,
		maxSpans:  maxSpansPerTrace,
		ttl:       ttl,
		writer:    writer,
		now:       time.Now,
	}
}

type pendingAdmission struct {
	newTraceIDs map[string]struct{}>
	newSpans    map[string]int // trace key -> number of new spans
}

// DryRun validates the batch against capacity without mutating any state.
// A batch is either fully accepted or fully rejected (atomicity requirement).
func (s *Store) DryRun(req *collectortrace.ExportTraceServiceRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	newTraces := map[string]struct{}{}
	addSpans := map[string]int{}
	for _, rs := range req.GetResourceSpans() {
		for _, ss := range rs.GetScopeSpans() {
			for _, span := range ss.GetSpans() {
				key := string(span.TraceId)
				if _, ok := s.pending[key]; ok {
					pt := s.pending[key]
					if _, dup := pt.seen[string(span.SpanId)]; dup {
						continue
					}
					addSpans[key]++
					if len(pt.spans)+addSpans[key] > s.maxSpans {
						return fmt.Errorf("trace %x span cap %d exceeded", span.TraceId, s.maxSpans)
					}
					continue
				}
				if cd, ok := s.cache[key]; ok {
					if _, dup := cd.seen[string(span.SpanId)]; dup {
						continue
					}
					if cd.decision == DecisionKeep {
						addSpans[key]++
						if len(cd.kept)+addSpans[key] > s.maxSpans {
							return fmt.Errorf("trace %x span cap %d exceeded", span.TraceId, s.maxSpans)
						}
					}
					continue
				}
				newTraces[key] = struct{}{}
				addSpans[key]++
				if addSpans[key] > s.maxSpans {
					return fmt.Errorf("trace %x span cap %d exceeded", span.TraceId, s.maxSpans)
				}
			}
		}
	}
	if s.totalLocked()+len(newTraces) > s.maxTraces {
		return fmt.Errorf("trace capacity %d exceeded (tracking %d, %d new)", s.maxTraces, s.totalLocked(), len(newTraces))
	}
	return nil
}

func (s *Store) totalLocked() int { return len(s.pending) + len(s.cache) }

// Ingest merges a validated batch. Must be called only after DryRun succeeds.
func (s *Store) Ingest(req *collectortrace.ExportTraceServiceRequest, policy Policy, onDeadline func(traceKey string)) IngestResult {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()
	res := IngestResult{}
	for _, rs := range req.GetResourceSpans() {
		for _, ss := range rs.GetScopeSpans() {
			for _, span := range ss.GetSpans() {
				key := string(span.TraceId)
				rec := &SpanRecord{Span: span, Resource: rs.GetResource(), Scope: ss.GetScope()}
				if pt := s.pending[key]; pt != nil {
					if _, dup := pt.seen[string(span.SpanId)]; dup {
						s.counters.DedupedSpans++
						res.DedupedSpans++
						continue
					}
					pt.seen[string(span.SpanId)] = struct{}{}
					pt.spans = append(pt.spans, rec)
					res.AcceptedSpans++
					continue
				}
				if cd := s.cache[key]; cd != nil {
					if _, dup := cd.seen[string(span.SpanId)]; dup {
						s.counters.DedupedSpans++
						res.DedupedSpans++
						continue
					}
					cd.seen[string(span.SpanId)] = struct{}{}
					res.AcceptedSpans++
					if cd.decision == DecisionKeep {
						cd.kept = append(cd.kept, rec)
						s.counters.LateAppended++
						// Output happens while holding the store lock, and the
						// decision record exists, so the append cannot be emitted twice.
						err := s.writer.WriteRetained([]byte(key), cd.policyVer, rec, ReasonLateKeep, now)
						if err != nil {
							s.counters.OutputFailures++
						}
					}
					continue
				}

				pt := &pendingTrace{
					traceID:   span.TraceId,
					seen:      map[string]struct{}{string(span.SpanId): {}},
					spans:     []*SpanRecord{rec},
					policy:    policy,
					firstSeen: now,
					deadline:  now.Add(policy.WaitDuration),
				}
				s.pending[key] = pt
				res.NewTraces++
				res.AcceptedSpans++
				deadlineKey := key
				d := policy.WaitDuration
				pt.timer = time.AfterFunc(d, func() { onDeadline(deadlineKey) })
			}
		}
	}
	s.counters.ReceivedSpans += int64(res.AcceptedSpans)
	return res
}

// Finalize is invoked by the deadline timer. It is idempotent: only the first
// call for a pending trace produces a decision and output.
func (s *Store) Finalize(traceKey string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pt := s.pending[traceKey]
	if pt == nil {
		return
	}
	delete(s.pending, traceKey)
	if pt.timer != nil {
		pt.timer.Stop()
	}

	now := s.now()
	decision, reason := Evaluate(pt.spans, pt.traceID, pt.policy)
	cd := &cachedDecision{
		decision:    decision,
		reason:      reason,
		policyVer:   pt.policy.Version,
		finalizedAt: now,
		seen:        pt.seen,
	}
	if decision == DecisionKeep {
		cd.kept = pt.spans
		s.counters.DecidedKeep++
		for _, rec := range pt.spans {
			if err := s.writer.WriteRetained(pt.traceID, pt.policy.Version, rec, reason, now); err != nil {
				s.counters.OutputFailures++
			}
		}
	} else {
		s.counters.DecidedDrop++
	}
	s.cache[traceKey] = cd
}

// SweepExpired removes decisions older than ttl. After expiry the same trace
// id may form a new group again.
func (s *Store) SweepExpired() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	removed := 0
	for key, cd := range s.cache {
		if now.Sub(cd.finalizedAt) >= s.ttl {
			delete(s.cache, key)
			removed++
		}
	}
	s.counters.ExpiredTraces += int64(removed)
	return removed
}

// Status is a consistent snapshot used by the status endpoint.
type Status struct {
	PendingTraces int      `json:"pending_traces"`
	CachedTraces  int      `json:"cached_traces"`
	KeptCached    int      `json:"kept_cached"`
	DropCached    int      `json:"drop_cached"`
	MaxTraces     int      `json:"max_traces"`
	MaxSpans      int      `json:"max_spans_per_trace"`
	Counters      Counters `json:"counters"`
}

func (s *Store) Snapshot() Status {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := Status{
		PendingTraces: len(s.pending),
		CachedTraces:  len(s.cache),
		MaxTraces:     s.maxTraces,
		MaxSpans:      s.maxSpans,
		Counters:      s.counters,
	}
	for _, cd := range s.cache {
		if cd.decision == DecisionKeep {
			st.KeptCached++
		} else {
			st.DropCached++
		}
	}
	return st
}

func (s *Store) incrRejected() {
	s.mu.Lock()
	s.counters.RejectedBatches++
	s.mu.Unlock()
}

func (s *Store) incrReceivedBatches() {
	s.mu.Lock()
	s.counters.ReceivedBatches++
	s.mu.Unlock()
}

// Shutdown stops pending timers. Finalized traces are already in the cache and
// late appends are only possible via the running HTTP server, which is shut
// down first by the caller.
func (s *Store) Shutdown() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for key, pt := range s.pending {
		if pt.timer != nil {
			pt.timer.Stop()
		}
		delete(s.pending, key)
	}
}

// RunSweeper periodically reclaims expired decisions until ctx is canceled.
func (s *Store) RunSweeper(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.SweepExpired()
		}
	}
}
