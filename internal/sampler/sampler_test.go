
package sampler

import (
	"errors"
	"testing"
	"time"

	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"

	"trace-tail-sampler/internal/policy"
)

type fakeSink struct{ recs []KeptRecord }

func (f *fakeSink) AppendKept(r KeptRecord) { f.recs = append(f.recs, r) }

func newTestSampler(t *testing.T, cfg Config, ratio float64) (*Sampler, *fakeSink, *policy.Manager) {
	t.Helper()
	sink := &fakeSink{}
	pm := policy.NewManager(policy.Policy{Version: 1, WaitDuration: time.Second, SlowThreshold: 500 * time.Millisecond, SampleRatio: ratio})
	s := New(cfg, pm, sink)
	base := time.Now()
	s.now = func() time.Time { return base }
	t.Cleanup(s.Close)
	return s, sink, pm
}

func span(traceByte, spanByte byte, start, end uint64, code tracev1.Status_StatusCode) *SpanEnvelope {
	tid := make([]byte, 16)
	for i := range tid {
		tid[i] = traceByte
	}
	sid := make([]byte, 8)
	for i := range sid {
		sid[i] = spanByte
	}
	return &SpanEnvelope{Span: &tracev1.Span{
		TraceId:           tid,
		SpanId:            sid,
		Name:              "op",
		StartTimeUnixNano: start,
		EndTimeUnixNano:   end,
		Status:            &tracev1.Status{Code: code},
	}}
}

func advance(s *Sampler, d time.Duration) {
	base := s.now()
	s.now = func() time.Time { return base.Add(d) }
}

func TestInvalidBatchDoesNotChangeState(t *testing.T) {
	s, _, _ := newTestSampler(t, Config{DecisionCacheTTL: time.Minute}, 1)
	bad := span(1, 1, 100, 50, 0) // end before start
	if err := s.Ingest([]*SpanEnvelope{bad}); err == nil {
		t.Fatal("expected validation error")
	}
	if got := s.Stats().InflightTraces; got != 0 {
		t.Fatalf("state changed: inflight=%d", got)
	}
	zero := span(0, 1, 100, 200, 0) // zero trace id
	if err := s.Ingest([]*SpanEnvelope{zero}); err == nil {
		t.Fatal("expected zero-id error")
	}
	if got := s.Stats().InvalidBatches; got != 2 {
		t.Fatalf("invalid batches=%d", got)
	}
}

func TestDuplicateSpanFirstWins(t *testing.T) {
	s, sink, _ := newTestSampler(t, Config{DecisionCacheTTL: time.Minute}, 1)
	first := span(1, 1, 100, 200, 0)
	first.Span.Name = "first"
	dup := span(1, 1, 100, 200, 0)
	dup.Span.Name = "dup"
	if err := s.Ingest([]*SpanEnvelope{first}); err != nil {
		t.Fatal(err)
	}
	if err := s.Ingest([]*SpanEnvelope{dup}); err != nil {
		t.Fatal(err)
	}
	advance(s, 2*time.Second)
	s.SweepNow()
	if len(sink.recs) != 1 || len(sink.recs[0].Spans) != 1 {
		t.Fatalf("recs=%+v", sink.recs)
	}
	if sink.recs[0].Spans[0].Span.Name != "first" {
		t.Fatalf("duplicate overwrote first content")
	}
	if s.Stats().DuplicateSpans != 1 {
		t.Fatalf("duplicates not counted")
	}
}

func TestDeadlineFixedAndErrorKeeps(t *testing.T) {
	s, sink, _ := newTestSampler(t, Config{DecisionCacheTTL: time.Minute}, 0)
	if err := s.Ingest([]*SpanEnvelope{span(2, 1, 100, 200, 0)}); err != nil {
		t.Fatal(err)
	}
	// Arrives 900ms later; must not extend the 1s deadline.
	advance(s, 900*time.Millisecond)
	if err := s.Ingest([]*SpanEnvelope{span(2, 2, 100, 300, tracev1.Status_STATUS_CODE_ERROR)}); err != nil {
		t.Fatal(err)
	}
	advance(s, 200*time.Millisecond) // now 1.1s past first arrival
	s.SweepNow()
	if len(sink.recs) != 1 {
		t.Fatalf("expected decision at fixed deadline, recs=%d", len(sink.recs))
	}
	if sink.recs[0].Reason != "error" {
		t.Fatalf("reason=%s", sink.recs[0].Reason)
	}
	if s.Stats().InflightTraces != 0 {
		t.Fatal("trace still inflight after deadline")
	}
}

func TestSlowTraceKept(t *testing.T) {
	s, sink, _ := newTestSampler(t, Config{DecisionCacheTTL: time.Minute}, 0)
	if err := s.Ingest([]*SpanEnvelope{span(3, 1, 1, uint64(time.Second), 0)}); err != nil {
		t.Fatal(err)
	}
	advance(s, 2*time.Second)
	s.SweepNow()
	if len(sink.recs) != 1 || sink.recs[0].Reason != "slow" {
		t.Fatalf("recs=%+v", sink.recs)
	}
}

func TestRatioSamplingAndDrop(t *testing.T) {
	// ratio 1 keeps everything as "sampled"
	s, sink, _ := newTestSampler(t, Config{DecisionCacheTTL: time.Minute}, 1)
	_ = s.Ingest([]*SpanEnvelope{span(4, 1, 1, 100, 0)})
	advance(s, 2*time.Second)
	s.SweepNow()
	if len(sink.recs) != 1 || sink.recs[0].Reason != "sampled" {
		t.Fatalf("recs=%+v", sink.recs)
	}
	// ratio 0 drops
	s2, sink2, _ := newTestSampler(t, Config{DecisionCacheTTL: time.Minute}, 0)
	_ = s2.Ingest([]*SpanEnvelope{span(4, 1, 1, 100, 0)})
	advance(s2, 2*time.Second)
	s2.SweepNow()
	if len(sink2.recs) != 0 || s2.Stats().TracesDropped != 1 {
		t.Fatalf("expected drop, recs=%d", len(sink2.recs))
	}
}

func TestLateSpanAppendsAndDoesNotFlip(t *testing.T) {
	s, sink, _ := newTestSampler(t, Config{DecisionCacheTTL: time.Minute}, 1)
	_ = s.Ingest([]*SpanEnvelope{span(5, 1, 1, 100, 0)})
	advance(s, 2*time.Second)
	s.SweepNow()
	// Late error span on kept trace: appended, decision unchanged.
	_ = s.Ingest([]*SpanEnvelope{span(5, 2, 1, 100, tracev1.Status_STATUS_CODE_ERROR)})
	if len(sink.recs) != 2 || !sink.recs[1].LateAppend || sink.recs[1].Reason != "sampled" {
		t.Fatalf("recs=%+v", sink.recs)
	}
	if s.Stats().LateSpansAppended != 1 {
		t.Fatal("late append not counted")
	}

	// Dropped trace: late error span must not flip the decision.
	s2, sink2, _ := newTestSampler(t, Config{DecisionCacheTTL: time.Minute}, 0)
	_ = s2.Ingest([]*SpanEnvelope{span(6, 1, 1, 100, 0)})
	advance(s2, 2*time.Second)
	s2.SweepNow()
	_ = s2.Ingest([]*SpanEnvelope{span(6, 2, 1, 100, tracev1.Status_STATUS_CODE_ERROR)})
	if len(sink2.recs) != 0 || s2.Stats().LateSpansIgnored != 1 {
		t.Fatalf("dropped trace flipped or late not ignored: recs=%d", len(sink2.recs))
	}
}

func TestCacheExpiryAllowsRegroup(t *testing.T) {
	s, sink, _ := newTestSampler(t, Config{DecisionCacheTTL: time.Second}, 1)
	_ = s.Ingest([]*SpanEnvelope{span(7, 1, 1, 100, 0)})
	advance(s, 2*time.Second)
	s.SweepNow()
	// Expire the decision cache.
	advance(s, 2*time.Second)
	s.SweepNow()
	if s.Stats().CachedDecisions != 0 {
		t.Fatal("decision cache not expired")
	}
	// Same trace ID can form a new group.
	if err := s.Ingest([]*SpanEnvelope{span(7, 9, 1, 100, 0)}); err != nil {
		t.Fatal(err)
	}
	if s.Stats().InflightTraces != 1 {
		t.Fatal("trace did not regroup after expiry")
	}
	advance(s, 2*time.Second)
	s.SweepNow()
	if len(sink.recs) != 2 {
		t.Fatalf("recs=%d", len(sink.recs))
	}
}

func TestCapacityLimits(t *testing.T) {
	s, _, _ := newTestSampler(t, Config{MaxTraces: 1, MaxSpansPerTrace: 2, DecisionCacheTTL: time.Minute}, 1)
	if err := s.Ingest([]*SpanEnvelope{span(8, 1, 1, 100, 0)}); err != nil {
		t.Fatal(err)
	}
	// Second distinct trace rejected, in-flight trace not evicted.
	if err := s.Ingest([]*SpanEnvelope{span(9, 1, 1, 100, 0)}); !errors.Is(err, ErrCapacity) {
		t.Fatalf("expected capacity error, got %v", err)
	}
	if s.Stats().InflightTraces != 1 {
		t.Fatal("in-flight trace evicted")
	}
	// Per-trace span limit: batch rejected atomically.
	err := s.Ingest([]*SpanEnvelope{span(8, 2, 1, 100, 0), span(8, 3, 1, 100, 0)})
	if !errors.Is(err, ErrCapacity) {
		t.Fatalf("expected span-limit error, got %v", err)
	}
	if got := s.Stats(); got.InflightTraces != 1 {
		t.Fatalf("partial batch applied: %+v", got)
	}
}

func TestInflightTraceKeepsPolicyVersion(t *testing.T) {
	s, sink, pm := newTestSampler(t, Config{DecisionCacheTTL: time.Minute}, 1)
	_ = s.Ingest([]*SpanEnvelope{span(10, 1, 1, 100, 0)})
	if _, err := pm.Replace(policy.Update{WaitDuration: "5s", SlowThreshold: "1s", SampleRatio: 0.5}); err != nil {
		t.Fatal(err)
	}
	advance(s, 1500*time.Millisecond) // past old 1s deadline, before new 5s
	s.SweepNow()
	if len(sink.recs) != 1 || sink.recs[0].PolicyVersion != 1 {
		t.Fatalf("in-flight trace did not use original policy: %+v", sink.recs)
	}
	if pm.Current().Version != 2 {
		t.Fatal("policy version not bumped")
	}
}

