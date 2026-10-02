package main

import (
	"hash/fnv"
)

// Decision values stored for every finalized trace.
const (
	DecisionKeep   = "keep"
	DecisionDrop   = "drop"
)

// Decision reasons reported in retained JSONL records.
const (
	ReasonError    = "error"
	ReasonSlow     = "slow"
	ReasonSampled  = "sampled"
	ReasonLateKeep = "late_append"
)

// Evaluate applies the tail sampling policy to a complete span set.
// Precedence: any ERROR span wins, then slow duration, then stable hash.
func Evaluate(spans []*SpanRecord, traceID []byte, p Policy) (decision, reason string) {
	var minStart, maxEnd uint64
	hasError := false
	for i, s := range spans {
		if s.Span.GetStatus().GetCode() == 2 { // STATUS_CODE_ERROR
			hasError = true
		}
		start := s.Span.GetStartTimeUnixNano()
		end := s.Span.GetEndTimeUnixNano()
		if i == 0 || start < minStart {
			minStart = start
		}
		if end > maxEnd {
			maxEnd = end
		}
	}
	if hasError {
		return DecisionKeep, ReasonError
	}
	if maxEnd >= minStart && maxEnd-minStart >= uint64(p.SlowThreshold.Nanoseconds()) {
		return DecisionKeep, ReasonSlow
	}
	if hashSelected(traceID, p.SampleRate) {
		return DecisionKeep, ReasonSampled
	}
	return DecisionDrop, "not_sampled"
}

// hashSelected gives a deterministic traceId -> bucket decision so that the
// same trace id always lands on the same side of the sampling fraction.
func hashSelected(traceID []byte, rate float64) bool {
	if rate <= 0 {
		return false
	}
	if rate >= 1 {
		return true
	}
	h := fnv.New64a()
	h.Write(traceID)
	// Use the high 53 bits so the value maps uniformly to [0,1) like a float64.
	bucket := float64(h.Sum64()>>11) / float64(1<<53)
	return bucket < rate
}
