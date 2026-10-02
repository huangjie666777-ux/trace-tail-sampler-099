
package store

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"

	"trace-tail-sampler/internal/sampler"
)

// Record is one JSONL line: a kept trace (or late-span append) with the
// full span contents, the decision reason and the policy version.
type Record struct {
	TraceID       string          `json:"trace_id"`
	Reason        string          `json:"reason"`
	PolicyVersion uint64          `json:"policy_version"`
	LateAppend    bool            `json:"late_append"`
	Spans         json.RawMessage `json:"spans"`
}

// Writer appends JSONL records to a file. Write failures are counted and
// the last error is retained so they stay visible via /status.
type Writer struct {
	mu        sync.Mutex
	f         *os.File
	enc       *json.Encoder
	failures  uint64
	lastError string
}

func NewWriter(path string) (*Writer, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open output file: %w", err)
	}
	return &Writer{f: f, enc: json.NewEncoder(f)}, nil
}

// AppendKept implements sampler.DecisionSink.
func (w *Writer) AppendKept(rec sampler.KeptRecord) {
	spans, err := json.Marshal(rec.Spans)
	if err != nil {
		w.recordErr(fmt.Errorf("marshal spans: %w", err))
		return
	}
	line := Record{
		TraceID:       rec.TraceID,
		Reason:        rec.Reason,
		PolicyVersion: rec.PolicyVersion,
		LateAppend:    rec.LateAppend,
		Spans:         spans,
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.enc.Encode(line); err != nil {
		w.recordErrLocked(fmt.Errorf("write record: %w", err))
	}
}

func (w *Writer) recordErr(err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.recordErrLocked(err)
}

func (w *Writer) recordErrLocked(err error) {
	w.failures++
	w.lastError = err.Error()
}

// OutputStats returns output failure count and last error.
func (w *Writer) OutputStats() (failures uint64, lastError string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.failures, w.lastError
}

func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.f.Close()
}

var _ sampler.DecisionSink = (*Writer)(nil)
