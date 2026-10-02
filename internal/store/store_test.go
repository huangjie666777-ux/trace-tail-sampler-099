
package store

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"

	"trace-tail-sampler/internal/sampler"
)

func TestWriterAppendsJSONL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.jsonl")
	w, err := NewWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	w.AppendKept(sampler.KeptRecord{
		TraceID: "abc", Reason: "error", PolicyVersion: 3,
		Spans: []*sampler.SpanEnvelope{{Span: &tracev1.Span{Name: "op"}}},
	})
	w.AppendKept(sampler.KeptRecord{TraceID: "abc", Reason: "error", PolicyVersion: 3, LateAppend: true})
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	n := 0
	for sc.Scan() {
		var rec Record
		if err := json.Unmarshal(sc.Bytes(), &rec); err != nil {
			t.Fatalf("line %d not valid JSON: %v", n, err)
		}
		if rec.TraceID != "abc" || rec.Reason != "error" || rec.PolicyVersion != 3 {
			t.Fatalf("rec=%+v", rec)
		}
		n++
	}
	if n != 2 {
		t.Fatalf("lines=%d", n)
	}
	if fails, _ := w.OutputStats(); fails != 0 {
		t.Fatalf("unexpected failures=%d", fails)
	}
}

