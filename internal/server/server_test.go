
package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	coltracev1 "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"

	"trace-tail-sampler/internal/policy"
	"trace-tail-sampler/internal/sampler"
)

type fakeSink struct{}

func (fakeSink) AppendKept(sampler.KeptRecord) {}

func newTestServer(t *testing.T) (*Server, *sampler.Sampler) {
	t.Helper()
	pm := policy.NewManager(policy.Policy{Version: 1, WaitDuration: time.Second, SlowThreshold: time.Second, SampleRatio: 1})
	sm := sampler.New(sampler.Config{DecisionCacheTTL: time.Minute}, pm, fakeSink{})
	t.Cleanup(sm.Close)
	s := New("127.0.0.1:0", 1<<20, sm, pm, nil)
	return s, sm
}

func exportRequest() []byte {
	tid := bytes.Repeat([]byte{7}, 16)
	sid := bytes.Repeat([]byte{8}, 8)
	req := &coltracev1.ExportTraceServiceRequest{
		ResourceSpans: []*tracev1.ResourceSpans{{
			Resource: &resourcev1.Resource{Attributes: []*commonv1.KeyValue{{
				Key:   "service.name",
				Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "svc"}},
			}}},
			ScopeSpans: []*tracev1.ScopeSpans{{
				Scope: &commonv1.InstrumentationScope{Name: "lib"},
				Spans: []*tracev1.Span{{
					TraceId: tid, SpanId: sid, Name: "op",
					StartTimeUnixNano: 1, EndTimeUnixNano: 2,
				}},
			}},
		}},
	}
	b, _ := proto.Marshal(req)
	return b
}

func TestTracesEndpoint(t *testing.T) {
	s, sm := newTestServer(t)
	ts := httptest.NewServer(s.mux)
	defer ts.Close()

	resp, err := http.Post(ts.URL+"/v1/traces", "application/x-protobuf", bytes.NewReader(exportRequest()))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	if sm.Stats().ReceivedSpans != 1 {
		t.Fatal("span not ingested")
	}

	// Wrong content type rejected.
	resp, _ = http.Post(ts.URL+"/v1/traces", "application/json", bytes.NewReader(exportRequest()))
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnsupportedMediaType {
		t.Fatalf("status=%d", resp.StatusCode)
	}

	// Garbage protobuf rejected without state change.
	resp, _ = http.Post(ts.URL+"/v1/traces", "application/x-protobuf", bytes.NewReader([]byte{0xff, 0xff, 0xff}))
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	if sm.Stats().ReceivedSpans != 1 {
		t.Fatal("invalid batch changed state")
	}
}

func TestPolicyEndpoints(t *testing.T) {
	s, _ := newTestServer(t)
	ts := httptest.NewServer(s.mux)
	defer ts.Close()

	resp, _ := http.Get(ts.URL + "/policy")
	var p struct {
		Version       uint64  `json:"version"`
		WaitDuration  string  `json:"wait_duration"`
		SlowThreshold string  `json:"slow_threshold"`
		SampleRatio   float64 `json:"sample_ratio"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&p)
	resp.Body.Close()
	if p.Version != 1 || p.SampleRatio != 1 {
		t.Fatalf("policy=%+v", p)
	}

	body := bytes.NewReader([]byte(`{"wait_duration":"2s","slow_threshold":"100ms","sample_ratio":0.25}`))
	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/policy", body)
	resp, _ = http.DefaultClient.Do(req)
	_ = json.NewDecoder(resp.Body).Decode(&p)
	resp.Body.Close()
	if p.Version != 2 || p.SampleRatio != 0.25 || p.WaitDuration != "2s" {
		t.Fatalf("policy=%+v", p)
	}

	// Invalid replacement rejected.
	body = bytes.NewReader([]byte(`{"wait_duration":"2s","slow_threshold":"100ms","sample_ratio":2}`))
	req, _ = http.NewRequest(http.MethodPut, ts.URL+"/policy", body)
	resp, _ = http.DefaultClient.Do(req)
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status=%d", resp.StatusCode)
	}
}

func TestStatusEndpoint(t *testing.T) {
	s, _ := newTestServer(t)
	ts := httptest.NewServer(s.mux)
	defer ts.Close()
	resp, _ := http.Get(ts.URL + "/status")
	var v statusView
	if err := json.NewDecoder(resp.Body).Decode(&v); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if v.Policy.Version != 1 {
		t.Fatalf("status=%+v", v)
	}
}
