
// Command genpayload emits an OTLP ExportTraceServiceRequest (protobuf) to
// stdout for curl demos: genpayload <traceByte> <spanByte> <error|ok> <slow|fast>
package main

import (
	"fmt"
	"os"
	"strconv"

	coltracev1 "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

func main() {
	tb, _ := strconv.Atoi(os.Args[1])
	sb, _ := strconv.Atoi(os.Args[2])
	code := tracev1.Status_STATUS_CODE_OK
	if os.Args[3] == "error" {
		code = tracev1.Status_STATUS_CODE_ERROR
	}
	end := uint64(2_000_000_000) // 2s
	if os.Args[4] == "fast" {
		end = 100_000_000 // 100ms
	}
	tid := make([]byte, 16)
	for i := range tid {
		tid[i] = byte(tb)
	}
	sid := make([]byte, 8)
	for i := range sid {
		sid[i] = byte(sb)
	}
	req := &coltracev1.ExportTraceServiceRequest{
		ResourceSpans: []*tracev1.ResourceSpans{{
			Resource: &resourcev1.Resource{Attributes: []*commonv1.KeyValue{{
				Key: "service.name", Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "demo-svc"}},
			}}},
			ScopeSpans: []*tracev1.ScopeSpans{{
				Scope: &commonv1.InstrumentationScope{Name: "demo-lib", Version: "1.0"},
				Spans: []*tracev1.Span{{
					TraceId: tid, SpanId: sid, Name: fmt.Sprintf("op-%d", sb),
					StartTimeUnixNano: 1_000_000_000, EndTimeUnixNano: 1_000_000_000 + end,
					Status: &tracev1.Status{Code: code},
					Attributes: []*commonv1.KeyValue{{
						Key: "http.method", Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "GET"}},
					}},
				}},
			}},
		}},
	}
	b, _ := proto.Marshal(req)
	_, _ = os.Stdout.Write(b)
}

