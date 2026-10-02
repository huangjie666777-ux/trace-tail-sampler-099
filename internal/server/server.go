
// Package server exposes the OTLP/HTTP receiver and admin endpoints.
package server

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"

	coltracev1 "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/proto"

	"trace-tail-sampler/internal/policy"
	"trace-tail-sampler/internal/sampler"
)

// StatusReporter supplies counters for /status.
type StatusReporter interface {
	Stats() sampler.Counters
}

// OutputReporter supplies output failure info for /status.
type OutputReporter interface {
	OutputStats() (failures uint64, lastError string)
}

type Server struct {
	sampler  *sampler.Sampler
	policies *policy.Manager
	output   OutputReporter
	maxBytes int64
	mux      *http.ServeMux
	http     *http.Server
}

func New(addr string, maxBytes int64, sm *sampler.Sampler, pm *policy.Manager, out OutputReporter) *Server {
	s := &Server{sampler: sm, policies: pm, output: out, maxBytes: maxBytes, mux: http.NewServeMux()}
	s.mux.HandleFunc("POST /v1/traces", s.handleTraces)
	s.mux.HandleFunc("GET /policy", s.handleGetPolicy)
	s.mux.HandleFunc("PUT /policy", s.handlePutPolicy)
	s.mux.HandleFunc("GET /status", s.handleStatus)
	s.http = &http.Server{Addr: addr, Handler: s.mux}
	return s
}

// ListenAndServe binds and serves until the server is shut down.
func (s *Server) ListenAndServe() error {
	ln, err := net.Listen("tcp", s.http.Addr)
	if err != nil {
		return err
	}
	return s.http.Serve(ln)
}

// Shutdown gracefully stops the HTTP server.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.http.Shutdown(ctx)
}

func (s *Server) handleTraces(w http.ResponseWriter, r *http.Request) {
	if ct := r.Header.Get("Content-Type"); ct != "application/x-protobuf" && ct != "application/protobuf" {
		http.Error(w, "unsupported content type", http.StatusUnsupportedMediaType)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, s.maxBytes))
	if err != nil {
		http.Error(w, "request too large or unreadable", http.StatusRequestEntityTooLarge)
		return
	}
	var req coltracev1.ExportTraceServiceRequest
	if err := proto.Unmarshal(body, &req); err != nil {
		http.Error(w, "invalid protobuf: "+err.Error(), http.StatusBadRequest)
		return
	}
	var envs []*sampler.SpanEnvelope
	for _, rs := range req.ResourceSpans {
		for _, ss := range rs.ScopeSpans {
			for _, sp := range ss.Spans {
				envs = append(envs, &sampler.SpanEnvelope{Resource: rs.Resource, Scope: ss.Scope, Span: sp})
			}
		}
	}
	if err := s.sampler.Ingest(envs); err != nil {
		if errors.Is(err, sampler.ErrCapacity) {
			http.Error(w, err.Error(), http.StatusTooManyRequests)
			return
		}
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/x-protobuf")
	w.WriteHeader(http.StatusOK)
	resp, _ := proto.Marshal(&coltracev1.ExportTraceServiceResponse{})
	_, _ = w.Write(resp)
}

func (s *Server) handleGetPolicy(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.policies.Current())
}

func (s *Server) handlePutPolicy(w http.ResponseWriter, r *http.Request) {
	var u policy.Update
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&u); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	p, err := s.policies.Replace(u)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeJSON(w, http.StatusOK, p)
}

type statusView struct {
	Policy           policy.Policy     `json:"policy"`
	Counters         sampler.Counters  `json:"counters"`
	OutputFailures   uint64            `json:"output_failures"`
	LastOutputError  string            `json:"last_output_error,omitempty"`
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	v := statusView{Policy: s.policies.Current(), Counters: s.sampler.Stats()}
	if s.output != nil {
		v.OutputFailures, v.LastOutputError = s.output.OutputStats()
	}
	writeJSON(w, http.StatusOK, v)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
