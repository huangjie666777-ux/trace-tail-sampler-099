
// Command trace-tail-sampler runs an OTLP/HTTP tail-sampling backend.
package main

import (
	"context"
	"errors"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"trace-tail-sampler/internal/policy"
	"trace-tail-sampler/internal/sampler"
	"trace-tail-sampler/internal/server"
	"trace-tail-sampler/internal/store"
)

type config struct {
	addr             string
	maxRequestBytes  int64
	waitDuration     time.Duration
	slowThreshold    time.Duration
	sampleRatio      float64
	maxTraces        int
	maxSpansPerTrace int
	decisionCacheTTL time.Duration
	outputFile       string
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func loadConfig() (config, error) {
	var c config
	var err error
	c.addr = envOr("SAMPLER_ADDR", "127.0.0.1:4318")
	if host, _, splitErr := net.SplitHostPort(c.addr); splitErr == nil && host != "" {
		ip := net.ParseIP(host)
		if ip == nil || !ip.IsLoopback() {
			return c, errors.New("SAMPLER_ADDR must bind to a loopback address (localhost only)")
		}
	}
	if c.waitDuration, err = time.ParseDuration(envOr("SAMPLER_WAIT", "10s")); err != nil {
		return c, err
	}
	if c.slowThreshold, err = time.ParseDuration(envOr("SAMPLER_SLOW_THRESHOLD", "500ms")); err != nil {
		return c, err
	}
	if c.sampleRatio, err = strconv.ParseFloat(envOr("SAMPLER_RATIO", "0.1"), 64); err != nil {
		return c, err
	}
	if c.maxTraces, err = strconv.Atoi(envOr("SAMPLER_MAX_TRACES", "1000")); err != nil {
		return c, err
	}
	if c.maxSpansPerTrace, err = strconv.Atoi(envOr("SAMPLER_MAX_SPANS_PER_TRACE", "200")); err != nil {
		return c, err
	}
	if c.decisionCacheTTL, err = time.ParseDuration(envOr("SAMPLER_DECISION_CACHE_TTL", "5m")); err != nil {
		return c, err
	}
	if c.maxRequestBytes, err = strconv.ParseInt(envOr("SAMPLER_MAX_REQUEST_BYTES", "4194304"), 10, 64); err != nil {
		return c, err
	}
	c.outputFile = envOr("SAMPLER_OUTPUT_FILE", "kept_traces.jsonl")
	return c, nil
}

func main() {
	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	writer, err := store.NewWriter(cfg.outputFile)
	if err != nil {
		log.Fatalf("output: %v", err)
	}

	pm := policy.NewManager(policy.Policy{
		Version:       1,
		WaitDuration:  cfg.waitDuration,
		SlowThreshold: cfg.slowThreshold,
		SampleRatio:   cfg.sampleRatio,
	})

	sm := sampler.New(sampler.Config{
		MaxTraces:        cfg.maxTraces,
		MaxSpansPerTrace: cfg.maxSpansPerTrace,
		DecisionCacheTTL: cfg.decisionCacheTTL,
	}, pm, writer)

	srv := server.New(cfg.addr, cfg.maxRequestBytes, sm, pm, writer)

	errCh := make(chan error, 1)
	go func() {
		log.Printf("listening on %s (OTLP/HTTP /v1/traces)", cfg.addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		log.Printf("received %v, shutting down", sig)
	case err := <-errCh:
		log.Printf("server error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
	sm.Close()
	if err := writer.Close(); err != nil {
		log.Printf("closing output file: %v", err)
	}
	log.Printf("shutdown complete")
}

