package main

import (
	"flag"
	"os"
	"strconv"
	"time"
)

// Config holds all runtime configuration. Everything is derivable from
// command line flags, with PORT also honored from the environment.
type Config struct {
	Addr          string
	Port          int
	OutputPath    string
	WaitDuration   time.Duration
	SlowThreshold  time.Duration
	SampleRate     float64
	DecisionTTL    time.Duration
	MaxTraces      int
	MaxSpansPerTrace int
	MaxRequestBytes int64
	SweepInterval  time.Duration
}

func envInt(name string, fallback int) int {
	if v := os.Getenv(name); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

// ParseConfig builds Config from flags and the environment. The listener is
// always bound to 127.0.0.1; only the port is configurable.
func ParseConfig(args []string) (Config, error) {
	fs := flag.NewFlagSet("trace-tail-sampler", flag.ContinueOnError)
	cfg := Config{}
	fs.IntVar(&cfg.Port, "port", envInt("PORT", 4318), "listen port (bound to 127.0.0.1)")
	fs.StringVar(&cfg.OutputPath, "output", "retained.jsonl", "JSONL file for retained traces")
	fs.DurationVar(&cfg.WaitDuration, "wait", 5*time.Second, "grace period after the first span of a trace arrives")
	fs.DurationVar(&cfg.SlowThreshold, "slow-threshold", 1*time.Second, "minimum duration classified as a slow trace")
	fs.Float64Var(&cfg.SampleRate, "sample-rate", 0.1, "fraction of non-error/non-slow traces sampled by stable hash, 0..1")
	fs.DurationVar(&cfg.DecisionTTL, "decision-ttl", 10*time.Minute, "how long a finalized decision is cached for late spans")
	fs.IntVar(&cfg.MaxTraces, "max-traces", 10000, "maximum simultaneously tracked traces (pending + cached)")
	fs.IntVar(&cfg.MaxSpansPerTrace, "max-spans-per-trace", 1000, "maximum spans accepted for one trace")
	fs.Int64Var(&cfg.MaxRequestBytes, "max-request-bytes", 10<<20, "maximum accepted OTLP request body size in bytes")
	fs.DurationVar(&cfg.SweepInterval, "sweep-interval", time.Second, "interval between expired-decision sweeps")
	if err := fs.Parse(args); err != nil {
		return cfg, err
	}
	cfg.Addr = "127.0.0.1"
	return cfg, nil
}

// InitialPolicy validates static configuration and derives the first policy.
func (c Config) InitialPolicy() (Policy, error) {
	p := Policy{
		Version:        1,
		WaitDuration:   c.WaitDuration,
		SlowThreshold:  c.SlowThreshold,
		SampleRate:     c.SampleRate,
	}
	if err := p.Validate(); err != nil {
		return Policy{}, err
	}
	if c.DecisionTTL <= 0 {
		return Policy{}, errInvalidTTL
	}
	if c.MaxTraces <= 0 || c.MaxSpansPerTrace <= 0 || c.MaxRequestBytes <= 0 || c.SweepInterval <= 0 {
		return Policy{}, errInvalidLimit
	}
	return p, nil
}
