package main

import (
	"errors"
	"sync"
	"time"
)

var (
	errInvalidPolicy  = errors.New("invalid policy: wait and slow threshold must be > 0, sample rate must be within [0,1]")
	errInvalidTTL     = errors.New("decision ttl must be > 0")
	errInvalidLimit   = errors.New("capacity limits and sweep interval must be > 0")
)

// Policy is an immutable, versioned sampling configuration. Pending traces
// capture the version active at first-span arrival and keep using it even if
// the whole policy is replaced later.
type Policy struct {
	Version       int64         `json:"version"`
	WaitDuration  time.Duration `json:"wait_duration"`
	SlowThreshold time.Duration `json:"slow_threshold"`
	SampleRate    float64       `json:"sample_rate"`
}

func (p Policy) Validate() error {
	if p.WaitDuration <= 0 || p.SlowThreshold <= 0 || p.SampleRate < 0 || p.SampleRate > 1 {
		return errInvalidPolicy
	}
	return nil
}

// JSONView is the external representation with human-friendly durations.
type PolicyView struct {
	Version       int64   `json:"version"`
	WaitMS        int64   `json:"wait_ms"`
	SlowThresholdMS int64 `json:"slow_threshold_ms"`
	SampleRate    float64 `json:"sample_rate"`
}

func (p Policy) View() PolicyView {
	return PolicyView{
		Version:         p.Version,
		WaitMS:          int64(p.WaitDuration / time.Millisecond),
		SlowThresholdMS: int64(p.SlowThreshold / time.Millisecond),
		SampleRate:      p.SampleRate,
	}
}

// PolicyStore provides atomic access to the current policy. Replace assigns a
// new monotonically increasing version.
type PolicyStore struct {
	mu      sync.RWMutex
	current Policy
}

func NewPolicyStore(initial Policy) *PolicyStore {
	return &PolicyStore{current: initial}
}

func (s *PolicyStore) Current() Policy {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.current
}

// Replace stores the new policy and returns the version actually assigned.
// Callers must not pre-set Version; it is overwritten.
func (s *PolicyStore) Replace(next Policy) (Policy, error) {
	if err := next.Validate(); err != nil {
		return Policy{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	next.Version = s.current.Version + 1
	s.current = next
	return next, nil
}
