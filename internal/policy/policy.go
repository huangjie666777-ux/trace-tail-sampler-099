package policy

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"time"
)

// Policy describes a sampling strategy. Version increments on every
// whole-policy replacement; in-flight traces keep the version captured
// when their first span arrived.
type Policy struct {
	Version       uint64        `json:"version"`
	WaitDuration  time.Duration `json:"-"`
	SlowThreshold time.Duration `json:"-"`
	SampleRatio   float64       `json:"sample_ratio"`
}

// Update is the wire form for PUT /policy (durations as strings like "10s").
type Update struct {
	WaitDuration  string  `json:"wait_duration"`
	SlowThreshold string  `json:"slow_threshold"`
	SampleRatio   float64 `json:"sample_ratio"`
}

func (u Update) validate() (Policy, error) {
	var p Policy
	var err error
	if p.WaitDuration, err = time.ParseDuration(u.WaitDuration); err != nil || p.WaitDuration <= 0 {
		return p, fmt.Errorf("invalid wait_duration %q", u.WaitDuration)
	}
	if p.SlowThreshold, err = time.ParseDuration(u.SlowThreshold); err != nil || p.SlowThreshold < 0 {
		return p, fmt.Errorf("invalid slow_threshold %q", u.SlowThreshold)
	}
	if u.SampleRatio < 0 || u.SampleRatio > 1 {
		return p, errors.New("sample_ratio must be in [0,1]")
	}
	p.SampleRatio = u.SampleRatio
	return p, nil
}

// Manager holds the current policy atomically.
type Manager struct {
	cur atomic.Value // Policy
}

func NewManager(initial Policy) *Manager {
	m := &Manager{}
	m.cur.Store(initial)
	return m
}

func (m *Manager) Current() Policy { return m.cur.Load().(Policy) }

// Replace validates and atomically swaps the whole policy, bumping Version.
func (m *Manager) Replace(u Update) (Policy, error) {
	p, err := u.validate()
	if err != nil {
		return Policy{}, err
	}
	p.Version = m.Current().Version + 1
	m.cur.Store(p)
	return p, nil
}

// MarshalJSON renders durations as strings.
func (p Policy) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Version       uint64  `json:"version"`
		WaitDuration  string  `json:"wait_duration"`
		SlowThreshold string  `json:"slow_threshold"`
		SampleRatio   float64 `json:"sample_ratio"`
	}{p.Version, p.WaitDuration.String(), p.SlowThreshold.String(), p.SampleRatio})
}

