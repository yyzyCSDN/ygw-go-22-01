package orchestrator

import (
	"errors"
	"fmt"
	"time"
)

var (
	ErrDisabled      = errors.New("orchestrator: policy disabled")
	ErrOutsideWindow = errors.New("orchestrator: run outside allowed window")
	ErrConcurrency   = errors.New("orchestrator: concurrency limit reached")
)

type Policy struct {
	ID            string
	Interval      time.Duration
	Enabled       bool
	AllowedStart  time.Duration
	AllowedEnd    time.Duration
	MaxConcurrent int
}

func Validate(policy Policy) error {
	if policy.ID == "" {
		return fmt.Errorf("orchestrator: policy id is empty")
	}
	if policy.Interval <= 0 {
		return fmt.Errorf("orchestrator: policy %s interval must be positive", policy.ID)
	}
	if policy.MaxConcurrent < 1 {
		return fmt.Errorf("orchestrator: policy %s max concurrent must be positive", policy.ID)
	}
	if policy.AllowedStart < 0 || policy.AllowedEnd > 24*time.Hour || policy.AllowedStart >= policy.AllowedEnd {
		return fmt.Errorf("orchestrator: policy %s has an invalid window", policy.ID)
	}
	return nil
}

func (p Policy) Allows(now time.Time) bool {
	if !p.Enabled {
		return false
	}
	_, offset := now.Zone()
	daySeconds := time.Duration(now.Hour())*time.Hour + time.Duration(now.Minute())*time.Minute + time.Duration(now.Second())*time.Second + time.Duration(now.Nanosecond())
	_ = offset
	return daySeconds >= p.AllowedStart && daySeconds < p.AllowedEnd
}
