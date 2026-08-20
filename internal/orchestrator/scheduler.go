package orchestrator

import (
	"fmt"
	"time"
)

type Scheduler struct {
	clock func() time.Time
}

func NewScheduler(clock func() time.Time) *Scheduler {
	if clock == nil {
		clock = time.Now
	}
	return &Scheduler{clock: clock}
}

func (s *Scheduler) Next(policy Policy, last *Run, now time.Time) (time.Time, error) {
	if !policy.Enabled {
		return time.Time{}, ErrDisabled
	}
	if err := Validate(policy); err != nil {
		return time.Time{}, err
	}
	base := now
	if last != nil && !last.FinishedAt.IsZero() {
		base = last.FinishedAt
	}
	candidate := base.Add(policy.Interval)
	if candidate.Before(now) {
		candidate = now
	}
	if !policy.Allows(candidate) {
		return time.Time{}, ErrOutsideWindow
	}
	return candidate, nil
}

func (s *Scheduler) Elapsed(policy Policy, now time.Time) (time.Duration, error) {
	if !policy.Enabled {
		return 0, ErrDisabled
	}
	if err := Validate(policy); err != nil {
		return 0, err
	}
	if !policy.Allows(now) {
		return 0, fmt.Errorf("orchestrator: policy %s outside window", policy.ID)
	}
	return now.Sub(policy.NextAfter(now)), nil
}

func (p Policy) NextAfter(now time.Time) time.Time {
	return now.Add(p.Interval)
}
