package orchestrator

import (
	"fmt"
	"sync"
	"time"
)

type Run struct {
	ID         string
	PolicyID   string
	Generation uint64
	SnapshotID string
	StartedAt  time.Time
	FinishedAt time.Time
	Succeeded  bool
}

type Runbook struct {
	mu     sync.Mutex
	next   uint64
	runs   map[string]*Run
	active map[string]int
	last   map[string]*Run
	limit  func(policyID string) int
}

func NewRunbook(limit func(policyID string) int) *Runbook {
	if limit == nil {
		limit = func(string) int { return 1 }
	}
	return &Runbook{runs: make(map[string]*Run), active: make(map[string]int), last: make(map[string]*Run), limit: limit}
}

func (r *Runbook) Start(policyID string, now time.Time) (*Run, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.active[policyID] >= r.limit(policyID) {
		return nil, ErrConcurrency
	}
	r.next++
	run := &Run{ID: fmt.Sprintf("run-%d", r.next), PolicyID: policyID, Generation: r.next, StartedAt: now.UTC(), SnapshotID: ""}
	r.runs[run.ID] = run
	r.active[policyID]++
	return run, nil
}

func (r *Runbook) Finish(runID string, succeeded bool, now time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	run, ok := r.runs[runID]
	if !ok {
		return fmt.Errorf("orchestrator: run %s missing", runID)
	}
	if !run.FinishedAt.IsZero() {
		return fmt.Errorf("orchestrator: run %s already finished", runID)
	}
	run.FinishedAt = now.UTC()
	run.Succeeded = succeeded
	r.active[run.PolicyID]--
	if r.active[run.PolicyID] < 0 {
		r.active[run.PolicyID] = 0
	}
	r.last[run.PolicyID] = run
	return nil
}

func (r *Runbook) Active(policyID string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.active[policyID]
}

func (r *Runbook) Last(policyID string) (*Run, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	run, ok := r.last[policyID]
	if !ok || run == nil {
		return nil, false
	}
	copyOf := *run
	return &copyOf, true
}

func (r *Runbook) Runs(policyID string) []Run {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make([]Run, 0)
	for _, run := range r.runs {
		if run.PolicyID == policyID {
			copyOf := *run
			result = append(result, copyOf)
		}
	}
	return result
}

func (r *Runbook) Total() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.runs)
}
