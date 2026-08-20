package replication

import (
	"sort"
	"sync"
	"time"
)

type Outcome struct {
	PlanID      string
	Destination string
	Succeeded   bool
	Bytes       int64
	At          time.Time
}

type Window struct {
	mu     sync.Mutex
	length time.Duration
	clock  func() time.Time
	items  []Outcome
}

func NewWindow(length time.Duration, clock func() time.Time) *Window {
	if length <= 0 {
		length = 10 * time.Minute
	}
	if clock == nil {
		clock = time.Now
	}
	return &Window{length: length, clock: clock}
}

func (w *Window) Add(outcome Outcome) {
	if w == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.addUniqueLocked(outcome)
}

func (w *Window) addUniqueLocked(outcome Outcome) bool {
	for _, existing := range w.items {
		if existing.PlanID == outcome.PlanID && existing.Destination == outcome.Destination {
			return false
		}
	}
	if outcome.At.IsZero() {
		outcome.At = w.clock().UTC()
	}
	w.items = append(w.items, outcome)
	w.pruneLocked(w.clock())
	return true
}

func (w *Window) pruneLocked(now time.Time) {
	cutoff := now.Add(-w.length)
	first := 0
	for first < len(w.items) && w.items[first].At.Before(cutoff) {
		first++
	}
	if first > 0 {
		copy(w.items, w.items[first:])
		w.items = w.items[:len(w.items)-first]
	}
}

func (w *Window) Snapshot() []Outcome {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.pruneLocked(w.clock())
	result := append([]Outcome(nil), w.items...)
	sort.SliceStable(result, func(i, j int) bool { return result[i].At.Before(result[j].At) })
	return result
}

func (w *Window) FailureRate() float64 {
	items := w.Snapshot()
	if len(items) == 0 {
		return 0
	}
	failures := 0
	for _, item := range items {
		if !item.Succeeded {
			failures++
		}
	}
	return float64(failures) / float64(len(items))
}
