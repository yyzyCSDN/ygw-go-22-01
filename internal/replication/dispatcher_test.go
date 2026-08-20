package replication

import (
	"testing"
	"time"

	"example.com/backupmesh/internal/audit"
)

func TestDispatcherCompleteIdempotent(t *testing.T) {
	now := time.Date(2026, 8, 20, 10, 0, 0, 0, time.UTC)
	ledger := audit.New()
	clock := func() time.Time { return now }
	d := NewDispatcher(nil, NewBudget(1<<30), nil, ledger, clock)

	plan := Plan{
		ID:         "rep-snap-1-1",
		SnapshotID: "snap-1",
		Generation: 1,
		Destinations: []Destination{
			{ID: "dest-a", Region: "r1", Capacity: 1024, Healthy: true},
		},
		TotalBytes: 100,
	}

	// First completion records the outcome, releases the budget and
	// writes an audit entry.
	d.Complete(plan, true)
	// The same plan completed a second time must be ignored so the result
	// window does not end up with duplicate records.
	d.Complete(plan, true)

	outcomes := d.Outcomes()
	if len(outcomes) != 1 {
		t.Fatalf("expected 1 outcome, got %d", len(outcomes))
	}

	completedEvents := 0
	for _, event := range ledger.EventsFor("snap-1") {
		if event.Kind == "replication-completed" {
			completedEvents++
		}
	}
	if completedEvents != 1 {
		t.Fatalf("expected 1 audit record, got %d", completedEvents)
	}

	if _, count := d.Status(); count != 1 {
		t.Fatalf("expected completed count 1, got %d", count)
	}
}

func TestDispatcherCompleteDistinctPlansNotCollapsed(t *testing.T) {
	now := time.Date(2026, 8, 20, 10, 0, 0, 0, time.UTC)
	ledger := audit.New()
	clock := func() time.Time { return now }
	d := NewDispatcher(nil, NewBudget(1<<30), nil, ledger, clock)

	destination := Destination{ID: "dest-a", Region: "r1", Capacity: 1024, Healthy: true}
	first := Plan{ID: "rep-snap-1-1", SnapshotID: "snap-1", Generation: 1, Destinations: []Destination{destination}, TotalBytes: 100}
	second := Plan{ID: "rep-snap-2-1", SnapshotID: "snap-2", Generation: 1, Destinations: []Destination{destination}, TotalBytes: 100}

	d.Complete(first, true)
	d.Complete(second, false)

	if len(d.Outcomes()) != 2 {
		t.Fatalf("expected 2 outcomes, got %d", len(d.Outcomes()))
	}
	if _, count := d.Status(); count != 2 {
		t.Fatalf("expected completed count 2, got %d", count)
	}
}
