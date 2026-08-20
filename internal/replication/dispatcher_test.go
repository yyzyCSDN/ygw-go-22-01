package replication

import (
	"testing"
	"time"

	"example.com/backupmesh/internal/audit"
	"example.com/backupmesh/internal/journal"
	"example.com/backupmesh/internal/model"
)

func fixedClock(now time.Time) func() time.Time { return func() time.Time { return now } }

// TestPlanSnapshotReleasesBudgetOnJournalFailure reproduces the reported bug:
// when the journal is closed, PlanSnapshot's planning fails at the journal
// step, but the budget reserved earlier in finishPlan must be released on the
// failure path so ReplicationBudgetUsed stays at zero.
func TestPlanSnapshotReleasesBudgetOnJournalFailure(t *testing.T) {
	now := time.Date(2026, 8, 20, 10, 0, 0, 0, time.UTC)
	registry := NewRegistry()
	registry.Register(Destination{ID: "dst-1", Region: "us-east", Capacity: 1 << 30, Healthy: true})
	j := journal.New()
	budget := NewBudget(1 << 30)
	dispatcher := NewDispatcher(registry, budget, j, audit.New(), fixedClock(now))

	snapshot := model.Snapshot{
		ID:         "snap-1",
		Generation: 1,
		State:      model.SnapshotPublished,
		Chunks:     []model.Chunk{{Index: 0, Digest: "d0", Data: []byte("alpha")}},
	}

	// Close the journal so journalPlan fails after the budget is reserved.
	j.Close()
	if _, err := dispatcher.PlanSnapshot(snapshot); err == nil {
		t.Fatalf("expected PlanSnapshot to fail when journal is closed")
	}

	if used := dispatcher.BudgetUsed(); used != 0 {
		t.Fatalf("budget used = %d, want 0 after failed planning (reservation not released)", used)
	}
}

// TestPlanSnapshotReservesBudgetOnSuccess confirms the happy path still
// reserves the snapshot bytes so a passing case cannot regress silently.
func TestPlanSnapshotReservesBudgetOnSuccess(t *testing.T) {
	now := time.Date(2026, 8, 20, 10, 0, 0, 0, time.UTC)
	registry := NewRegistry()
	registry.Register(Destination{ID: "dst-1", Region: "us-east", Capacity: 1 << 30, Healthy: true})
	j := journal.New()
	budget := NewBudget(1 << 30)
	dispatcher := NewDispatcher(registry, budget, j, audit.New(), fixedClock(now))

	snapshot := model.Snapshot{
		ID:         "snap-1",
		Generation: 1,
		State:      model.SnapshotPublished,
		Chunks:     []model.Chunk{{Index: 0, Digest: "d0", Data: []byte("alpha")}},
	}

	plan, err := dispatcher.PlanSnapshot(snapshot)
	if err != nil {
		t.Fatalf("expected planning to succeed, got %v", err)
	}
	if used := dispatcher.BudgetUsed(); used != plan.TotalBytes {
		t.Fatalf("budget used = %d, want %d on successful planning", used, plan.TotalBytes)
	}
}
