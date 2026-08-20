package replication_test

import (
	"testing"
	"time"

	"example.com/backupmesh/internal/audit"
	"example.com/backupmesh/internal/journal"
	"example.com/backupmesh/internal/model"
	"example.com/backupmesh/internal/replication"
)

var testClock = func() time.Time { return time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC) }

func newTestDispatcher(budgetLimit int64) (*replication.Dispatcher, *replication.Budget) {
	registry := replication.NewRegistry()
	registry.Register(replication.Destination{ID: "dest-a", Region: "us", Capacity: 1 << 20, Healthy: true})
	budget := replication.NewBudget(budgetLimit)
	d := replication.NewDispatcher(registry, budget, journal.New(), audit.New(), testClock)
	return d, budget
}

func publishedSnapshot(id string) model.Snapshot {
	return model.Snapshot{
		ID:         id,
		Generation: 1,
		State:      model.SnapshotPublished,
		Chunks:     []model.Chunk{{Index: 0, Digest: "d0", Data: make([]byte, 100)}},
	}
}

// Re-planning the same snapshot while the first plan is still in flight must be
// rejected and must not charge the budget a second time.
func TestPlanSnapshotDuplicateIsBlocked(t *testing.T) {
	d, budget := newTestDispatcher(1 << 30)
	snap := publishedSnapshot("snap-1")

	first, err := d.PlanSnapshot(snap)
	if err != nil {
		t.Fatalf("first plan failed: %v", err)
	}
	if budget.Used() != first.TotalBytes {
		t.Fatalf("after first plan: used=%d want=%d", budget.Used(), first.TotalBytes)
	}

	if _, err := d.PlanSnapshot(snap); err == nil {
		t.Fatal("duplicate plan was accepted instead of being blocked")
	}

	if budget.Used() != first.TotalBytes {
		t.Fatalf("duplicate plan changed budget: used=%d want=%d (quota consumed twice)", budget.Used(), first.TotalBytes)
	}
}

// The chunk-based Plan entry point shares the same in-flight guard.
func TestPlanDuplicateIsBlocked(t *testing.T) {
	d, budget := newTestDispatcher(1 << 30)
	chunks := []replication.ChunkPlan{{Index: 0, Digest: "d0", Bytes: 100}}

	first, err := d.Plan("snap-1", 1, chunks)
	if err != nil {
		t.Fatalf("first plan failed: %v", err)
	}
	if _, err := d.Plan("snap-1", 1, chunks); err == nil {
		t.Fatal("duplicate plan was accepted instead of being blocked")
	}
	if budget.Used() != first.TotalBytes {
		t.Fatalf("duplicate plan changed budget: used=%d want=%d", budget.Used(), first.TotalBytes)
	}
}

// The budget itself must be the hard guarantor: reserving the same plan twice
// never doubles the used counter, regardless of what the caller does.
func TestBudgetReserveDoesNotDoubleCount(t *testing.T) {
	b := replication.NewBudget(1 << 30)
	if !b.Reserve("rep-snap-1-1", 100) {
		t.Fatal("first reserve failed")
	}
	if b.Reserve("rep-snap-1-1", 100) {
		t.Fatal("duplicate reserve must be rejected")
	}
	if b.Used() != 100 {
		t.Fatalf("used=%d want=100 (same data must not consume quota twice)", b.Used())
	}
	if !b.Reserved("rep-snap-1-1") {
		t.Fatal("Reserved should report the active reservation")
	}
	if b.Reserved("rep-snap-1-2") {
		t.Fatal("Reserved should not report an unreserved plan")
	}
}

// Completing a plan releases its reservation so the quota is freed again.
func TestCompleteReleasesBudget(t *testing.T) {
	d, budget := newTestDispatcher(1 << 30)
	plan, err := d.PlanSnapshot(publishedSnapshot("snap-1"))
	if err != nil {
		t.Fatal(err)
	}
	if budget.Used() != plan.TotalBytes {
		t.Fatalf("used=%d want=%d", budget.Used(), plan.TotalBytes)
	}
	d.Complete(plan, true)
	if budget.Used() != 0 {
		t.Fatalf("after complete: used=%d want=0", budget.Used())
	}
	if budget.Reserved(plan.ID) {
		t.Fatal("Reserved should be false after the plan completed")
	}
}
