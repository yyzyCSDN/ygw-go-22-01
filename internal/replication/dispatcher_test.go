package replication

import (
	"strings"
	"testing"
	"time"

	"example.com/backupmesh/internal/model"
)

func fixedNow() time.Time { return time.Date(2026, 8, 20, 10, 0, 0, 0, time.UTC) }

func newSnapshot(id string, state model.SnapshotState, data []byte) model.Snapshot {
	return model.Snapshot{
		ID:         id,
		Generation: 1,
		State:      state,
		Chunks: []model.Chunk{{
			Index:  1,
			Digest: "sha256:" + id,
			Data:   data,
		}},
	}
}

// TestPlanSnapshotRejectsOversizedSnapshot reproduces the capacity-validation
// hole: a destination registered with capacity 64 must not be selected for a
// 200-byte snapshot. Planning must be rejected outright rather than yielding a
// plan the destination quota cannot honor.
func TestPlanSnapshotRejectsOversizedSnapshot(t *testing.T) {
	registry := NewRegistry()
	registry.Register(Destination{ID: "dst-small", Region: "us-east", Capacity: 64, Healthy: true, UpdatedAt: fixedNow()})

	dispatcher := NewDispatcher(registry, NewBudget(1<<30), nil, nil, fixedNow)

	snapshot := newSnapshot("snap-oversized", model.SnapshotPublished, make([]byte, 200))
	plan, err := dispatcher.PlanSnapshot(snapshot)
	if err == nil {
		t.Fatalf("expected planning to be rejected for a snapshot that exceeds destination capacity, got plan %s with %d bytes", plan.ID, plan.TotalBytes)
	}
	if !strings.Contains(err.Error(), "no destination capacity") {
		t.Fatalf("expected a no-destination-capacity error, got %q", err)
	}
	if plan.ID != "" {
		t.Fatalf("expected empty plan on rejection, got %s", plan.ID)
	}
}

// TestPlanSnapshotAdmitsFittingSnapshot is the positive control: when at least
// one healthy destination has capacity greater than or equal to the snapshot
// size, planning succeeds.
func TestPlanSnapshotAdmitsFittingSnapshot(t *testing.T) {
	registry := NewRegistry()
	registry.Register(Destination{ID: "dst-large", Region: "us-east", Capacity: 256, Healthy: true, UpdatedAt: fixedNow()})

	dispatcher := NewDispatcher(registry, NewBudget(1<<30), nil, nil, fixedNow)

	snapshot := newSnapshot("snap-fitting", model.SnapshotPublished, make([]byte, 200))
	plan, err := dispatcher.PlanSnapshot(snapshot)
	if err != nil {
		t.Fatalf("expected planning to succeed for a fitting snapshot, got %v", err)
	}
	if plan.TotalBytes != 200 {
		t.Fatalf("expected total bytes 200, got %d", plan.TotalBytes)
	}
	if len(plan.Destinations) != 1 || plan.Destinations[0].ID != "dst-large" {
		t.Fatalf("expected plan to target dst-large, got %+v", plan.Destinations)
	}
}
