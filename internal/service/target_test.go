package service_test

import (
	"context"
	"testing"
	"time"

	"example.com/backupmesh/internal/model"
	"example.com/backupmesh/internal/replication"
	"example.com/backupmesh/internal/service"
)

func TestReplicationDuplicatePlanDoesNotDoubleCharge(t *testing.T) {
	svc := service.New(64, model.SystemClock{})
	if !svc.RegisterReplicationDestination(replication.Destination{ID: "dest", Region: "east", Capacity: 1 << 20, Healthy: true}) {
		t.Fatal("register destination")
	}
	payload := []byte("hello-world")
	handle, err := svc.BeginCapture(context.Background(), "snap2", "owner", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.StageChunk(handle, 0, payload); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.CommitCapture(handle, "", true, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.PlanReplication("snap2"); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.PlanReplication("snap2"); err == nil {
		t.Fatal("expected duplicate plan to be rejected")
	}
	if got := svc.ReplicationBudgetUsed(); got != int64(len(payload)) {
		t.Fatalf("budget used = %d, want %d", got, len(payload))
	}
}
