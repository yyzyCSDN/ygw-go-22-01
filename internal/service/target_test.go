package service_test

import (
	"context"
	"testing"
	"time"

	"example.com/backupmesh/internal/model"
	"example.com/backupmesh/internal/replication"
	"example.com/backupmesh/internal/service"
)

func TestReplicationOutcomesTrackSuccess(t *testing.T) {
	svc := service.New(64, model.SystemClock{})
	if !svc.RegisterReplicationDestination(replication.Destination{ID: "dest", Region: "east", Capacity: 1 << 20, Healthy: true}) {
		t.Fatal("register destination")
	}
	handle, err := svc.BeginCapture(context.Background(), "snap7", "owner", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.StageChunk(handle, 0, []byte("payload")); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.CommitCapture(handle, "", true, nil); err != nil {
		t.Fatal(err)
	}
	plan, err := svc.PlanReplication("snap7")
	if err != nil {
		t.Fatal(err)
	}
	svc.CompleteReplication(plan, true)
	outcomes := svc.ReplicationOutcomes()
	if got := len(outcomes); got != len(plan.Destinations) {
		t.Fatalf("outcomes = %d, want %d", got, len(plan.Destinations))
	}
	for _, outcome := range outcomes {
		if !outcome.Succeeded {
			t.Fatal("successful outcome misrecorded as failure")
		}
	}
}
