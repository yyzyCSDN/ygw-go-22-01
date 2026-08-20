package service_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"example.com/backupmesh/internal/model"
	"example.com/backupmesh/internal/replication"
	"example.com/backupmesh/internal/service"
)

func TestReplicationBudgetUsedConcurrentSafe(t *testing.T) {
	svc := service.New(64, model.SystemClock{})
	if !svc.RegisterReplicationDestination(replication.Destination{ID: "dest", Region: "east", Capacity: 1 << 20, Healthy: true}) {
		t.Fatal("register destination")
	}
	handle, err := svc.BeginCapture(context.Background(), "snap5", "owner", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.StageChunk(handle, 0, []byte("payload")); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.CommitCapture(handle, "", true, nil); err != nil {
		t.Fatal(err)
	}
	plan, err := svc.PlanReplication("snap5")
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = svc.ReplicationBudgetUsed()
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		svc.CompleteReplication(plan, true)
	}()
	wg.Wait()
}
