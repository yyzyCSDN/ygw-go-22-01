package service_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"example.com/backupmesh/internal/model"
	"example.com/backupmesh/internal/replication"
	"example.com/backupmesh/internal/service"
)

func capture(t *testing.T, svc *service.Service, id string, payload []byte) {
	t.Helper()
	handle, err := svc.BeginCapture(context.Background(), id, "owner", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	for start := 0; start < len(payload); start += 16 {
		end := start + 16
		if end > len(payload) {
			end = len(payload)
		}
		if err := svc.StageChunk(handle, start/16, payload[start:end]); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := svc.CommitCapture(handle, "", true, nil); err != nil {
		t.Fatal(err)
	}
}

func TestReplicationRejectsUndersizedDestination(t *testing.T) {
	svc := service.New(64, model.SystemClock{})
	if !svc.RegisterReplicationDestination(replication.Destination{ID: "small", Region: "east", Capacity: 64, Healthy: true}) {
		t.Fatal("register destination")
	}
	capture(t, svc, "snap1", bytes.Repeat([]byte("x"), 200))
	if _, err := svc.PlanReplication("snap1"); err == nil {
		t.Fatal("expected undersized destination to be rejected")
	}
}
