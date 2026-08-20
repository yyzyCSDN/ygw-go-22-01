package service_test

import (
	"context"
	"testing"
	"time"

	"example.com/backupmesh/internal/model"
	"example.com/backupmesh/internal/service"
)

func TestFinishRestoreKeepsProtectionUntilDurable(t *testing.T) {
	svc := service.New(64, model.SystemClock{})
	handle, err := svc.BeginCapture(context.Background(), "snap8", "owner", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.StageChunk(handle, 0, []byte("payload")); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.CommitCapture(handle, "", true, nil); err != nil {
		t.Fatal(err)
	}
	plan, err := svc.BeginRestore("snap8", "owner", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	svc.CloseJournal()
	if err := svc.FinishRestore(plan, "owner"); err == nil {
		t.Fatal("expected journal failure")
	}
	svc.ApplyRetention(0)
	snapshot, ok := svc.Snapshot("snap8")
	if !ok {
		t.Fatal("snapshot missing")
	}
	if snapshot.State == model.SnapshotExpired {
		t.Fatal("snapshot expired while restore is pending")
	}
}
