package service_test

import (
	"context"
	"testing"
	"time"

	"example.com/backupmesh/internal/model"
	"example.com/backupmesh/internal/service"
)

func TestVerifyFailureDoesNotLeaveReceipt(t *testing.T) {
	svc := service.New(64, model.SystemClock{})
	handle, err := svc.BeginCapture(context.Background(), "snap9", "owner", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.StageChunk(handle, 0, []byte("payload")); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.CommitCapture(handle, "", true, nil); err != nil {
		t.Fatal(err)
	}
	svc.CloseJournal()
	if _, err := svc.VerifySnapshot("snap9"); err == nil {
		t.Fatal("expected journal failure")
	}
	if _, ok := svc.Receipt("snap9"); ok {
		t.Fatal("receipt must not exist after failed verification")
	}
}
