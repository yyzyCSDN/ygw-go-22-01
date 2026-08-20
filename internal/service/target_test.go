package service_test

import (
	"context"
	"testing"
	"time"

	"example.com/backupmesh/internal/model"
	"example.com/backupmesh/internal/service"
)

func TestCommitCaptureFailureObservedAsFailure(t *testing.T) {
	svc := service.New(64, model.SystemClock{})
	handle, err := svc.BeginCapture(context.Background(), "snap10", "owner", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.StageChunk(handle, 0, []byte("payload")); err != nil {
		t.Fatal(err)
	}
	svc.CloseJournal()
	if _, err := svc.CommitCapture(handle, "", true, nil); err == nil {
		t.Fatal("expected journal failure")
	}
	observed := false
	for _, event := range svc.TelemetryEvents() {
		if event.Name == "capture.commit" && event.Snapshot == "snap10" && !event.Succeeded {
			observed = true
		}
	}
	if !observed {
		t.Fatal("capture failure not observed in telemetry")
	}
}
