package main

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"example.com/backupmesh/internal/model"
	"example.com/backupmesh/internal/orchestrator"
	"example.com/backupmesh/internal/replication"
	servicepkg "example.com/backupmesh/internal/service"
	"example.com/backupmesh/replsnap"
)

func main() {
	store := replsnap.NewMemoryStore()
	service := replsnap.NewService(store)
	snapshot, err := service.Capture(context.Background(), replsnap.SnapshotMeta{ID: "demo"}, bytes.NewReader([]byte("replica snapshot")), 8)
	if err != nil {
		fmt.Printf("capture err=%v\n", err)
		return
	}
	receipt, err := service.Promote("demo", 3, []string{"replica-a", "replica-b"})
	fmt.Printf("snapshot=%s chunks=%d root=%s promoted=%v err=%v\n", snapshot.Meta.ID, len(snapshot.Chunks), snapshot.Manifest.MerkleRoot[:12], receipt.Promoted, err)

	ops := servicepkg.New(64, model.SystemClock{})
	handle, err := ops.BeginCapture(context.Background(), "demo-orchestrated", "demo", time.Minute)
	if err != nil {
		fmt.Printf("begin orchestrated capture err=%v\n", err)
		return
	}
	if err := ops.StageChunk(handle, 0, []byte("replica snapshot")); err != nil {
		fmt.Printf("stage orchestrated capture err=%v\n", err)
		return
	}
	if _, err := ops.CommitCapture(handle, "", true, map[string]string{"source": "demo"}); err != nil {
		fmt.Printf("commit orchestrated capture err=%v\n", err)
		return
	}
	ops.RegisterReplicationDestination(replication.Destination{ID: "region-east", Region: "east", Capacity: 1 << 20, Healthy: true})
	plan, err := ops.PlanReplication("demo-orchestrated")
	if err != nil {
		fmt.Printf("plan replication err=%v\n", err)
		return
	}
	ops.CompleteReplication(plan, true)
	ops.RegisterOrchestrationPolicy(orchestrator.Policy{ID: "nightly", Interval: time.Hour, Enabled: true, AllowedStart: 0, AllowedEnd: 24 * time.Hour, MaxConcurrent: 2})
	if err := ops.RunScheduledJob("nightly", "demo-orchestrated"); err != nil {
		fmt.Printf("scheduled run err=%v\n", err)
		return
	}
	envelope, err := ops.MaterializeManifest("demo-orchestrated")
	if err != nil {
		fmt.Printf("materialize manifest err=%v\n", err)
		return
	}
	fmt.Printf("manifest=%s version=%d replication=%s outcomes=%d orchestration_runs=%d audit=%d\n", envelope.Digest[:12], envelope.Version, plan.ID, len(ops.ReplicationOutcomes()), ops.OrchestrationRunbookTotal(), len(ops.AuditEvents()))
}
