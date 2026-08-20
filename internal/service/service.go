package service

import (
	"sync"
	"sync/atomic"
	"time"

	"example.com/backupmesh/internal/audit"
	"example.com/backupmesh/internal/catalog"
	"example.com/backupmesh/internal/checkpoint"
	"example.com/backupmesh/internal/journal"
	"example.com/backupmesh/internal/lease"
	"example.com/backupmesh/internal/manifest"
	"example.com/backupmesh/internal/model"
	"example.com/backupmesh/internal/orchestrator"
	"example.com/backupmesh/internal/policy"
	"example.com/backupmesh/internal/replication"
	"example.com/backupmesh/internal/restore"
	"example.com/backupmesh/internal/retry"
	"example.com/backupmesh/internal/telemetry"
	"example.com/backupmesh/internal/verify"
)

type Service struct {
	catalog               *catalog.Store
	journal               *journal.Log
	leases                *lease.Store
	retries               *retry.Queue
	cursors               *restore.CursorStore
	verifications         *verify.Store
	checkpoints           *checkpoint.Store
	manifests             *manifest.Store
	policies              *policy.Engine
	audit                 *audit.Ledger
	telemetry             *telemetry.Registry
	replication           *replication.Dispatcher
	destinations          *replication.Registry
	orchestration         *orchestrator.Controller
	policiesMu            sync.RWMutex
	orchestrationPolicies map[string]orchestrator.Policy
	clock                 model.Clock

	mu                sync.Mutex
	replayGenerations map[string]uint64
	generation        atomic.Uint64
}

func (s *Service) RestoreCursor(planID string) int { return s.cursors.Cursor(planID) }

func New(maxChunks int, clock model.Clock) *Service {
	if clock == nil {
		clock = model.SystemClock{}
	}
	j := journal.New()
	a := audit.New()
	destinations := replication.NewRegistry()
	s := &Service{
		catalog:               catalog.New(maxChunks),
		journal:               j,
		leases:                lease.New(),
		retries:               retry.New(),
		cursors:               restore.NewCursorStore(),
		verifications:         verify.NewStore(),
		checkpoints:           checkpoint.New(),
		manifests:             manifest.New(),
		policies:              policy.New(),
		audit:                 a,
		telemetry:             telemetry.New(64),
		destinations:          destinations,
		orchestrationPolicies: make(map[string]orchestrator.Policy),
		clock:                 clock,
		replayGenerations:     make(map[string]uint64),
	}
	s.replication = replication.NewDispatcher(destinations, replication.NewBudget(1<<30), j, a, clock.Now)
	runbook := orchestrator.NewRunbook(func(policyID string) int {
		s.policiesMu.RLock()
		policy, ok := s.orchestrationPolicies[policyID]
		s.policiesMu.RUnlock()
		if !ok {
			return 1
		}
		return policy.MaxConcurrent
	})
	s.orchestration = orchestrator.NewController(runbook, j, a, clock.Now)
	return s
}

func (s *Service) CloseJournal() { s.journal.Close() }
func (s *Service) OpenJournal()  { s.journal.Open() }

func (s *Service) Snapshot(id string) (model.Snapshot, bool) { return s.catalog.Snapshot(id) }

func (s *Service) Receipt(id string) (model.VerificationReceipt, bool) {
	return s.verifications.Receipt(id)
}

func (s *Service) RetryPending(snapshotID string) int { return s.retries.Pending(snapshotID) }

func (s *Service) JournalEntries() []journal.Entry { return s.journal.Entries() }

func (s *Service) Lease(resource string) (model.Lease, bool) { return s.leases.Lease(resource) }

func (s *Service) MaterializeManifest(snapshotID string) (manifest.Envelope, error) {
	snapshot, ok := s.catalog.Snapshot(snapshotID)
	if !ok {
		return manifest.Envelope{}, model.ErrNotFound
	}
	envelope, err := s.manifests.Publish(snapshot, s.clock.Now())
	if err != nil {
		return manifest.Envelope{}, err
	}
	if _, exists := s.checkpoints.Get(snapshot.ID); !exists {
		if _, err := s.checkpoints.Start(snapshot.ID, snapshot.Operation, snapshot.Generation, s.clock.Now()); err != nil {
			return manifest.Envelope{}, err
		}
		if _, err := s.checkpoints.Advance(snapshot.ID, snapshot.Generation, checkpoint.Staged, len(snapshot.Chunks), envelope.Digest, s.clock.Now()); err != nil {
			return manifest.Envelope{}, err
		}
		if _, err := s.checkpoints.Advance(snapshot.ID, snapshot.Generation, checkpoint.Published, len(snapshot.Chunks), envelope.Digest, s.clock.Now()); err != nil {
			return manifest.Envelope{}, err
		}
	}
	if snapshot.State == model.SnapshotVerified {
		record, _ := s.checkpoints.Get(snapshot.ID)
		if record.State == checkpoint.Published {
			if _, err := s.checkpoints.Advance(snapshot.ID, snapshot.Generation, checkpoint.Verified, len(snapshot.Chunks), envelope.Digest, s.clock.Now()); err != nil {
				return manifest.Envelope{}, err
			}
		}
	}
	s.audit.Record("manifest-published", snapshot.ID, envelope.Digest, s.clock.Now())
	return envelope, nil
}

func (s *Service) VerifyMaterializedManifest(snapshotID string) error {
	if err := s.manifests.Verify(snapshotID); err != nil {
		return err
	}
	s.audit.Record("manifest-verified", snapshotID, "payload digest matched", s.clock.Now())
	return nil
}

func (s *Service) ManifestPayload(snapshotID string) ([]byte, bool) {
	return s.manifests.Payload(snapshotID)
}

func (s *Service) Checkpoint(snapshotID string) (checkpoint.Record, bool) {
	return s.checkpoints.Get(snapshotID)
}

func (s *Service) CheckpointHistory(snapshotID string) []checkpoint.State {
	return s.checkpoints.History(snapshotID)
}

func (s *Service) AuditEvents() []audit.Event { return s.audit.Events() }
func (s *Service) VerifyAudit() error         { return s.audit.Verify() }

func (s *Service) AuditEventsFor(snapshotID string) []audit.Event {
	return s.audit.EventsFor(snapshotID)
}

func (s *Service) Health(now time.Time) telemetry.Health {
	return s.telemetry.HealthSnapshot(now)
}

func (s *Service) HealthJSON(now time.Time) ([]byte, error) {
	return telemetry.MarshalHealth(s.Health(now))
}

func (s *Service) OperationCount(name string) uint64 { return s.telemetry.Count(name) }

func (s *Service) TelemetryEvents() []telemetry.Event { return s.telemetry.Events() }

func (s *Service) RetentionPolicy() policy.Retention { return s.policies.RetentionPolicy() }
func (s *Service) RetryPolicy() policy.Retry         { return s.policies.RetryPolicy() }

func (s *Service) NextRetry(task model.RetryTask, now time.Time) (time.Time, error) {
	return s.policies.NextRetry(task, now)
}

func (s *Service) RegisterReplicationDestination(destination replication.Destination) bool {
	return s.destinations.Register(destination)
}

func (s *Service) ReplicationDestinations() []replication.Destination {
	return s.destinations.Snapshot()
}

func (s *Service) PlanReplication(snapshotID string) (replication.Plan, error) {
	snapshot, ok := s.catalog.Snapshot(snapshotID)
	if !ok {
		return replication.Plan{}, model.ErrNotFound
	}
	return s.replication.PlanSnapshot(snapshot)
}

func (s *Service) CompleteReplication(plan replication.Plan, succeeded bool) {
	s.replication.Complete(plan, succeeded)
}

func (s *Service) ReplicationBudgetUsed() int64 {
	return s.replication.BudgetUsed()
}

func (s *Service) ReplicationOutcomes() []replication.Outcome {
	return s.replication.Outcomes()
}

func (s *Service) ReplicationStatus() (int64, int) {
	return s.replication.BudgetUsed(), 0
}

func (s *Service) ReplicationOutcomeSummary() (int, int, int) {
	return s.replication.OutcomeSummary()
}

func (s *Service) RegisterOrchestrationPolicy(policy orchestrator.Policy) bool {
	if err := orchestrator.Validate(policy); err != nil {
		return false
	}
	s.policiesMu.Lock()
	s.orchestrationPolicies[policy.ID] = policy
	s.policiesMu.Unlock()
	return true
}

func (s *Service) OrchestrationPolicies() []orchestrator.Policy {
	s.policiesMu.RLock()
	defer s.policiesMu.RUnlock()
	result := make([]orchestrator.Policy, 0, len(s.orchestrationPolicies))
	for _, policy := range s.orchestrationPolicies {
		result = append(result, policy)
	}
	return result
}

func (s *Service) OrchestrationPolicy(policyID string) (orchestrator.Policy, bool) {
	s.policiesMu.RLock()
	defer s.policiesMu.RUnlock()
	policy, ok := s.orchestrationPolicies[policyID]
	return policy, ok
}

func (s *Service) NextScheduledRun(policyID string) (time.Time, error) {
	policy, ok := s.OrchestrationPolicy(policyID)
	if !ok {
		return time.Time{}, model.ErrNotFound
	}
	last, _ := s.orchestration.Runbook().Last(policyID)
	return s.orchestration.Scheduled(policy, last, s.clock.Now())
}

func (s *Service) RunScheduledJob(policyID, snapshotID string) error {
	policy, ok := s.OrchestrationPolicy(policyID)
	if !ok {
		return model.ErrNotFound
	}
	return s.orchestration.Execute(policy, snapshotID)
}

func (s *Service) OrchestrationOutcomes() []orchestrator.Outcome {
	return s.orchestration.Outcomes()
}

func (s *Service) OrchestrationRuns(policyID string) []orchestrator.Run {
	return s.orchestration.Runbook().Runs(policyID)
}

func (s *Service) OrchestrationFailureRate() float64 {
	return s.orchestration.FailureRate()
}

func (s *Service) OrchestrationRunbookTotal() int {
	return s.orchestration.Runbook().Total()
}

func (s *Service) CaptureFailureCount(name string) uint64 {
	return s.telemetry.FailureCount(name)
}
