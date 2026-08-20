package replication

import (
	"fmt"
	"sync"
	"time"

	"example.com/backupmesh/internal/audit"
	"example.com/backupmesh/internal/journal"
	"example.com/backupmesh/internal/model"
)

type Dispatcher struct {
	registry  *Registry
	planner   *Planner
	budget    *Budget
	window    *Window
	journal   *journal.Log
	audit     *audit.Ledger
	clock     func() time.Time
	mu        sync.Mutex
	completed map[string]bool
}

func NewDispatcher(registry *Registry, budget *Budget, journalLog *journal.Log, ledger *audit.Ledger, clock func() time.Time) *Dispatcher {
	if registry == nil {
		registry = NewRegistry()
	}
	if budget == nil {
		budget = NewBudget(1 << 30)
	}
	if clock == nil {
		clock = time.Now
	}
	return &Dispatcher{registry: registry, planner: NewPlanner(), budget: budget, window: NewWindow(10*time.Minute, clock), journal: journalLog, audit: ledger, clock: clock, completed: make(map[string]bool)}
}

func (d *Dispatcher) Plan(snapshotID string, generation uint64, chunks []ChunkPlan) (Plan, error) {
	if d == nil || snapshotID == "" || len(chunks) == 0 {
		return Plan{}, fmt.Errorf("replication: incomplete dispatch request")
	}
	destinations, total, err := d.eligibleForChunks(chunks)
	if err != nil {
		return Plan{}, err
	}
	if len(destinations) == 0 {
		return Plan{}, fmt.Errorf("replication: no destination capacity")
	}
	plan := Plan{ID: fmt.Sprintf("rep-%s-%d", snapshotID, generation), SnapshotID: snapshotID, Generation: generation, Destinations: destinations, Chunks: append([]ChunkPlan(nil), chunks...), TotalBytes: total, CreatedAtUnix: d.clock().Unix()}
	return plan, d.finishPlan(plan)
}

func (d *Dispatcher) PlanSnapshot(snapshot model.Snapshot) (Plan, error) {
	if d == nil {
		return Plan{}, fmt.Errorf("replication: dispatcher unavailable")
	}
	if err := d.planGuard(snapshot); err != nil {
		return Plan{}, err
	}
	if err := validatePlanableSnapshot(snapshot); err != nil {
		return Plan{}, err
	}
	total, err := totalSnapshotBytes(snapshot.Chunks)
	if err != nil {
		return Plan{}, err
	}
	destinations := d.registry.Eligible(total)
	plan, err := d.planner.Build(snapshot, destinations, d.clock().Unix())
	if err != nil {
		return Plan{}, err
	}
	return plan, d.finishPlan(plan)
}

func (d *Dispatcher) planGuard(snapshot model.Snapshot) error {
	if _, err := planableSnapshotState(snapshot); err != nil {
		return err
	}
	total, err := totalSnapshotBytes(snapshot.Chunks)
	if err != nil {
		return err
	}
	if total <= 0 {
		return fmt.Errorf("replication: snapshot %s has no bytes to replicate", snapshot.ID)
	}
	return nil
}

func (d *Dispatcher) reservePlan(plan Plan) error {
	if d.budget.HasReservation(plan.ID) {
		if d.audit != nil {
			d.audit.Record("replication-quota-rejected", plan.SnapshotID, plan.ID, d.clock())
		}
		return fmt.Errorf("replication: plan %s already reserved", plan.ID)
	}
	if !d.budget.Reserve(plan.ID, plan.TotalBytes) {
		if d.audit != nil {
			d.audit.Record("replication-quota-rejected", plan.SnapshotID, plan.ID, d.clock())
		}
		return fmt.Errorf("replication: quota exhausted for plan %s", plan.ID)
	}
	return nil
}

func (d *Dispatcher) journalPlan(plan Plan) error {
	if d.journal == nil {
		return nil
	}
	if d.journal.Closed() {
		d.budget.Release(plan.ID)
		reason := "journal-closed"
		if d.audit != nil {
			d.audit.Record("replication-plan-rolled-back", plan.SnapshotID, fmt.Sprintf("plan=%s reason=%s", plan.ID, reason), d.clock())
		}
		return model.ErrJournalClosed
	}
	entry := journal.Entry{Generation: plan.Generation, Kind: "replication-plan", SnapshotID: plan.SnapshotID, Operation: plan.ID, Detail: fmt.Sprintf("destinations=%d bytes=%d", len(plan.Destinations), plan.TotalBytes), RecordedAt: d.clock()}
	if _, err := d.journal.Append(entry); err != nil {
		d.budget.Release(plan.ID)
		if d.audit != nil {
			d.audit.Record("replication-plan-rolled-back", plan.SnapshotID, plan.ID, d.clock())
		}
		return err
	}
	return nil
}

func (d *Dispatcher) finishPlan(plan Plan) error {
	if err := d.reservePlan(plan); err != nil {
		return err
	}
	if err := d.journalPlan(plan); err != nil {
		return err
	}
	if d.audit != nil {
		d.audit.Record("replication-planned", plan.SnapshotID, plan.ID, d.clock())
	}
	return nil
}

func (d *Dispatcher) markCompleted(planID string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.completed[planID] {
		return false
	}
	d.completed[planID] = true
	return true
}

func (d *Dispatcher) Complete(plan Plan, succeeded bool) {
	if d == nil {
		return
	}
	if !d.markCompleted(plan.ID) {
		return
	}
	d.budget.Release(plan.ID)
	now := d.clock().UTC()
	for _, destination := range plan.Destinations {
		d.window.Add(Outcome{PlanID: plan.ID, Destination: destination.ID, Succeeded: succeeded, Bytes: plan.TotalBytes, At: now})
	}
	if d.audit != nil {
		d.audit.Record("replication-completed", plan.SnapshotID, fmt.Sprintf("plan=%s success=%t", plan.ID, succeeded), now)
	}
}

func (d *Dispatcher) Status() (used int64, completed int) {
	used, _, _ = d.budget.Snapshot()
	d.mu.Lock()
	completed = len(d.completed)
	d.mu.Unlock()
	return used, completed
}

func (d *Dispatcher) OutcomeSummary() (total, succeeded, failed int) {
	for _, outcome := range d.window.Snapshot() {
		total++
		if outcome.Succeeded {
			succeeded++
		} else {
			failed++
		}
	}
	return total, succeeded, failed
}

func (d *Dispatcher) eligibleForChunks(chunks []ChunkPlan) ([]Destination, int64, error) {
	total, err := totalChunkBytes(chunks)
	if err != nil {
		return nil, 0, err
	}
	destinations := d.registry.Eligible(total)
	return destinations, total, nil
}

func totalChunkBytes(chunks []ChunkPlan) (int64, error) {
	var total int64
	for _, chunk := range chunks {
		if chunk.Index < 0 || chunk.Digest == "" {
			return 0, fmt.Errorf("replication: invalid chunk %d", chunk.Index)
		}
		total += int64(chunk.Bytes)
	}
	return total, nil
}

func totalSnapshotBytes(chunks []model.Chunk) (int64, error) {
	var total int64
	for _, chunk := range chunks {
		if chunk.Index < 0 || chunk.Digest == "" {
			return 0, fmt.Errorf("replication: invalid chunk %d", chunk.Index)
		}
		total += int64(len(chunk.Data))
	}
	return total, nil
}

func (d *Dispatcher) BudgetUsed() int64   { return d.budget.Used() }
func (d *Dispatcher) Outcomes() []Outcome { return d.window.Snapshot() }
