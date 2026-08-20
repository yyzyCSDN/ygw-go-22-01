package orchestrator

import (
	"fmt"
	"time"

	"example.com/backupmesh/internal/audit"
	"example.com/backupmesh/internal/journal"
)

type Controller struct {
	runbook *Runbook
	window  *Window
	journal *journal.Log
	audit   *audit.Ledger
	clock   func() time.Time
}

func NewController(runbook *Runbook, journalLog *journal.Log, ledger *audit.Ledger, clock func() time.Time) *Controller {
	if runbook == nil {
		runbook = NewRunbook(nil)
	}
	if clock == nil {
		clock = time.Now
	}
	return &Controller{runbook: runbook, window: NewWindow(10*time.Minute, clock), journal: journalLog, audit: ledger, clock: clock}
}

func (c *Controller) Execute(policy Policy, snapshotID string) error {
	if !policy.Enabled {
		return ErrDisabled
	}
	if !policy.Allows(c.clock().UTC()) {
		return ErrOutsideWindow
	}
	started := c.clock().UTC()
	run, err := c.runbook.Start(policy.ID, started)
	if err != nil {
		return err
	}
	succeeded := false
	defer func() {
		now := c.clock().UTC()
		_ = c.runbook.Finish(run.ID, succeeded, now)
		c.window.Add(Outcome{PolicyID: policy.ID, Succeeded: succeeded, At: now})
	}()
	if c.journal != nil {
		entry := journal.Entry{Generation: run.Generation, Kind: "orchestration-run", SnapshotID: snapshotID, Operation: run.ID, Detail: policy.ID, RecordedAt: started}
		if _, err := c.journal.Append(entry); err != nil {
			return err
		}
	}
	if c.audit != nil {
		c.audit.Record("orchestration-run", snapshotID, run.ID, started)
	}
	succeeded = true
	return nil
}

func (c *Controller) Outcomes() []Outcome {
	if c == nil {
		return nil
	}
	return c.window.Snapshot()
}

func (c *Controller) Runbook() *Runbook { return c.runbook }

func (c *Controller) Scheduled(policy Policy, last *Run, now time.Time) (time.Time, error) {
	return NewScheduler(c.clock).Next(policy, last, now)
}

func (c *Controller) FailureRate() float64 {
	if c == nil {
		return 0
	}
	return c.window.FailureRate()
}

func (c *Controller) Describe(run *Run) string {
	if run == nil {
		return "run=<nil>"
	}
	return fmt.Sprintf("run=%s policy=%s gen=%d success=%t", run.ID, run.PolicyID, run.Generation, run.Succeeded)
}
