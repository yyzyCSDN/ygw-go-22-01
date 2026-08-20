package replication

import (
	"fmt"
	"sync"
	"testing"
)

func TestBudgetReserveAndRelease(t *testing.T) {
	b := NewBudget(100)
	if got := b.Limit(); got != 100 {
		t.Fatalf("limit = %d, want 100", got)
	}
	if got := b.Used(); got != 0 {
		t.Fatalf("used = %d, want 0", got)
	}
	if !b.Reserve("p1", 40) {
		t.Fatal("reserve p1 failed")
	}
	if got := b.Used(); got != 40 {
		t.Fatalf("used = %d, want 40", got)
	}
	if b.Reserve("p1", 10) {
		t.Fatal("duplicate reserve should fail")
	}
	if b.Reserve("p2", 70) {
		t.Fatal("reserve over limit should fail")
	}
	if got := b.Used(); got != 40 {
		t.Fatalf("used = %d, want 40 after failed reserve", got)
	}
	if !b.Reserve("p2", 60) {
		t.Fatal("reserve p2 failed")
	}
	if got := b.Used(); got != 100 {
		t.Fatalf("used = %d, want 100", got)
	}
	if released := b.Release("p1"); released != 40 {
		t.Fatalf("released = %d, want 40", released)
	}
	if got := b.Used(); got != 60 {
		t.Fatalf("used = %d, want 60 after release", got)
	}
	if released := b.Release("missing"); released != 0 {
		t.Fatalf("released missing = %d, want 0", released)
	}
	if got := b.Used(); got != 60 {
		t.Fatalf("used = %d, want 60 after releasing missing", got)
	}
}

// TestBudgetConcurrentReadsAndWrites exercises the scenario where budget queries
// (Used/Limit) race against plan completion (Release) and reservation (Reserve).
// Run with -race to confirm no data race and that Used() never reports a value
// inconsistent with the reservations.
func TestBudgetConcurrentReadsAndWrites(t *testing.T) {
	const limit = int64(1 << 20)
	b := NewBudget(limit)

	// Reserve a fixed pool of plans up front so releases are well-defined.
	const plans = 64
	const planBytes = int64(1 << 10)
	for i := 0; i < plans; i++ {
		if !b.Reserve(fmt.Sprintf("init-%d", i), planBytes) {
			t.Fatalf("reserve init-%d failed", i)
		}
	}

	var wg sync.WaitGroup

	// Reader goroutine: queries the budget while writers mutate it. Bounded so it
	// terminates on its own rather than relying on a signal that would deadlock wg.Wait.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 8000; i++ {
			used := b.Used()
			limit := b.Limit()
			if used < 0 || used > limit {
				t.Errorf("inconsistent budget: used=%d limit=%d", used, limit)
				return
			}
		}
	}()

	// Writer goroutines: reserve and release plans concurrently.
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				planID := fmt.Sprintf("w-%d-%d", id, i)
				if b.Reserve(planID, planBytes) {
					b.Release(planID)
				}
			}
		}(w)
	}

	wg.Wait()

	// After all the churn settles, used must equal the reserved initial pool.
	if got, want := b.Used(), int64(plans)*planBytes; got != want {
		t.Fatalf("used = %d, want %d after concurrent churn", got, want)
	}
}
