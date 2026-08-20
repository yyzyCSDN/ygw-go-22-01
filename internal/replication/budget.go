package replication

import "sync"

type Budget struct {
	mu           sync.Mutex
	limit        int64
	reservations map[string]int64
	used         int64
}

func NewBudget(limit int64) *Budget {
	if limit < 0 {
		limit = 0
	}
	return &Budget{limit: limit, reservations: make(map[string]int64)}
}

func (b *Budget) Reserve(planID string, bytes int64) bool {
	if b == nil || planID == "" || bytes <= 0 {
		return false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, exists := b.reservations[planID]; exists {
		return false
	}
	if b.used+bytes > b.limit {
		return false
	}
	b.reservations[planID] = bytes
	b.used += bytes
	return true
}

func (b *Budget) HasReservation(planID string) bool {
	if b == nil || planID == "" {
		return false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	_, exists := b.reservations[planID]
	return exists
}

func (b *Budget) Snapshot() (used, limit int64, reserved int) {
	if b == nil {
		return 0, 0, 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.used, b.limit, len(b.reservations)
}

func (b *Budget) Release(planID string) int64 {
	if b == nil {
		return 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	bytes := b.reservations[planID]
	if bytes == 0 {
		return 0
	}
	delete(b.reservations, planID)
	b.used -= bytes
	if b.used < 0 {
		b.used = 0
	}
	return bytes
}

func (b *Budget) Used() int64 {
	if b == nil {
		return 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.used
}

func (b *Budget) Limit() int64 {
	if b == nil {
		return 0
	}
	return b.limit
}
