package replication

import (
	"sort"
	"sync"
	"time"
)

type Destination struct {
	ID        string
	Region    string
	Capacity  int64
	Healthy   bool
	UpdatedAt time.Time
}

type Registry struct {
	mu           sync.RWMutex
	destinations map[string]Destination
}

func NewRegistry() *Registry {
	return &Registry{destinations: make(map[string]Destination)}
}

func (r *Registry) Register(destination Destination) bool {
	if r == nil || destination.ID == "" || destination.Capacity <= 0 {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if destination.UpdatedAt.IsZero() {
		destination.UpdatedAt = time.Now().UTC()
	}
	r.destinations[destination.ID] = destination
	return true
}

func (r *Registry) MarkHealth(id string, healthy bool, now time.Time) bool {
	if r == nil {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	destination, ok := r.destinations[id]
	if !ok {
		return false
	}
	destination.Healthy = healthy
	destination.UpdatedAt = now.UTC()
	r.destinations[id] = destination
	return true
}

func (r *Registry) Eligible(required int64) []Destination {
	if r == nil || required <= 0 {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]Destination, 0, len(r.destinations))
	for _, destination := range r.destinations {
		if destination.Healthy && destination.Capacity >= required {
			result = append(result, destination)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Region == result[j].Region {
			return result[i].ID < result[j].ID
		}
		return result[i].Region < result[j].Region
	})
	return result
}

func (r *Registry) Snapshot() []Destination {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]Destination, 0, len(r.destinations))
	for _, destination := range r.destinations {
		result = append(result, destination)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result
}
