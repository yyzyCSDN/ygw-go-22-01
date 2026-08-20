package replication

import (
	"fmt"
	"sort"
	"strings"

	"example.com/backupmesh/internal/model"
)

type ChunkPlan struct {
	Index  int
	Digest string
	Bytes  int
}

type Plan struct {
	ID            string
	SnapshotID    string
	Generation    uint64
	Destinations  []Destination
	Chunks        []ChunkPlan
	TotalBytes    int64
	CreatedAtUnix int64
}

type Planner struct{}

func NewPlanner() *Planner { return &Planner{} }

func planableSnapshotState(snapshot model.Snapshot) (string, error) {
	switch snapshot.State {
	case model.SnapshotExpired:
		return string(snapshot.State), fmt.Errorf("replication: snapshot %s is expired and not planable", snapshot.ID)
	case model.SnapshotStaged, model.SnapshotPublished, model.SnapshotVerified:
		return string(snapshot.State), nil
	default:
		return string(snapshot.State), fmt.Errorf("replication: snapshot %s has unknown state %s", snapshot.ID, snapshot.State)
	}
}

func validatePlanableSnapshot(snapshot model.Snapshot) error {
	if snapshot.ID == "" {
		return fmt.Errorf("replication: snapshot id is empty")
	}
	if len(snapshot.Chunks) == 0 {
		return fmt.Errorf("replication: snapshot has no chunks")
	}
	if _, err := planableSnapshotState(snapshot); err != nil {
		return err
	}
	return nil
}

func validateDestinationCapacity(destinations []Destination, total int64) error {
	var undersized []string
	for _, destination := range destinations {
		if destination.Capacity < total {
			undersized = append(undersized, fmt.Sprintf("%s(capacity=%d)", destination.ID, destination.Capacity))
		}
	}
	if len(undersized) > 0 {
		return fmt.Errorf("replication: destinations lack capacity for %d bytes: %s", total, strings.Join(undersized, ", "))
	}
	return nil
}

func (p *Planner) Build(snapshot model.Snapshot, destinations []Destination, createdAtUnix int64) (Plan, error) {
	if err := validatePlanableSnapshot(snapshot); err != nil {
		return Plan{}, err
	}
	if len(destinations) == 0 {
		return Plan{}, fmt.Errorf("replication: no eligible destinations")
	}
	chunks := make([]ChunkPlan, 0, len(snapshot.Chunks))
	seenIndexes := make(map[int]struct{}, len(snapshot.Chunks))
	var total int64
	for _, chunk := range snapshot.Chunks {
		if chunk.Index < 0 || chunk.Digest == "" {
			return Plan{}, fmt.Errorf("replication: invalid chunk %d", chunk.Index)
		}
		if _, exists := seenIndexes[chunk.Index]; exists {
			return Plan{}, fmt.Errorf("replication: duplicate chunk index %d", chunk.Index)
		}
		seenIndexes[chunk.Index] = struct{}{}
		item := ChunkPlan{Index: chunk.Index, Digest: chunk.Digest, Bytes: len(chunk.Data)}
		chunks = append(chunks, item)
		total += int64(item.Bytes)
	}
	sort.Slice(chunks, func(i, j int) bool { return chunks[i].Index < chunks[j].Index })
	sort.Slice(destinations, func(i, j int) bool { return destinations[i].ID < destinations[j].ID })
	if err := validateDestinationCapacity(destinations, total); err != nil {
		return Plan{}, err
	}
	return Plan{ID: fmt.Sprintf("rep-%s-%d", snapshot.ID, snapshot.Generation), SnapshotID: snapshot.ID, Generation: snapshot.Generation, Destinations: append([]Destination(nil), destinations...), Chunks: chunks, TotalBytes: total, CreatedAtUnix: createdAtUnix}, nil
}
