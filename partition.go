package worker

import (
	"sync"
	"time"
)

type ShardStatus struct {
	ID            string
	ParentShardId string
	Checkpoint    string
	AssignedTo    string
	Mux           *sync.Mutex
	LeaseTimeout  time.Time
	// Shard Range
	StartingSequenceNumber string
	// child shard doesn't have end sequence number
	EndingSequenceNumber string
}

func (ss *ShardStatus) GetLeaseOwner() string {
	ss.Mux.Lock()
	defer ss.Mux.Unlock()
	return ss.AssignedTo
}

func (ss *ShardStatus) SetLeaseOwner(owner string) {
	ss.Mux.Lock()
	defer ss.Mux.Unlock()
	ss.AssignedTo = owner
}
