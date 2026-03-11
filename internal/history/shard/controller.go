package shard

import (
	"errors"
	"sync"

	"github.com/linkflow/engine/internal/history/types"
)

var (
	ErrShardNotOwned = errors.New("shard not owned by this host")
	ErrShardNotFound = errors.New("shard not found")
)

type Shard interface {
	GetID() int32
}

type ShardImpl struct {
	id int32
	// Add other shard-specific fields here, e.g., locking, status
}

func (s *ShardImpl) GetID() int32 {
	return s.id
}

type Controller struct {
	numShards int32
	shards    map[int32]Shard
	mu        sync.RWMutex
	status    int32 // 0: stopped, 1: starting, 2: running, 3: stopping
}

const (
	statusStopped  = 0
	statusStarting = 1
	statusRunning  = 2
	statusStopping = 3
)

func NewController(numShards int32) *Controller {
	if numShards <= 0 {
		numShards = 16 // Default
	}
	return &Controller{
		numShards: numShards,
		shards:    make(map[int32]Shard),
		status:    statusStopped,
	}
}

func (c *Controller) Start() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.status == statusRunning {
		return nil
	}

	c.status = statusStarting
	// Initialize shards
	for i := int32(0); i < c.numShards; i++ {
		c.shards[i] = &ShardImpl{id: i}
	}
	c.status = statusRunning
	return nil
}

func (c *Controller) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.status == statusStopped {
		return
	}
	c.status = statusStopping
	// Cleanup logic
	c.status = statusStopped
}

func (c *Controller) GetShardForExecution(key types.ExecutionKey) (Shard, error) {
	shardID := c.GetShardIDForExecution(key)

	c.mu.RLock()
	shard, ok := c.shards[shardID]
	c.mu.RUnlock()

	if !ok {
		// In a real system, we might try to acquire ownership here.
		// For now, assuming static assignment or initialization in Start()
		return nil, ErrShardNotFound
	}

	return shard, nil
}

func (c *Controller) GetShardIDForExecution(key types.ExecutionKey) int32 {
	// Simple hash-based sharding
	data := key.NamespaceID + "/" + key.WorkflowID
	var hash uint32
	for i := 0; i < len(data); i++ {
		hash = 31*hash + uint32(data[i])
	}
	return int32(hash % uint32(c.numShards))
}

func (c *Controller) isShardOwned(shardID int32) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, ok := c.shards[shardID]
	return ok
}
