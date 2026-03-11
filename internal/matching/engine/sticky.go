package engine

import (
	"sync"
	"time"
)

// StickyAffinity tracks the mapping between workflow IDs and worker identities
// for sticky task queues, allowing workflow tasks to be pinned to specific workers.
type StickyAffinity struct {
	affinityMap map[string]affinityRecord // workflowID -> record
	mu          sync.Mutex
}

type affinityRecord struct {
	identity string
	lastSeen time.Time
}

// NewStickyAffinity creates a new StickyAffinity tracker.
func NewStickyAffinity() *StickyAffinity {
	return &StickyAffinity{
		affinityMap: make(map[string]affinityRecord),
	}
}

// Bind sets the affinity for a workflow to a specific worker identity.
func (sa *StickyAffinity) Bind(workflowID, identity string) {
	sa.mu.Lock()
	defer sa.mu.Unlock()

	sa.affinityMap[workflowID] = affinityRecord{
		identity: identity,
		lastSeen: time.Now(),
	}
}

// GetIdentity returns the worker identity bound to a workflow, if any.
func (sa *StickyAffinity) GetIdentity(workflowID string) (string, bool) {
	sa.mu.Lock()
	defer sa.mu.Unlock()

	rec, ok := sa.affinityMap[workflowID]
	if !ok {
		return "", false
	}
	return rec.identity, true
}

// IsExpired returns true if the affinity for a workflow has exceeded the given timeout.
func (sa *StickyAffinity) IsExpired(workflowID string, timeout time.Duration) bool {
	sa.mu.Lock()
	defer sa.mu.Unlock()

	rec, ok := sa.affinityMap[workflowID]
	if !ok {
		return true
	}
	return time.Since(rec.lastSeen) > timeout
}

// Touch updates the last-seen time for a workflow affinity.
func (sa *StickyAffinity) Touch(workflowID string) {
	sa.mu.Lock()
	defer sa.mu.Unlock()

	if rec, ok := sa.affinityMap[workflowID]; ok {
		rec.lastSeen = time.Now()
		sa.affinityMap[workflowID] = rec
	}
}

// Remove removes the affinity for a workflow.
func (sa *StickyAffinity) Remove(workflowID string) {
	sa.mu.Lock()
	defer sa.mu.Unlock()
	delete(sa.affinityMap, workflowID)
}
