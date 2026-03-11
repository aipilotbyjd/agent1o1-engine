package cache

import (
	"container/list"
	"context"
	"errors"
	"sync"
	"time"
)

var (
	ErrNotFound = errors.New("cache: key not found")
	ErrExpired  = errors.New("cache: key expired")
)

// Cache is the interface for cache implementations.
type Cache interface {
	Get(ctx context.Context, key string) ([]byte, error)
	Set(ctx context.Context, key string, value []byte, ttl time.Duration) error
	Delete(ctx context.Context, key string) error
	Clear(ctx context.Context) error
}

// MultiLevelCache implements L1/L2 caching.
type MultiLevelCache struct {
	l1 *LRUCache // In-process cache (fastest)
	l2 Cache     // Optional distributed cache (e.g., Redis)

	l1TTL time.Duration
	l2TTL time.Duration

	metrics *CacheMetrics
	mu      sync.RWMutex
}

// MultiLevelConfig holds multi-level cache configuration.
type MultiLevelConfig struct {
	L1MaxSize int
	L1TTL     time.Duration
	L2TTL     time.Duration
	EnableL2  bool
}

// DefaultMultiLevelConfig returns default config.
func DefaultMultiLevelConfig() MultiLevelConfig {
	return MultiLevelConfig{
		L1MaxSize: 10000,
		L1TTL:     5 * time.Minute,
		L2TTL:     30 * time.Minute,
		EnableL2:  false,
	}
}

// NewMultiLevelCache creates a new multi-level cache.
func NewMultiLevelCache(config MultiLevelConfig, l2 Cache) *MultiLevelCache {
	return &MultiLevelCache{
		l1:      NewLRUCache(config.L1MaxSize),
		l2:      l2,
		l1TTL:   config.L1TTL,
		l2TTL:   config.L2TTL,
		metrics: &CacheMetrics{},
	}
}

// Get retrieves a value from the cache.
func (c *MultiLevelCache) Get(ctx context.Context, key string) ([]byte, error) {
	// Try L1 first
	if value, err := c.l1.Get(key); err == nil {
		c.metrics.l1Hits++
		return value, nil
	}
	c.metrics.l1Misses++

	// Try L2 if available
	if c.l2 != nil {
		if value, err := c.l2.Get(ctx, key); err == nil {
			c.metrics.l2Hits++
			// Populate L1
			c.l1.Set(key, value, c.l1TTL)
			return value, nil
		}
		c.metrics.l2Misses++
	}

	return nil, ErrNotFound
}

// Set stores a value in the cache.
func (c *MultiLevelCache) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	// Set in L1
	l1TTL := ttl
	if l1TTL == 0 || l1TTL > c.l1TTL {
		l1TTL = c.l1TTL
	}
	c.l1.Set(key, value, l1TTL)

	// Set in L2 if available
	if c.l2 != nil {
		l2TTL := ttl
		if l2TTL == 0 || l2TTL > c.l2TTL {
			l2TTL = c.l2TTL
		}
		return c.l2.Set(ctx, key, value, l2TTL)
	}

	return nil
}

// Delete removes a value from the cache.
func (c *MultiLevelCache) Delete(ctx context.Context, key string) error {
	c.l1.Delete(key)
	if c.l2 != nil {
		return c.l2.Delete(ctx, key)
	}
	return nil
}

// Clear clears all cache entries.
func (c *MultiLevelCache) Clear(ctx context.Context) error {
	c.l1.Clear()
	if c.l2 != nil {
		return c.l2.Clear(ctx)
	}
	return nil
}

// GetOrSet gets a value or sets it using the loader.
func (c *MultiLevelCache) GetOrSet(ctx context.Context, key string, loader func() ([]byte, error)) ([]byte, error) {
	// Try cache first
	value, err := c.Get(ctx, key)
	if err == nil {
		return value, nil
	}

	// Load from source
	value, err = loader()
	if err != nil {
		return nil, err
	}

	// Store in cache
	c.Set(ctx, key, value, 0)
	return value, nil
}

// Metrics returns cache metrics.
func (c *MultiLevelCache) Metrics() CacheMetrics {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return *c.metrics
}

// CacheMetrics holds cache metrics.
type CacheMetrics struct {
	l1Hits   int64
	l1Misses int64
	l2Hits   int64
	l2Misses int64
}

// LRUCache is a simple LRU cache implementation.
type LRUCache struct {
	capacity int
	items    map[string]*list.Element
	order    *list.List
	mu       sync.RWMutex
}

type lruItem struct {
	key       string
	value     []byte
	expiresAt time.Time
}

// NewLRUCache creates a new LRU cache.
func NewLRUCache(capacity int) *LRUCache {
	return &LRUCache{
		capacity: capacity,
		items:    make(map[string]*list.Element),
		order:    list.New(),
	}
}

// Get retrieves a value.
func (c *LRUCache) Get(key string) ([]byte, error) {
	c.mu.RLock()
	elem, exists := c.items[key]
	c.mu.RUnlock()

	if !exists {
		return nil, ErrNotFound
	}

	item := elem.Value.(*lruItem)
	if !item.expiresAt.IsZero() && time.Now().After(item.expiresAt) {
		c.Delete(key)
		return nil, ErrExpired
	}

	// Move to front
	c.mu.Lock()
	c.order.MoveToFront(elem)
	c.mu.Unlock()

	return item.value, nil
}

// Set stores a value.
func (c *LRUCache) Set(key string, value []byte, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if exists
	if elem, exists := c.items[key]; exists {
		c.order.MoveToFront(elem)
		item := elem.Value.(*lruItem)
		item.value = value
		if ttl > 0 {
			item.expiresAt = time.Now().Add(ttl)
		} else {
			item.expiresAt = time.Time{}
		}
		return
	}

	// Evict if at capacity
	if c.order.Len() >= c.capacity {
		c.evict()
	}

	// Add new item
	item := &lruItem{
		key:   key,
		value: value,
	}
	if ttl > 0 {
		item.expiresAt = time.Now().Add(ttl)
	}

	elem := c.order.PushFront(item)
	c.items[key] = elem
}

// Delete removes a value.
func (c *LRUCache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, exists := c.items[key]; exists {
		c.order.Remove(elem)
		delete(c.items, key)
	}
}

// Clear removes all values.
func (c *LRUCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.items = make(map[string]*list.Element)
	c.order = list.New()
}

func (c *LRUCache) evict() {
	elem := c.order.Back()
	if elem != nil {
		item := elem.Value.(*lruItem)
		c.order.Remove(elem)
		delete(c.items, item.key)
	}
}

// Size returns current cache size.
func (c *LRUCache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.order.Len()
}

// InMemoryCache is a simple in-memory cache.
type InMemoryCache struct {
	items map[string]*cacheEntry
	mu    sync.RWMutex
}

type cacheEntry struct {
	value     []byte
	expiresAt time.Time
}

// NewInMemoryCache creates a new in-memory cache.
func NewInMemoryCache() *InMemoryCache {
	cache := &InMemoryCache{
		items: make(map[string]*cacheEntry),
	}
	go cache.cleanup()
	return cache
}

func (c *InMemoryCache) Get(ctx context.Context, key string) ([]byte, error) {
	c.mu.RLock()
	entry, exists := c.items[key]
	c.mu.RUnlock()

	if !exists {
		return nil, ErrNotFound
	}

	if !entry.expiresAt.IsZero() && time.Now().After(entry.expiresAt) {
		c.Delete(ctx, key)
		return nil, ErrExpired
	}

	return entry.value, nil
}

func (c *InMemoryCache) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry := &cacheEntry{value: value}
	if ttl > 0 {
		entry.expiresAt = time.Now().Add(ttl)
	}
	c.items[key] = entry
	return nil
}

func (c *InMemoryCache) Delete(ctx context.Context, key string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.items, key)
	return nil
}

func (c *InMemoryCache) Clear(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items = make(map[string]*cacheEntry)
	return nil
}

func (c *InMemoryCache) cleanup() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		c.mu.Lock()
		now := time.Now()
		for key, entry := range c.items {
			if !entry.expiresAt.IsZero() && now.After(entry.expiresAt) {
				delete(c.items, key)
			}
		}
		c.mu.Unlock()
	}
}
