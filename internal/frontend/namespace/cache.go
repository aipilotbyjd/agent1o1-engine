package namespace

import (
	"errors"
	"sync"
)

var (
	ErrNamespaceNotFound = errors.New("namespace not found")
)

type Namespace struct {
	ID            int64
	Name          string
	RetentionDays int
	Config        map[string]interface{}
}

type Cache struct {
	namespaces   map[string]*Namespace
	namespaceIDs map[int64]*Namespace
	mu           sync.RWMutex
}

func NewCache() *Cache {
	return &Cache{
		namespaces:   make(map[string]*Namespace),
		namespaceIDs: make(map[int64]*Namespace),
	}
}

func (c *Cache) Get(name string) (*Namespace, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	ns, ok := c.namespaces[name]
	if !ok {
		return nil, ErrNamespaceNotFound
	}
	return ns, nil
}

func (c *Cache) GetByID(id int64) (*Namespace, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	ns, ok := c.namespaceIDs[id]
	if !ok {
		return nil, ErrNamespaceNotFound
	}
	return ns, nil
}

func (c *Cache) GetByName(name string) (*Namespace, error) {
	return c.Get(name)
}

func (c *Cache) Put(ns *Namespace) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.namespaces[ns.Name] = ns
	c.namespaceIDs[ns.ID] = ns
}

func (c *Cache) Invalidate(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if ns, ok := c.namespaces[name]; ok {
		delete(c.namespaceIDs, ns.ID)
		delete(c.namespaces, name)
	}
}

func (c *Cache) InvalidateByID(id int64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if ns, ok := c.namespaceIDs[id]; ok {
		delete(c.namespaces, ns.Name)
		delete(c.namespaceIDs, id)
	}
}

func (c *Cache) List() []*Namespace {
	c.mu.RLock()
	defer c.mu.RUnlock()

	result := make([]*Namespace, 0, len(c.namespaces))
	for _, ns := range c.namespaces {
		result = append(result, ns)
	}
	return result
}
