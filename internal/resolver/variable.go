package resolver

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Variable represents a workspace variable.
type Variable struct {
	Name     string
	Value    string
	IsSecret bool
}

// VariableResolver resolves workspace variables.
type VariableResolver struct {
	pool  *pgxpool.Pool
	cache *variableCache
}

// NewVariableResolver creates a new variable resolver.
func NewVariableResolver(pool *pgxpool.Pool) *VariableResolver {
	return &VariableResolver{
		pool:  pool,
		cache: newVariableCache(),
	}
}

// Resolve resolves a single variable.
func (r *VariableResolver) Resolve(ctx context.Context, namespaceID, name string) (string, error) {
	// Check cache
	if value, found := r.cache.get(namespaceID, name); found {
		return value, nil
	}

	query := `
		SELECT value FROM variables
		WHERE namespace_id = $1 AND name = $2
	`

	var value string
	err := r.pool.QueryRow(ctx, query, namespaceID, name).Scan(&value)
	if err != nil {
		return "", fmt.Errorf("variable not found: %s", name)
	}

	r.cache.set(namespaceID, name, value)
	return value, nil
}

// ResolveAll resolves all variables for a namespace.
func (r *VariableResolver) ResolveAll(ctx context.Context, namespaceID string) (map[string]string, error) {
	// Check cache for full namespace
	if vars := r.cache.getAll(namespaceID); vars != nil {
		return vars, nil
	}

	query := `
		SELECT name, value FROM variables
		WHERE namespace_id = $1
	`

	rows, err := r.pool.Query(ctx, query, namespaceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]string)
	for rows.Next() {
		var name, value string
		if err := rows.Scan(&name, &value); err != nil {
			return nil, err
		}
		result[name] = value
	}

	r.cache.setAll(namespaceID, result)
	return result, nil
}

// Interpolate replaces {{variable}} placeholders with actual values.
func (r *VariableResolver) Interpolate(ctx context.Context, namespaceID, template string) (string, error) {
	if !strings.Contains(template, "{{") {
		return template, nil
	}

	vars, err := r.ResolveAll(ctx, namespaceID)
	if err != nil {
		return "", err
	}

	result := template
	for name, value := range vars {
		placeholder := "{{" + name + "}}"
		result = strings.ReplaceAll(result, placeholder, value)
	}

	return result, nil
}

// InterpolateJSON interpolates variables in a JSON structure.
func (r *VariableResolver) InterpolateJSON(ctx context.Context, namespaceID string, data json.RawMessage) (json.RawMessage, error) {
	str, err := r.Interpolate(ctx, namespaceID, string(data))
	if err != nil {
		return nil, err
	}
	return json.RawMessage(str), nil
}

// InvalidateCache invalidates the variable cache for a namespace.
func (r *VariableResolver) InvalidateCache(namespaceID string) {
	r.cache.clear(namespaceID)
}

// variableCache caches variables.
type variableCache struct {
	items map[string]map[string]string // namespace -> name -> value
	mu    sync.RWMutex
}

func newVariableCache() *variableCache {
	return &variableCache{
		items: make(map[string]map[string]string),
	}
}

func (c *variableCache) get(namespace, name string) (string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if vars, exists := c.items[namespace]; exists {
		if value, found := vars[name]; found {
			return value, true
		}
	}
	return "", false
}

func (c *variableCache) getAll(namespace string) map[string]string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if vars, exists := c.items[namespace]; exists {
		// Return a copy
		result := make(map[string]string, len(vars))
		for k, v := range vars {
			result[k] = v
		}
		return result
	}
	return nil
}

func (c *variableCache) set(namespace, name, value string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.items[namespace] == nil {
		c.items[namespace] = make(map[string]string)
	}
	c.items[namespace][name] = value
}

func (c *variableCache) setAll(namespace string, vars map[string]string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.items[namespace] = make(map[string]string, len(vars))
	for k, v := range vars {
		c.items[namespace][k] = v
	}
}

func (c *variableCache) clear(namespace string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.items, namespace)
}
