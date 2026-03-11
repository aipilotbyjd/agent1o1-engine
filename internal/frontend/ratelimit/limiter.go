package ratelimit

import (
	"sync"
	"time"

	"golang.org/x/time/rate"
)

type Limiter struct {
	limiters     map[string]*rate.Limiter
	global       *rate.Limiter
	mu           sync.RWMutex
	defaultRate  rate.Limit
	defaultBurst int
}

type Config struct {
	GlobalRPS      float64
	GlobalBurst    int
	NamespaceRPS   float64
	NamespaceBurst int
}

func DefaultConfig() Config {
	return Config{
		GlobalRPS:      1000,
		GlobalBurst:    2000,
		NamespaceRPS:   100,
		NamespaceBurst: 200,
	}
}

func NewLimiter(cfg Config) *Limiter {
	return &Limiter{
		limiters:     make(map[string]*rate.Limiter),
		global:       rate.NewLimiter(rate.Limit(cfg.GlobalRPS), cfg.GlobalBurst),
		defaultRate:  rate.Limit(cfg.NamespaceRPS),
		defaultBurst: cfg.NamespaceBurst,
	}
}

func (l *Limiter) Allow(namespace string) bool {
	if !l.global.Allow() {
		return false
	}

	nsLimiter := l.getOrCreateNamespaceLimiter(namespace)
	return nsLimiter.Allow()
}

func (l *Limiter) AllowN(namespace string, n int) bool {
	now := time.Now()
	if !l.global.AllowN(now, n) {
		return false
	}

	nsLimiter := l.getOrCreateNamespaceLimiter(namespace)
	return nsLimiter.AllowN(now, n)
}

func (l *Limiter) getOrCreateNamespaceLimiter(namespace string) *rate.Limiter {
	l.mu.RLock()
	limiter, ok := l.limiters[namespace]
	l.mu.RUnlock()

	if ok {
		return limiter
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if limiter, ok = l.limiters[namespace]; ok {
		return limiter
	}

	limiter = rate.NewLimiter(l.defaultRate, l.defaultBurst)
	l.limiters[namespace] = limiter
	return limiter
}

func (l *Limiter) SetNamespaceLimit(namespace string, rps float64, burst int) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.limiters[namespace] = rate.NewLimiter(rate.Limit(rps), burst)
}

func (l *Limiter) RemoveNamespace(namespace string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	delete(l.limiters, namespace)
}
