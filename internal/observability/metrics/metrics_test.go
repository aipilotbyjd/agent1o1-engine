package metrics

import (
	"sync"
	"testing"
)

func TestMakeKey_Consistency(t *testing.T) {
	labels := Labels{
		"service": "matching",
		"method":  "AddTask",
		"region":  "us-east",
	}

	// Multiple calls should produce the same key
	key1 := makeKey("requests_total", labels)
	key2 := makeKey("requests_total", labels)

	if key1 != key2 {
		t.Errorf("makeKey should be consistent: got %q and %q", key1, key2)
	}
}

func TestMakeKey_DifferentLabelOrder(t *testing.T) {
	// Even with maps (which iterate in random order), keys should be consistent
	labels1 := Labels{"a": "1", "b": "2", "c": "3"}
	labels2 := Labels{"c": "3", "a": "1", "b": "2"}

	key1 := makeKey("metric", labels1)
	key2 := makeKey("metric", labels2)

	if key1 != key2 {
		t.Errorf("makeKey should produce same key regardless of insertion order: got %q and %q", key1, key2)
	}
}

func TestMakeKey_EmptyLabels(t *testing.T) {
	key := makeKey("metric", Labels{})
	if key != "metric" {
		t.Errorf("makeKey with empty labels = %q, want %q", key, "metric")
	}
}

func TestCounter_Operations(t *testing.T) {
	c := NewCounter("test_counter", nil)

	if c.Value() != 0 {
		t.Errorf("Initial value = %d, want 0", c.Value())
	}

	c.Inc()
	if c.Value() != 1 {
		t.Errorf("After Inc = %d, want 1", c.Value())
	}

	c.Add(5)
	if c.Value() != 6 {
		t.Errorf("After Add(5) = %d, want 6", c.Value())
	}
}

func TestCounter_Concurrent(t *testing.T) {
	c := NewCounter("test_counter", nil)

	var wg sync.WaitGroup
	iterations := 1000

	for i := 0; i < iterations; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.Inc()
		}()
	}

	wg.Wait()

	if c.Value() != int64(iterations) {
		t.Errorf("After concurrent Inc = %d, want %d", c.Value(), iterations)
	}
}

func TestGauge_Operations(t *testing.T) {
	g := NewGauge("test_gauge", nil)

	if g.Value() != 0 {
		t.Errorf("Initial value = %f, want 0", g.Value())
	}

	g.Set(42.5)
	if g.Value() != 42.5 {
		t.Errorf("After Set(42.5) = %f, want 42.5", g.Value())
	}

	g.Inc()
	if g.Value() != 43.5 {
		t.Errorf("After Inc = %f, want 43.5", g.Value())
	}

	g.Dec()
	if g.Value() != 42.5 {
		t.Errorf("After Dec = %f, want 42.5", g.Value())
	}

	g.Add(7.5)
	if g.Value() != 50 {
		t.Errorf("After Add(7.5) = %f, want 50", g.Value())
	}
}

func TestGauge_FloatPrecision(t *testing.T) {
	g := NewGauge("test_gauge", nil)

	// Test that float values are stored correctly
	g.Set(0.123456789)
	if g.Value() != 0.123456789 {
		t.Errorf("Float precision lost: got %f, want 0.123456789", g.Value())
	}
}

func TestGauge_Concurrent(t *testing.T) {
	g := NewGauge("test_gauge", nil)

	var wg sync.WaitGroup
	iterations := 1000

	for i := 0; i < iterations; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			g.Inc()
		}()
		go func() {
			defer wg.Done()
			g.Dec()
		}()
	}

	wg.Wait()

	// After equal Inc/Dec, should be back to 0
	if g.Value() != 0 {
		t.Errorf("After concurrent Inc/Dec = %f, want 0", g.Value())
	}
}

func TestHistogram_Observe(t *testing.T) {
	h := NewHistogram("test_histogram", nil, nil)

	h.Observe(10)
	h.Observe(50)
	h.Observe(100)

	if h.Count() != 3 {
		t.Errorf("Count = %d, want 3", h.Count())
	}

	expectedSum := 10.0 + 50.0 + 100.0
	if h.Sum() != expectedSum {
		t.Errorf("Sum = %f, want %f", h.Sum(), expectedSum)
	}
}

func TestRegistry_GetOrCreate(t *testing.T) {
	r := NewRegistry()

	labels := Labels{"method": "test"}

	// First call creates
	c1 := r.Counter("requests", labels)
	c1.Inc()

	// Second call returns same counter
	c2 := r.Counter("requests", labels)

	if c2.Value() != 1 {
		t.Errorf("Registry should return same counter, got value %d", c2.Value())
	}
}

func TestRegistry_DifferentLabels(t *testing.T) {
	r := NewRegistry()

	c1 := r.Counter("requests", Labels{"method": "get"})
	c2 := r.Counter("requests", Labels{"method": "post"})

	c1.Inc()
	c2.Add(5)

	if c1.Value() != 1 {
		t.Errorf("c1.Value() = %d, want 1", c1.Value())
	}
	if c2.Value() != 5 {
		t.Errorf("c2.Value() = %d, want 5", c2.Value())
	}
}
