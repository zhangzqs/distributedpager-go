package distributedpager

import (
	"context"
	"errors"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCachedDataSource(t *testing.T) {
	callCount := atomic.Int32{}

	source := DataSourceFunc[int](func(ctx context.Context, cursor Cursor, limit int) (ListResult[int], error) {
		callCount.Add(1)
		return ListResult[int]{
			Items:      []int{1, 2, 3},
			NextCursor: "next",
			HasMore:    true,
		}, nil
	})

	cached := NewCachedDataSource(source, 1*time.Second)

	// First call - should hit the source
	result1, err := cached.List(context.Background(), "", 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result1.Items) != 3 {
		t.Errorf("expected 3 items, got %d", len(result1.Items))
	}
	if callCount.Load() != 1 {
		t.Errorf("expected 1 call to source, got %d", callCount.Load())
	}

	// Second call with same cursor - should use cache
	result2, err := cached.List(context.Background(), "", 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result2.Items) != 3 {
		t.Errorf("expected 3 items, got %d", len(result2.Items))
	}
	if callCount.Load() != 1 {
		t.Errorf("expected 1 call to source (cached), got %d", callCount.Load())
	}

	// Wait for cache to expire
	time.Sleep(1100 * time.Millisecond)

	// Third call - cache expired, should hit source again
	result3, err := cached.List(context.Background(), "", 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result3.Items) != 3 {
		t.Errorf("expected 3 items, got %d", len(result3.Items))
	}
	if callCount.Load() != 2 {
		t.Errorf("expected 2 calls to source (cache expired), got %d", callCount.Load())
	}
}

func TestCachedDataSourceClearCache(t *testing.T) {
	callCount := atomic.Int32{}

	source := DataSourceFunc[int](func(ctx context.Context, cursor Cursor, limit int) (ListResult[int], error) {
		callCount.Add(1)
		return ListResult[int]{Items: []int{1, 2, 3}, HasMore: false}, nil
	})

	cached := NewCachedDataSource(source, 10*time.Second)

	// First call
	_, err := cached.List(context.Background(), "", 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Clear cache
	cached.ClearCache()

	// Second call - should hit source again since cache was cleared
	_, err = cached.List(context.Background(), "", 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if callCount.Load() != 2 {
		t.Errorf("expected 2 calls to source, got %d", callCount.Load())
	}
}

func TestCachedDataSourceEvictExpired(t *testing.T) {
	source := DataSourceFunc[int](func(ctx context.Context, cursor Cursor, limit int) (ListResult[int], error) {
		return ListResult[int]{Items: []int{1, 2, 3}, HasMore: false}, nil
	})

	cached := NewCachedDataSource(source, 100*time.Millisecond)

	// Add entry to cache
	_, _ = cached.List(context.Background(), "", 10)

	// Wait for expiration
	time.Sleep(150 * time.Millisecond)

	// Evict expired entries
	cached.EvictExpired()

	// Check cache is empty using public method
	cacheLen := cached.CacheSize()
	if cacheLen != 0 {
		t.Errorf("expected empty cache after eviction, got %d entries", cacheLen)
	}
}

func TestCachedDataSourceDefaultTTL(t *testing.T) {
	source := DataSourceFunc[int](func(ctx context.Context, cursor Cursor, limit int) (ListResult[int], error) {
		return ListResult[int]{Items: []int{}, HasMore: false}, nil
	})

	cached := NewCachedDataSource(source, 0)
	if cached.ttl != 5*time.Minute {
		t.Errorf("expected default TTL of 5 minutes, got %v", cached.ttl)
	}
}

func TestRateLimitedDataSource(t *testing.T) {
	var mu sync.Mutex
	callTimes := make([]time.Time, 0)

	source := DataSourceFunc[int](func(ctx context.Context, cursor Cursor, limit int) (ListResult[int], error) {
		mu.Lock()
		callTimes = append(callTimes, time.Now())
		mu.Unlock()
		return ListResult[int]{Items: []int{1, 2, 3}, HasMore: false}, nil
	})

	// 2 requests per second, burst of 2
	rateLimited := NewRateLimitedDataSource(source, 2.0, 2)

	// Make 3 calls quickly - first 2 should be immediate (burst), 3rd should be delayed
	start := time.Now()
	for i := 0; i < 3; i++ {
		_, err := rateLimited.List(context.Background(), "", 10)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	elapsed := time.Since(start)

	// Should take at least 400ms (third request waits for token refill at 2/sec = 500ms per token)
	if elapsed < 400*time.Millisecond {
		t.Errorf("expected at least 400ms elapsed, got %v", elapsed)
	}
}

func TestRateLimitedDataSourceContextCancellation(t *testing.T) {
	source := DataSourceFunc[int](func(ctx context.Context, cursor Cursor, limit int) (ListResult[int], error) {
		return ListResult[int]{Items: []int{1, 2, 3}, HasMore: false}, nil
	})

	// Very low rate limit
	rateLimited := NewRateLimitedDataSource(source, 0.1, 1)

	// Exhaust the bucket
	_, _ = rateLimited.List(context.Background(), "", 10)

	// Try to make another call with cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := rateLimited.List(ctx, "", 10)
	if err == nil {
		t.Error("expected error for cancelled context")
	}
}

func TestRateLimitedDataSourceDefaultValues(t *testing.T) {
	source := DataSourceFunc[int](func(ctx context.Context, cursor Cursor, limit int) (ListResult[int], error) {
		return ListResult[int]{Items: []int{}, HasMore: false}, nil
	})

	// Test default rate
	rateLimited := NewRateLimitedDataSource(source, 0, 0)
	if rateLimited.refillRate != 10 {
		t.Errorf("expected default rate of 10, got %f", rateLimited.refillRate)
	}
	if rateLimited.maxTokens != 10 {
		t.Errorf("expected default burst of 10, got %f", rateLimited.maxTokens)
	}
}

func TestRetryDataSource(t *testing.T) {
	callCount := atomic.Int32{}

	source := DataSourceFunc[int](func(ctx context.Context, cursor Cursor, limit int) (ListResult[int], error) {
		count := callCount.Add(1)
		if count < 3 {
			return ListResult[int]{}, errors.New("temporary error")
		}
		return ListResult[int]{Items: []int{1, 2, 3}, HasMore: false}, nil
	})

	retry := NewRetryDataSource(source, 3, 10*time.Millisecond)

	result, err := retry.List(context.Background(), "", 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result.Items) != 3 {
		t.Errorf("expected 3 items, got %d", len(result.Items))
	}

	if callCount.Load() != 3 {
		t.Errorf("expected 3 calls (2 retries), got %d", callCount.Load())
	}
}

func TestRetryDataSourceMaxRetriesExceeded(t *testing.T) {
	source := DataSourceFunc[int](func(ctx context.Context, cursor Cursor, limit int) (ListResult[int], error) {
		return ListResult[int]{}, errors.New("persistent error")
	})

	retry := NewRetryDataSource(source, 2, 10*time.Millisecond)

	_, err := retry.List(context.Background(), "", 10)
	if err == nil {
		t.Error("expected error after max retries")
	}
}

func TestRetryDataSourceContextCancellation(t *testing.T) {
	callCount := atomic.Int32{}

	source := DataSourceFunc[int](func(ctx context.Context, cursor Cursor, limit int) (ListResult[int], error) {
		callCount.Add(1)
		return ListResult[int]{}, errors.New("error")
	})

	retry := NewRetryDataSource(source, 5, 100*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err := retry.List(ctx, "", 10)
	if err == nil {
		t.Error("expected error for cancelled context")
	}

	// Should not retry all 5 times due to context cancellation
	if callCount.Load() > 2 {
		t.Logf("call count: %d (context cancelled early)", callCount.Load())
	}
}

func TestRetryDataSourceDefaultValues(t *testing.T) {
	source := DataSourceFunc[int](func(ctx context.Context, cursor Cursor, limit int) (ListResult[int], error) {
		return ListResult[int]{}, nil
	})

	retry := NewRetryDataSource(source, -1, 0)
	if retry.maxRetries != 3 {
		t.Errorf("expected default maxRetries of 3, got %d", retry.maxRetries)
	}
	if retry.initialWait != 100*time.Millisecond {
		t.Errorf("expected default initialWait of 100ms, got %v", retry.initialWait)
	}
}

func TestLoggingDataSource(t *testing.T) {
	source := DataSourceFunc[int](func(ctx context.Context, cursor Cursor, limit int) (ListResult[int], error) {
		return ListResult[int]{Items: []int{1, 2, 3}, HasMore: true}, nil
	})

	// Create a custom logger that writes to a buffer
	var buf []byte
	logger := log.New(&testWriter{buf: &buf}, "[TEST] ", 0)

	logging := NewLoggingDataSource(source, logger)

	_, err := logging.List(context.Background(), "test-cursor", 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Just verify it doesn't crash - actual log output is hard to test
}

func TestLoggingDataSourceWithError(t *testing.T) {
	source := DataSourceFunc[int](func(ctx context.Context, cursor Cursor, limit int) (ListResult[int], error) {
		return ListResult[int]{}, errors.New("test error")
	})

	logger := log.New(os.Stderr, "[TEST] ", 0)
	logging := NewLoggingDataSource(source, logger)

	_, err := logging.List(context.Background(), "", 10)
	if err == nil {
		t.Error("expected error to be propagated")
	}
}

func TestLoggingDataSourceDefaultLogger(t *testing.T) {
	source := DataSourceFunc[int](func(ctx context.Context, cursor Cursor, limit int) (ListResult[int], error) {
		return ListResult[int]{Items: []int{}, HasMore: false}, nil
	})

	logging := NewLoggingDataSource(source, nil)
	if logging.logger == nil {
		t.Error("expected default logger to be set")
	}
}

func TestTransformDataSource(t *testing.T) {
	source := DataSourceFunc[int](func(ctx context.Context, cursor Cursor, limit int) (ListResult[int], error) {
		return ListResult[int]{
			Items:      []int{1, 2, 3, 4, 5},
			NextCursor: "next",
			HasMore:    true,
		}, nil
	})

	// Transform int to string
	transform := NewTransformDataSource(source, func(n int) string {
		return string(rune('A' + n - 1))
	})

	result, err := transform.List(context.Background(), "", 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := []string{"A", "B", "C", "D", "E"}
	if len(result.Items) != len(expected) {
		t.Fatalf("expected %d items, got %d", len(expected), len(result.Items))
	}

	for i, item := range result.Items {
		if item != expected[i] {
			t.Errorf("item %d: expected %s, got %s", i, expected[i], item)
		}
	}

	if result.NextCursor != "next" {
		t.Errorf("expected cursor 'next', got %s", result.NextCursor)
	}
	if !result.HasMore {
		t.Error("expected HasMore to be true")
	}
}

func TestTransformDataSourceWithStructs(t *testing.T) {
	type User struct {
		ID   int
		Name string
	}

	type UserDTO struct {
		UserID   int
		Username string
	}

	source := DataSourceFunc[User](func(ctx context.Context, cursor Cursor, limit int) (ListResult[User], error) {
		return ListResult[User]{
			Items: []User{
				{ID: 1, Name: "Alice"},
				{ID: 2, Name: "Bob"},
			},
			HasMore: false,
		}, nil
	})

	// Transform User to UserDTO
	transform := NewTransformDataSource(source, func(u User) UserDTO {
		return UserDTO{
			UserID:   u.ID,
			Username: u.Name,
		}
	})

	result, err := transform.List(context.Background(), "", 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result.Items) != 2 {
		t.Fatalf("expected 2 items, got %d", len(result.Items))
	}

	if result.Items[0].UserID != 1 || result.Items[0].Username != "Alice" {
		t.Errorf("unexpected first item: %+v", result.Items[0])
	}
	if result.Items[1].UserID != 2 || result.Items[1].Username != "Bob" {
		t.Errorf("unexpected second item: %+v", result.Items[1])
	}
}

func TestTransformDataSourceError(t *testing.T) {
	source := DataSourceFunc[int](func(ctx context.Context, cursor Cursor, limit int) (ListResult[int], error) {
		return ListResult[int]{}, errors.New("source error")
	})

	transform := NewTransformDataSource(source, func(n int) string {
		return ""
	})

	_, err := transform.List(context.Background(), "", 10)
	if err == nil {
		t.Error("expected error to be propagated")
	}
}

// Benchmark tests
func BenchmarkCachedDataSource(b *testing.B) {
	source := DataSourceFunc[int](func(ctx context.Context, cursor Cursor, limit int) (ListResult[int], error) {
		time.Sleep(1 * time.Millisecond) // Simulate latency
		return ListResult[int]{Items: []int{1, 2, 3}, HasMore: false}, nil
	})

	cached := NewCachedDataSource(source, 1*time.Minute)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = cached.List(context.Background(), "", 10)
	}
}

func BenchmarkRateLimitedDataSource(b *testing.B) {
	source := DataSourceFunc[int](func(ctx context.Context, cursor Cursor, limit int) (ListResult[int], error) {
		return ListResult[int]{Items: []int{1, 2, 3}, HasMore: false}, nil
	})

	rateLimited := NewRateLimitedDataSource(source, 1000, 100)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = rateLimited.List(context.Background(), "", 10)
	}
}

func BenchmarkTransformDataSource(b *testing.B) {
	source := DataSourceFunc[int](func(ctx context.Context, cursor Cursor, limit int) (ListResult[int], error) {
		items := make([]int, 100)
		for i := 0; i < 100; i++ {
			items[i] = i
		}
		return ListResult[int]{Items: items, HasMore: false}, nil
	})

	transform := NewTransformDataSource(source, func(n int) int {
		return n * 2
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = transform.List(context.Background(), "", 100)
	}
}

// testWriter is a simple io.Writer for testing logging
type testWriter struct {
	buf *[]byte
}

func (w *testWriter) Write(p []byte) (n int, err error) {
	*w.buf = append(*w.buf, p...)
	return len(p), nil
}
