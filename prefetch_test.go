package distributedpager

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

func TestPrefetchDataSource(t *testing.T) {
	callCount := atomic.Int32{}

	// Create a slow data source to simulate network latency
	slowSource := DataSourceFunc[int](func(ctx context.Context, cursor Cursor, limit int) (ListResult[int], error) {
		callCount.Add(1)
		time.Sleep(10 * time.Millisecond) // Simulate latency

		start := 0
		if cursor != "" {
			idx, _ := strconv.Atoi(cursor)
			start = idx
		}

		items := make([]int, 0, limit)
		for i := 0; i < limit && start+i < 100; i++ {
			items = append(items, start+i)
		}

		hasMore := start+len(items) < 100
		var nextCursor Cursor
		if hasMore {
			nextCursor = strconv.Itoa(start + len(items))
		}

		return ListResult[int]{Items: items, NextCursor: nextCursor, HasMore: hasMore}, nil
	})

	// Wrap with prefetch
	prefetchSource := NewPrefetchDataSource(slowSource, 10)

	// First call - no prefetch available
	result1, err := prefetchSource.List(context.Background(), "", 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result1.Items) != 10 {
		t.Errorf("expected 10 items, got %d", len(result1.Items))
	}

	// Wait a bit for prefetch to complete
	time.Sleep(50 * time.Millisecond)

	// Second call - should use prefetched data (fast)
	start := time.Now()
	result2, err := prefetchSource.List(context.Background(), result1.NextCursor, 10)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result2.Items) != 10 {
		t.Errorf("expected 10 items, got %d", len(result2.Items))
	}

	// Second call should be much faster due to prefetch
	if elapsed > 5*time.Millisecond {
		t.Logf("second call took %v (prefetch may not have completed)", elapsed)
	}
}

func TestPrefetchDataSourceClearCache(t *testing.T) {
	source := DataSourceFunc[int](func(ctx context.Context, cursor Cursor, limit int) (ListResult[int], error) {
		return ListResult[int]{
			Items:      []int{1, 2, 3},
			NextCursor: "next",
			HasMore:    true,
		}, nil
	})

	prefetchSource := NewPrefetchDataSource(source, 10)

	// Trigger a fetch to start prefetching
	_, err := prefetchSource.List(context.Background(), "", 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Wait for prefetch to start
	time.Sleep(10 * time.Millisecond)

	// Clear the cache
	prefetchSource.ClearCache()

	// Verify cache is empty
	prefetchSource.mu.Lock()
	cacheLen := len(prefetchSource.cache)
	prefetchSource.mu.Unlock()

	if cacheLen != 0 {
		t.Errorf("expected empty cache, got %d entries", cacheLen)
	}
}

func TestPrefetchDataSourceContextCancellation(t *testing.T) {
	source := DataSourceFunc[int](func(ctx context.Context, cursor Cursor, limit int) (ListResult[int], error) {
		time.Sleep(100 * time.Millisecond) // Long operation
		return ListResult[int]{Items: []int{1, 2, 3}, HasMore: false}, nil
	})

	prefetchSource := NewPrefetchDataSource(source, 10)

	// Create a cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Should fail quickly due to cancelled context
	_, err := prefetchSource.List(ctx, "", 10)
	if err == nil {
		t.Error("expected error for cancelled context")
	}
}

func TestBufferedDataSource(t *testing.T) {
	source := DataSourceFunc[int](func(ctx context.Context, cursor Cursor, limit int) (ListResult[int], error) {
		items := make([]int, limit)
		for i := 0; i < limit; i++ {
			items[i] = i + 1
		}
		return ListResult[int]{Items: items, HasMore: false}, nil
	})

	bufferedSource := NewBufferedDataSource(source, 50)

	// Request 10 items, but buffered source will fetch 50
	result, err := bufferedSource.List(context.Background(), "", 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should return 50 items (buffer size) instead of 10
	if len(result.Items) != 50 {
		t.Errorf("expected 50 items (buffer size), got %d", len(result.Items))
	}
}

func TestBufferedDataSourceLargeRequest(t *testing.T) {
	source := DataSourceFunc[int](func(ctx context.Context, cursor Cursor, limit int) (ListResult[int], error) {
		items := make([]int, limit)
		for i := 0; i < limit; i++ {
			items[i] = i + 1
		}
		return ListResult[int]{Items: items, HasMore: false}, nil
	})

	bufferedSource := NewBufferedDataSource(source, 50)

	// Request 100 items, more than buffer size
	result, err := bufferedSource.List(context.Background(), "", 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should return 100 items since request is larger than buffer
	if len(result.Items) != 100 {
		t.Errorf("expected 100 items, got %d", len(result.Items))
	}
}

func TestNewPrefetchDataSourceDefaultLimit(t *testing.T) {
	source := DataSourceFunc[int](func(ctx context.Context, cursor Cursor, limit int) (ListResult[int], error) {
		return ListResult[int]{Items: []int{}, HasMore: false}, nil
	})

	// Pass 0 for prefetchLimit, should default to 10
	prefetchSource := NewPrefetchDataSource(source, 0)
	if prefetchSource.prefetchLimit != 10 {
		t.Errorf("expected default prefetchLimit of 10, got %d", prefetchSource.prefetchLimit)
	}

	// Pass negative value, should default to 10
	prefetchSource = NewPrefetchDataSource(source, -5)
	if prefetchSource.prefetchLimit != 10 {
		t.Errorf("expected default prefetchLimit of 10, got %d", prefetchSource.prefetchLimit)
	}
}

func TestNewBufferedDataSourceDefaultSize(t *testing.T) {
	source := DataSourceFunc[int](func(ctx context.Context, cursor Cursor, limit int) (ListResult[int], error) {
		return ListResult[int]{Items: []int{}, HasMore: false}, nil
	})

	// Pass 0 for bufferSize, should default to 100
	bufferedSource := NewBufferedDataSource(source, 0)
	if bufferedSource.bufferSize != 100 {
		t.Errorf("expected default bufferSize of 100, got %d", bufferedSource.bufferSize)
	}

	// Pass negative value, should default to 100
	bufferedSource = NewBufferedDataSource(source, -5)
	if bufferedSource.bufferSize != 100 {
		t.Errorf("expected default bufferSize of 100, got %d", bufferedSource.bufferSize)
	}
}
