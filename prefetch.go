package distributedpager

import (
	"context"
	"sync"
)

// PrefetchDataSource wraps a DataSource and prefetches the next page of data
// in the background to reduce latency.
type PrefetchDataSource[T any] struct {
	source DataSource[T]

	mu            sync.Mutex
	prefetchLimit int
	cache         map[Cursor]*prefetchResult[T]
}

// prefetchResult holds the result of a prefetch operation.
type prefetchResult[T any] struct {
	result ListResult[T]
	err    error
	done   chan struct{}
}

// NewPrefetchDataSource creates a new PrefetchDataSource that wraps the given source.
// prefetchLimit specifies how many items to prefetch (typically same as the expected limit).
func NewPrefetchDataSource[T any](source DataSource[T], prefetchLimit int) *PrefetchDataSource[T] {
	if prefetchLimit <= 0 {
		prefetchLimit = 10
	}
	return &PrefetchDataSource[T]{
		source:        source,
		prefetchLimit: prefetchLimit,
		cache:         make(map[Cursor]*prefetchResult[T]),
	}
}

// List retrieves items from the data source, using prefetched data if available.
func (p *PrefetchDataSource[T]) List(ctx context.Context, cursor Cursor, limit int) (ListResult[T], error) {
	// Check context cancellation first
	if err := ctx.Err(); err != nil {
		return ListResult[T]{}, err
	}

	p.mu.Lock()

	// Check if we have prefetched data for this cursor
	if cached, ok := p.cache[cursor]; ok {
		delete(p.cache, cursor)
		p.mu.Unlock()

		// Wait for prefetch to complete
		select {
		case <-cached.done:
			if cached.err != nil {
				return ListResult[T]{}, cached.err
			}
			// Start prefetching the next page in background
			if cached.result.HasMore && cached.result.NextCursor != "" {
				p.startPrefetch(ctx, cached.result.NextCursor)
			}
			return cached.result, nil
		case <-ctx.Done():
			return ListResult[T]{}, ctx.Err()
		}
	}
	p.mu.Unlock()

	// No cached data, fetch synchronously
	result, err := p.source.List(ctx, cursor, limit)
	if err != nil {
		return ListResult[T]{}, err
	}

	// Start prefetching the next page in background
	if result.HasMore && result.NextCursor != "" {
		p.startPrefetch(ctx, result.NextCursor)
	}

	return result, nil
}

// startPrefetch starts a background goroutine to prefetch the next page.
func (p *PrefetchDataSource[T]) startPrefetch(ctx context.Context, cursor Cursor) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Don't prefetch if already in cache
	if _, ok := p.cache[cursor]; ok {
		return
	}

	// Create a prefetch result placeholder
	pr := &prefetchResult[T]{
		done: make(chan struct{}),
	}
	p.cache[cursor] = pr

	// Start background fetch
	go func() {
		defer close(pr.done)
		// Use a background context that won't be cancelled when the parent returns
		bgCtx := context.Background()
		pr.result, pr.err = p.source.List(bgCtx, cursor, p.prefetchLimit)
	}()
}

// ClearCache clears all prefetched data from the cache.
func (p *PrefetchDataSource[T]) ClearCache() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cache = make(map[Cursor]*prefetchResult[T])
}

// BufferedDataSource wraps a DataSource and maintains a buffer of items
// to reduce the number of calls to the underlying source.
type BufferedDataSource[T any] struct {
	source     DataSource[T]
	bufferSize int
}

// NewBufferedDataSource creates a new BufferedDataSource that batches requests
// to the underlying source.
func NewBufferedDataSource[T any](source DataSource[T], bufferSize int) *BufferedDataSource[T] {
	if bufferSize <= 0 {
		bufferSize = 100
	}
	return &BufferedDataSource[T]{
		source:     source,
		bufferSize: bufferSize,
	}
}

// List retrieves items from the buffered data source.
func (b *BufferedDataSource[T]) List(ctx context.Context, cursor Cursor, limit int) (ListResult[T], error) {
	// Fetch more items than requested to fill the buffer
	fetchLimit := b.bufferSize
	if limit > fetchLimit {
		fetchLimit = limit
	}

	return b.source.List(ctx, cursor, fetchLimit)
}
