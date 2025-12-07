package distributedpager

import (
	"context"
	"log"
	"sync"
	"time"
)

// CachedDataSource wraps a DataSource and caches results for a specified TTL.
// This reduces the number of calls to the underlying data source for frequently accessed data.
type CachedDataSource[T any] struct {
	source DataSource[T]
	ttl    time.Duration

	mu    sync.RWMutex
	cache map[Cursor]*cacheEntry[T]
}

// cacheEntry holds cached data with expiration time.
type cacheEntry[T any] struct {
	result    ListResult[T]
	err       error
	expiresAt time.Time
}

// NewCachedDataSource creates a new CachedDataSource with the specified TTL.
// If ttl is 0 or negative, a default TTL of 5 minutes is used.
func NewCachedDataSource[T any](source DataSource[T], ttl time.Duration) *CachedDataSource[T] {
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	return &CachedDataSource[T]{
		source: source,
		ttl:    ttl,
		cache:  make(map[Cursor]*cacheEntry[T]),
	}
}

// List retrieves items from the data source, using cached data if available and not expired.
func (c *CachedDataSource[T]) List(ctx context.Context, cursor Cursor, limit int) (ListResult[T], error) {
	// Check cache first (read lock)
	c.mu.RLock()
	if entry, ok := c.cache[cursor]; ok {
		if time.Now().Before(entry.expiresAt) {
			c.mu.RUnlock()
			return entry.result, entry.err
		}
	}
	c.mu.RUnlock()

	// Cache miss or expired, fetch from source
	result, err := c.source.List(ctx, cursor, limit)

	// Store in cache (write lock)
	c.mu.Lock()
	c.cache[cursor] = &cacheEntry[T]{
		result:    result,
		err:       err,
		expiresAt: time.Now().Add(c.ttl),
	}
	c.mu.Unlock()

	return result, err
}

// ClearCache removes all cached entries.
func (c *CachedDataSource[T]) ClearCache() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cache = make(map[Cursor]*cacheEntry[T])
}

// EvictExpired removes expired entries from the cache.
func (c *CachedDataSource[T]) EvictExpired() {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	for cursor, entry := range c.cache {
		if now.After(entry.expiresAt) {
			delete(c.cache, cursor)
		}
	}
}

// RateLimitedDataSource wraps a DataSource and rate limits requests using a token bucket algorithm.
type RateLimitedDataSource[T any] struct {
	source DataSource[T]

	mu            sync.Mutex
	tokens        float64
	maxTokens     float64
	refillRate    float64 // tokens per second
	lastRefillAt  time.Time
}

// NewRateLimitedDataSource creates a new RateLimitedDataSource.
// requestsPerSecond specifies how many requests are allowed per second.
// burst specifies the maximum number of requests that can be made in a burst.
func NewRateLimitedDataSource[T any](source DataSource[T], requestsPerSecond float64, burst int) *RateLimitedDataSource[T] {
	if requestsPerSecond <= 0 {
		requestsPerSecond = 10
	}
	if burst <= 0 {
		burst = int(requestsPerSecond)
	}
	return &RateLimitedDataSource[T]{
		source:       source,
		tokens:       float64(burst),
		maxTokens:    float64(burst),
		refillRate:   requestsPerSecond,
		lastRefillAt: time.Now(),
	}
}

// List retrieves items from the data source, respecting rate limits.
func (r *RateLimitedDataSource[T]) List(ctx context.Context, cursor Cursor, limit int) (ListResult[T], error) {
	// Wait for token availability
	for {
		r.mu.Lock()
		
		// Refill tokens based on elapsed time
		now := time.Now()
		elapsed := now.Sub(r.lastRefillAt).Seconds()
		r.tokens = min(r.maxTokens, r.tokens+elapsed*r.refillRate)
		r.lastRefillAt = now

		if r.tokens >= 1 {
			r.tokens--
			r.mu.Unlock()
			break
		}

		// Calculate wait time for next token
		waitTime := time.Duration((1-r.tokens)/r.refillRate*1000) * time.Millisecond
		r.mu.Unlock()

		// Check context cancellation
		select {
		case <-ctx.Done():
			return ListResult[T]{}, ctx.Err()
		case <-time.After(waitTime):
			// Continue to try again
		}
	}

	return r.source.List(ctx, cursor, limit)
}

// RetryDataSource wraps a DataSource and automatically retries failed requests with exponential backoff.
type RetryDataSource[T any] struct {
	source      DataSource[T]
	maxRetries  int
	initialWait time.Duration
}

// NewRetryDataSource creates a new RetryDataSource.
// maxRetries specifies the maximum number of retry attempts (0 means no retries).
// initialWait specifies the initial wait time before the first retry (doubled for each subsequent retry).
func NewRetryDataSource[T any](source DataSource[T], maxRetries int, initialWait time.Duration) *RetryDataSource[T] {
	if maxRetries < 0 {
		maxRetries = 3
	}
	if initialWait <= 0 {
		initialWait = 100 * time.Millisecond
	}
	return &RetryDataSource[T]{
		source:      source,
		maxRetries:  maxRetries,
		initialWait: initialWait,
	}
}

// List retrieves items from the data source with automatic retry on failure.
func (r *RetryDataSource[T]) List(ctx context.Context, cursor Cursor, limit int) (ListResult[T], error) {
	var lastErr error
	wait := r.initialWait

	for attempt := 0; attempt <= r.maxRetries; attempt++ {
		result, err := r.source.List(ctx, cursor, limit)
		if err == nil {
			return result, nil
		}

		lastErr = err

		// Don't retry if context is cancelled
		if ctx.Err() != nil {
			return ListResult[T]{}, ctx.Err()
		}

		// Don't wait after the last attempt
		if attempt < r.maxRetries {
			select {
			case <-ctx.Done():
				return ListResult[T]{}, ctx.Err()
			case <-time.After(wait):
				wait *= 2 // Exponential backoff
			}
		}
	}

	return ListResult[T]{}, lastErr
}

// LoggingDataSource wraps a DataSource and logs all operations for debugging.
type LoggingDataSource[T any] struct {
	source DataSource[T]
	logger *log.Logger
}

// NewLoggingDataSource creates a new LoggingDataSource.
// If logger is nil, the default logger from the log package is used.
func NewLoggingDataSource[T any](source DataSource[T], logger *log.Logger) *LoggingDataSource[T] {
	if logger == nil {
		logger = log.Default()
	}
	return &LoggingDataSource[T]{
		source: source,
		logger: logger,
	}
}

// List retrieves items from the data source and logs the operation.
func (l *LoggingDataSource[T]) List(ctx context.Context, cursor Cursor, limit int) (ListResult[T], error) {
	l.logger.Printf("[LoggingDataSource] List called with cursor=%q, limit=%d", cursor, limit)
	
	start := time.Now()
	result, err := l.source.List(ctx, cursor, limit)
	elapsed := time.Since(start)

	if err != nil {
		l.logger.Printf("[LoggingDataSource] List failed after %v: %v", elapsed, err)
	} else {
		l.logger.Printf("[LoggingDataSource] List succeeded after %v: returned %d items, hasMore=%v", 
			elapsed, len(result.Items), result.HasMore)
	}

	return result, err
}

// TransformDataSource wraps a DataSource and transforms items from type S to type T.
type TransformDataSource[S any, T any] struct {
	source    DataSource[S]
	transform func(S) T
}

// NewTransformDataSource creates a new TransformDataSource that applies a transformation function to each item.
func NewTransformDataSource[S any, T any](source DataSource[S], transform func(S) T) *TransformDataSource[S, T] {
	return &TransformDataSource[S, T]{
		source:    source,
		transform: transform,
	}
}

// List retrieves items from the data source and transforms each item.
func (t *TransformDataSource[S, T]) List(ctx context.Context, cursor Cursor, limit int) (ListResult[T], error) {
	result, err := t.source.List(ctx, cursor, limit)
	if err != nil {
		return ListResult[T]{}, err
	}

	transformedItems := make([]T, len(result.Items))
	for i, item := range result.Items {
		transformedItems[i] = t.transform(item)
	}

	return ListResult[T]{
		Items:      transformedItems,
		NextCursor: result.NextCursor,
		HasMore:    result.HasMore,
	}, nil
}

// min returns the minimum of two float64 values.
func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}
