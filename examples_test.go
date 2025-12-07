package distributedpager_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	dp "github.com/zhangzqs/distributedpager-go"
)

// Example_decoratorComposition demonstrates how to compose multiple decorators
// for a robust and performant data source.
func Example_decoratorComposition() {
	// Create a base data source (simulating a remote API)
	baseSource := dp.DataSourceFunc[int](func(ctx context.Context, cursor dp.Cursor, limit int) (dp.ListResult[int], error) {
		// Simulate API call
		items := []int{1, 2, 3, 4, 5}
		return dp.ListResult[int]{
			Items:   items,
			HasMore: false,
		}, nil
	})

	// Layer 1: Add retry logic for reliability
	withRetry := dp.NewRetryDataSource(baseSource, 3, 100*time.Millisecond)

	// Layer 2: Add rate limiting to respect API limits
	withRateLimit := dp.NewRateLimitedDataSource(withRetry, 10.0, 20)

	// Layer 3: Add caching to reduce API calls
	withCache := dp.NewCachedDataSource(withRateLimit, 5*time.Minute)

	// Layer 4: Add logging for observability
	logger := log.New(os.Stdout, "[API] ", log.LstdFlags)
	finalSource := dp.NewLoggingDataSource(withCache, logger)

	// Use the enhanced data source
	result, err := finalSource.List(context.Background(), "", 10)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Printf("Retrieved %d items\n", len(result.Items))
	// Output will include logging and the result
}

// Example_transformDataSource demonstrates type transformation.
func Example_transformDataSource() {
	// Source that returns user IDs
	idSource := dp.DataSourceFunc[int](func(ctx context.Context, cursor dp.Cursor, limit int) (dp.ListResult[int], error) {
		return dp.ListResult[int]{
			Items:   []int{1, 2, 3},
			HasMore: false,
		}, nil
	})

	// Transform IDs to user objects
	type User struct {
		ID   int
		Name string
	}

	userSource := dp.NewTransformDataSource(idSource, func(id int) User {
		return User{
			ID:   id,
			Name: fmt.Sprintf("User%d", id),
		}
	})

	result, _ := userSource.List(context.Background(), "", 10)
	for _, user := range result.Items {
		fmt.Printf("ID: %d, Name: %s\n", user.ID, user.Name)
	}
	// Output:
	// ID: 1, Name: User1
	// ID: 2, Name: User2
	// ID: 3, Name: User3
}

// Example_cachedDataSource demonstrates caching with TTL.
func Example_cachedDataSource() {
	callCount := 0
	source := dp.DataSourceFunc[string](func(ctx context.Context, cursor dp.Cursor, limit int) (dp.ListResult[string], error) {
		callCount++
		return dp.ListResult[string]{
			Items:   []string{"a", "b", "c"},
			HasMore: false,
		}, nil
	})

	cached := dp.NewCachedDataSource(source, 1*time.Minute)

	// First call - hits the source
	result1, _ := cached.List(context.Background(), "", 10)
	fmt.Printf("First call: %d items, source called %d times\n", len(result1.Items), callCount)

	// Second call - uses cache
	result2, _ := cached.List(context.Background(), "", 10)
	fmt.Printf("Second call: %d items, source called %d times\n", len(result2.Items), callCount)

	// Output:
	// First call: 3 items, source called 1 times
	// Second call: 3 items, source called 1 times
}

// Example_rateLimitedDataSource demonstrates rate limiting.
func Example_rateLimitedDataSource() {
	source := dp.DataSourceFunc[int](func(ctx context.Context, cursor dp.Cursor, limit int) (dp.ListResult[int], error) {
		return dp.ListResult[int]{
			Items:   []int{1, 2, 3},
			HasMore: false,
		}, nil
	})

	// Allow 2 requests per second with burst of 2
	rateLimited := dp.NewRateLimitedDataSource(source, 2.0, 2)

	start := time.Now()
	// First two calls go through immediately (burst)
	rateLimited.List(context.Background(), "", 10)
	rateLimited.List(context.Background(), "", 10)

	// Third call is rate limited
	rateLimited.List(context.Background(), "", 10)
	elapsed := time.Since(start)

	fmt.Printf("Three calls took at least %dms\n", elapsed.Milliseconds())
	// Output will show delay due to rate limiting
}

// Example_retryDataSource demonstrates automatic retry with exponential backoff.
func Example_retryDataSource() {
	attempts := 0
	source := dp.DataSourceFunc[int](func(ctx context.Context, cursor dp.Cursor, limit int) (dp.ListResult[int], error) {
		attempts++
		if attempts < 3 {
			return dp.ListResult[int]{}, fmt.Errorf("temporary error")
		}
		return dp.ListResult[int]{
			Items:   []int{1, 2, 3},
			HasMore: false,
		}, nil
	})

	// Retry up to 3 times with 10ms initial wait
	retry := dp.NewRetryDataSource(source, 3, 10*time.Millisecond)

	result, err := retry.List(context.Background(), "", 10)
	if err != nil {
		fmt.Printf("Failed after retries: %v\n", err)
	} else {
		fmt.Printf("Succeeded after %d attempts with %d items\n", attempts, len(result.Items))
	}
	// Output:
	// Succeeded after 3 attempts with 3 items
}
