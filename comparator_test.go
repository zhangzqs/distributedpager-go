package distributedpager

import (
	"context"
	"testing"
)

func TestCompareBy(t *testing.T) {
	type Person struct {
		Name string
		Age  int
	}

	// Test sorting by Age
	cmp := CompareBy[Person](func(p Person) int { return p.Age })

	alice := Person{Name: "Alice", Age: 25}
	bob := Person{Name: "Bob", Age: 30}
	charlie := Person{Name: "Charlie", Age: 25}

	if cmp(alice, bob) >= 0 {
		t.Error("Expected Alice < Bob by age")
	}
	if cmp(bob, alice) <= 0 {
		t.Error("Expected Bob > Alice by age")
	}
	if cmp(alice, charlie) != 0 {
		t.Error("Expected Alice == Charlie by age")
	}
}

func TestCompareByDesc(t *testing.T) {
	cmp := CompareByDesc[int](func(n int) int { return n })

	if cmp(1, 2) <= 0 {
		t.Error("Expected 1 > 2 in descending order")
	}
	if cmp(2, 1) >= 0 {
		t.Error("Expected 2 < 1 in descending order")
	}
}

func TestChainComparators(t *testing.T) {
	type Person struct {
		Name string
		Age  int
	}

	// Sort by Age first, then by Name
	cmp := ChainComparators(
		CompareBy[Person](func(p Person) int { return p.Age }),
		CompareBy[Person](func(p Person) string { return p.Name }),
	)

	alice := Person{Name: "Alice", Age: 25}
	bob := Person{Name: "Bob", Age: 25}
	charlie := Person{Name: "Charlie", Age: 30}

	// Same age, different name
	if cmp(alice, bob) >= 0 {
		t.Error("Expected Alice < Bob (same age, name comparison)")
	}

	// Different age
	if cmp(alice, charlie) >= 0 {
		t.Error("Expected Alice < Charlie (different age)")
	}
}

func TestReverseComparator(t *testing.T) {
	cmp := CompareBy[int](func(n int) int { return n })
	reverseCmp := ReverseComparator(cmp)

	if cmp(1, 2) != -reverseCmp(1, 2) {
		t.Error("Reverse comparator should negate the result")
	}
}

// Helper function to create a simple in-memory data source
func newSliceDataSource[T any](items []T) DataSource[T] {
	return DataSourceFunc[T](func(ctx context.Context, cursor Cursor, limit int) (ListResult[T], error) {
		// Parse cursor as index
		startIdx := 0
		if cursor != "" {
			for i, item := range items {
				_ = item
				if i > 0 && cursor == string(rune('0'+i)) {
					startIdx = i
					break
				}
			}
		}

		endIdx := startIdx + limit
		if endIdx > len(items) {
			endIdx = len(items)
		}

		var nextCursor Cursor
		hasMore := endIdx < len(items)
		if hasMore {
			nextCursor = string(rune('0' + endIdx))
		}

		return ListResult[T]{
			Items:      items[startIdx:endIdx],
			NextCursor: nextCursor,
			HasMore:    hasMore,
		}, nil
	})
}
