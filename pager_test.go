package distributedpager

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"testing"
)

// createTestDataSource creates a test data source with sequential integers.
// It supports pagination using numeric cursor values.
func createTestDataSource(start, end int) DataSource[int] {
	return DataSourceFunc[int](func(ctx context.Context, cursor Cursor, limit int) (ListResult[int], error) {
		current := start
		if cursor != "" {
			parsed, err := strconv.Atoi(cursor)
			if err == nil {
				current = parsed
			}
		}

		var items []int
		for i := 0; i < limit && current < end; i++ {
			items = append(items, current)
			current++
		}

		hasMore := current < end
		var nextCursor Cursor
		if hasMore {
			nextCursor = strconv.Itoa(current)
		}

		return ListResult[int]{
			Items:      items,
			NextCursor: nextCursor,
			HasMore:    hasMore,
		}, nil
	})
}

func TestMergePagerEmpty(t *testing.T) {
	pager := NewMergePager[int]([]DataSource[int]{}, nil)
	result, err := pager.List(context.Background(), "", 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Items) != 0 {
		t.Errorf("expected 0 items, got %d", len(result.Items))
	}
	if result.HasMore {
		t.Error("expected HasMore to be false")
	}
}

func TestMergePagerSingleSource(t *testing.T) {
	source := createTestDataSource(1, 6) // [1, 2, 3, 4, 5]
	cmp := CompareBy[int](func(n int) int { return n })
	pager := NewMergePager[int]([]DataSource[int]{source}, cmp)

	result, err := pager.List(context.Background(), "", 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := []int{1, 2, 3, 4, 5}
	if len(result.Items) != len(expected) {
		t.Fatalf("expected %d items, got %d", len(expected), len(result.Items))
	}

	for i, item := range result.Items {
		if item != expected[i] {
			t.Errorf("item %d: expected %d, got %d", i, expected[i], item)
		}
	}
}

func TestMergePagerMultipleSources(t *testing.T) {
	// Source 1: odd numbers [1, 3, 5, 7, 9]
	source1 := DataSourceFunc[int](func(ctx context.Context, cursor Cursor, limit int) (ListResult[int], error) {
		items := []int{1, 3, 5, 7, 9}
		start := 0
		if cursor != "" {
			idx, _ := strconv.Atoi(cursor)
			start = idx
		}
		end := start + limit
		if end > len(items) {
			end = len(items)
		}
		hasMore := end < len(items)
		var nextCursor Cursor
		if hasMore {
			nextCursor = strconv.Itoa(end)
		}
		return ListResult[int]{Items: items[start:end], NextCursor: nextCursor, HasMore: hasMore}, nil
	})

	// Source 2: even numbers [2, 4, 6, 8, 10]
	source2 := DataSourceFunc[int](func(ctx context.Context, cursor Cursor, limit int) (ListResult[int], error) {
		items := []int{2, 4, 6, 8, 10}
		start := 0
		if cursor != "" {
			idx, _ := strconv.Atoi(cursor)
			start = idx
		}
		end := start + limit
		if end > len(items) {
			end = len(items)
		}
		hasMore := end < len(items)
		var nextCursor Cursor
		if hasMore {
			nextCursor = strconv.Itoa(end)
		}
		return ListResult[int]{Items: items[start:end], NextCursor: nextCursor, HasMore: hasMore}, nil
	})

	cmp := CompareBy[int](func(n int) int { return n })
	pager := NewMergePager[int]([]DataSource[int]{source1, source2}, cmp)

	result, err := pager.List(context.Background(), "", 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should be sorted: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
	expected := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	if len(result.Items) != len(expected) {
		t.Fatalf("expected %d items, got %d", len(expected), len(result.Items))
	}

	for i, item := range result.Items {
		if item != expected[i] {
			t.Errorf("item %d: expected %d, got %d", i, expected[i], item)
		}
	}
}

func TestMergePagerPagination(t *testing.T) {
	// Source 1: [1, 3, 5]
	source1 := DataSourceFunc[int](func(ctx context.Context, cursor Cursor, limit int) (ListResult[int], error) {
		items := []int{1, 3, 5}
		start := 0
		if cursor != "" {
			idx, _ := strconv.Atoi(cursor)
			start = idx
		}
		end := start + limit
		if end > len(items) {
			end = len(items)
		}
		hasMore := end < len(items)
		var nextCursor Cursor
		if hasMore {
			nextCursor = strconv.Itoa(end)
		}
		return ListResult[int]{Items: items[start:end], NextCursor: nextCursor, HasMore: hasMore}, nil
	})

	// Source 2: [2, 4, 6]
	source2 := DataSourceFunc[int](func(ctx context.Context, cursor Cursor, limit int) (ListResult[int], error) {
		items := []int{2, 4, 6}
		start := 0
		if cursor != "" {
			idx, _ := strconv.Atoi(cursor)
			start = idx
		}
		end := start + limit
		if end > len(items) {
			end = len(items)
		}
		hasMore := end < len(items)
		var nextCursor Cursor
		if hasMore {
			nextCursor = strconv.Itoa(end)
		}
		return ListResult[int]{Items: items[start:end], NextCursor: nextCursor, HasMore: hasMore}, nil
	})

	cmp := CompareBy[int](func(n int) int { return n })
	pager := NewMergePager[int]([]DataSource[int]{source1, source2}, cmp)

	// Collect all items across multiple pages
	var allItems []int
	cursor := Cursor("")
	for {
		result, err := pager.List(context.Background(), cursor, 2)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		allItems = append(allItems, result.Items...)
		if !result.HasMore {
			break
		}
		cursor = result.NextCursor
	}

	expected := []int{1, 2, 3, 4, 5, 6}
	if len(allItems) != len(expected) {
		t.Fatalf("expected %d items, got %d: %v", len(expected), len(allItems), allItems)
	}

	for i, item := range allItems {
		if item != expected[i] {
			t.Errorf("item %d: expected %d, got %d", i, expected[i], item)
		}
	}
}

func TestMergePagerWithoutComparator(t *testing.T) {
	// Source 1: [1, 2, 3]
	source1 := DataSourceFunc[int](func(ctx context.Context, cursor Cursor, limit int) (ListResult[int], error) {
		items := []int{1, 2, 3}
		start := 0
		if cursor != "" {
			idx, _ := strconv.Atoi(cursor)
			start = idx
		}
		end := start + limit
		if end > len(items) {
			end = len(items)
		}
		hasMore := end < len(items)
		var nextCursor Cursor
		if hasMore {
			nextCursor = strconv.Itoa(end)
		}
		return ListResult[int]{Items: items[start:end], NextCursor: nextCursor, HasMore: hasMore}, nil
	})

	// Source 2: [4, 5, 6]
	source2 := DataSourceFunc[int](func(ctx context.Context, cursor Cursor, limit int) (ListResult[int], error) {
		items := []int{4, 5, 6}
		start := 0
		if cursor != "" {
			idx, _ := strconv.Atoi(cursor)
			start = idx
		}
		end := start + limit
		if end > len(items) {
			end = len(items)
		}
		hasMore := end < len(items)
		var nextCursor Cursor
		if hasMore {
			nextCursor = strconv.Itoa(end)
		}
		return ListResult[int]{Items: items[start:end], NextCursor: nextCursor, HasMore: hasMore}, nil
	})

	// No comparator - should use round-robin
	pager := NewMergePager[int]([]DataSource[int]{source1, source2}, nil)

	result, err := pager.List(context.Background(), "", 6)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Round-robin should alternate: [1, 4, 2, 5, 3, 6]
	if len(result.Items) != 6 {
		t.Fatalf("expected 6 items, got %d: %v", len(result.Items), result.Items)
	}

	// Verify we got all items (order may vary in round-robin)
	sum := 0
	for _, item := range result.Items {
		sum += item
	}
	if sum != 21 { // 1+2+3+4+5+6 = 21
		t.Errorf("expected sum of 21, got %d", sum)
	}
}

func TestMergePagerDescending(t *testing.T) {
	// Source 1: descending [9, 7, 5, 3, 1]
	source1 := DataSourceFunc[int](func(ctx context.Context, cursor Cursor, limit int) (ListResult[int], error) {
		items := []int{9, 7, 5, 3, 1}
		start := 0
		if cursor != "" {
			idx, _ := strconv.Atoi(cursor)
			start = idx
		}
		end := start + limit
		if end > len(items) {
			end = len(items)
		}
		hasMore := end < len(items)
		var nextCursor Cursor
		if hasMore {
			nextCursor = strconv.Itoa(end)
		}
		return ListResult[int]{Items: items[start:end], NextCursor: nextCursor, HasMore: hasMore}, nil
	})

	// Source 2: descending [10, 8, 6, 4, 2]
	source2 := DataSourceFunc[int](func(ctx context.Context, cursor Cursor, limit int) (ListResult[int], error) {
		items := []int{10, 8, 6, 4, 2}
		start := 0
		if cursor != "" {
			idx, _ := strconv.Atoi(cursor)
			start = idx
		}
		end := start + limit
		if end > len(items) {
			end = len(items)
		}
		hasMore := end < len(items)
		var nextCursor Cursor
		if hasMore {
			nextCursor = strconv.Itoa(end)
		}
		return ListResult[int]{Items: items[start:end], NextCursor: nextCursor, HasMore: hasMore}, nil
	})

	// Descending comparator
	cmp := CompareByDesc[int](func(n int) int { return n })
	pager := NewMergePager[int]([]DataSource[int]{source1, source2}, cmp)

	result, err := pager.List(context.Background(), "", 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should be sorted descending: [10, 9, 8, 7, 6, 5, 4, 3, 2, 1]
	expected := []int{10, 9, 8, 7, 6, 5, 4, 3, 2, 1}
	if len(result.Items) != len(expected) {
		t.Fatalf("expected %d items, got %d", len(expected), len(result.Items))
	}

	for i, item := range result.Items {
		if item != expected[i] {
			t.Errorf("item %d: expected %d, got %d", i, expected[i], item)
		}
	}
}

func TestMergePagerWithStructs(t *testing.T) {
	type Event struct {
		ID        int
		Timestamp int64
		Source    string
	}

	// Source 1: events with timestamps [100, 300, 500]
	source1 := DataSourceFunc[Event](func(ctx context.Context, cursor Cursor, limit int) (ListResult[Event], error) {
		items := []Event{
			{ID: 1, Timestamp: 100, Source: "A"},
			{ID: 2, Timestamp: 300, Source: "A"},
			{ID: 3, Timestamp: 500, Source: "A"},
		}
		start := 0
		if cursor != "" {
			idx, _ := strconv.Atoi(cursor)
			start = idx
		}
		end := start + limit
		if end > len(items) {
			end = len(items)
		}
		hasMore := end < len(items)
		var nextCursor Cursor
		if hasMore {
			nextCursor = strconv.Itoa(end)
		}
		return ListResult[Event]{Items: items[start:end], NextCursor: nextCursor, HasMore: hasMore}, nil
	})

	// Source 2: events with timestamps [200, 400, 600]
	source2 := DataSourceFunc[Event](func(ctx context.Context, cursor Cursor, limit int) (ListResult[Event], error) {
		items := []Event{
			{ID: 4, Timestamp: 200, Source: "B"},
			{ID: 5, Timestamp: 400, Source: "B"},
			{ID: 6, Timestamp: 600, Source: "B"},
		}
		start := 0
		if cursor != "" {
			idx, _ := strconv.Atoi(cursor)
			start = idx
		}
		end := start + limit
		if end > len(items) {
			end = len(items)
		}
		hasMore := end < len(items)
		var nextCursor Cursor
		if hasMore {
			nextCursor = strconv.Itoa(end)
		}
		return ListResult[Event]{Items: items[start:end], NextCursor: nextCursor, HasMore: hasMore}, nil
	})

	// Sort by timestamp
	cmp := CompareBy[Event](func(e Event) int64 { return e.Timestamp })
	pager := NewMergePager[Event]([]DataSource[Event]{source1, source2}, cmp)

	result, err := pager.List(context.Background(), "", 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should be sorted by timestamp: [100, 200, 300, 400, 500, 600]
	expectedTimestamps := []int64{100, 200, 300, 400, 500, 600}
	if len(result.Items) != len(expectedTimestamps) {
		t.Fatalf("expected %d items, got %d", len(expectedTimestamps), len(result.Items))
	}

	for i, item := range result.Items {
		if item.Timestamp != expectedTimestamps[i] {
			t.Errorf("item %d: expected timestamp %d, got %d", i, expectedTimestamps[i], item.Timestamp)
		}
	}
}

func TestMergePagerThreeSources(t *testing.T) {
	// Three sources with interleaved values
	source1 := DataSourceFunc[int](func(ctx context.Context, cursor Cursor, limit int) (ListResult[int], error) {
		items := []int{1, 4, 7, 10}
		start := 0
		if cursor != "" {
			idx, _ := strconv.Atoi(cursor)
			start = idx
		}
		end := start + limit
		if end > len(items) {
			end = len(items)
		}
		hasMore := end < len(items)
		var nextCursor Cursor
		if hasMore {
			nextCursor = strconv.Itoa(end)
		}
		return ListResult[int]{Items: items[start:end], NextCursor: nextCursor, HasMore: hasMore}, nil
	})

	source2 := DataSourceFunc[int](func(ctx context.Context, cursor Cursor, limit int) (ListResult[int], error) {
		items := []int{2, 5, 8, 11}
		start := 0
		if cursor != "" {
			idx, _ := strconv.Atoi(cursor)
			start = idx
		}
		end := start + limit
		if end > len(items) {
			end = len(items)
		}
		hasMore := end < len(items)
		var nextCursor Cursor
		if hasMore {
			nextCursor = strconv.Itoa(end)
		}
		return ListResult[int]{Items: items[start:end], NextCursor: nextCursor, HasMore: hasMore}, nil
	})

	source3 := DataSourceFunc[int](func(ctx context.Context, cursor Cursor, limit int) (ListResult[int], error) {
		items := []int{3, 6, 9, 12}
		start := 0
		if cursor != "" {
			idx, _ := strconv.Atoi(cursor)
			start = idx
		}
		end := start + limit
		if end > len(items) {
			end = len(items)
		}
		hasMore := end < len(items)
		var nextCursor Cursor
		if hasMore {
			nextCursor = strconv.Itoa(end)
		}
		return ListResult[int]{Items: items[start:end], NextCursor: nextCursor, HasMore: hasMore}, nil
	})

	cmp := CompareBy[int](func(n int) int { return n })
	pager := NewMergePager[int]([]DataSource[int]{source1, source2, source3}, cmp)

	result, err := pager.List(context.Background(), "", 12)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
	if len(result.Items) != len(expected) {
		t.Fatalf("expected %d items, got %d", len(expected), len(result.Items))
	}

	for i, item := range result.Items {
		if item != expected[i] {
			t.Errorf("item %d: expected %d, got %d", i, expected[i], item)
		}
	}
}

func TestCursorEncodingDecoding(t *testing.T) {
	state := &mergeState{
		SourceCursors:   []Cursor{"cursor1", "cursor2"},
		SourceExhausted: []bool{false, true},
		Buffers:         []json.RawMessage{json.RawMessage("[1,2,3]"), json.RawMessage("[4,5,6]")},
	}

	encoded, err := encodeCursor(state)
	if err != nil {
		t.Fatalf("failed to encode cursor: %v", err)
	}

	decoded, err := decodeCursor(encoded)
	if err != nil {
		t.Fatalf("failed to decode cursor: %v", err)
	}

	if len(decoded.SourceCursors) != len(state.SourceCursors) {
		t.Errorf("cursor count mismatch")
	}

	for i, c := range decoded.SourceCursors {
		if c != state.SourceCursors[i] {
			t.Errorf("cursor %d mismatch: expected %s, got %s", i, state.SourceCursors[i], c)
		}
	}
}

func TestDecodeCursorEmpty(t *testing.T) {
	result, err := decodeCursor("")
	if err != nil {
		t.Fatalf("unexpected error for empty cursor: %v", err)
	}
	if result != nil {
		t.Error("expected nil for empty cursor")
	}
}

func TestDecodeCursorInvalid(t *testing.T) {
	_, err := decodeCursor("invalid-base64!!!")
	if err == nil {
		t.Error("expected error for invalid cursor")
	}
}

// Benchmark tests
func BenchmarkMergePagerTwoSources(b *testing.B) {
	source1 := createTestDataSource(0, 1000)
	source2 := createTestDataSource(1000, 2000)

	cmp := CompareBy[int](func(n int) int { return n })
	pager := NewMergePager[int]([]DataSource[int]{source1, source2}, cmp)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := pager.List(context.Background(), "", 100)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMergePagerFiveSources(b *testing.B) {
	sources := make([]DataSource[int], 5)
	for i := 0; i < 5; i++ {
		sources[i] = createTestDataSource(i*200, (i+1)*200)
	}

	cmp := CompareBy[int](func(n int) int { return n })
	pager := NewMergePager[int](sources, cmp)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := pager.List(context.Background(), "", 100)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Example test to demonstrate usage
func ExampleMergePager() {
	// Create two data sources
	source1 := DataSourceFunc[int](func(ctx context.Context, cursor Cursor, limit int) (ListResult[int], error) {
		return ListResult[int]{Items: []int{1, 3, 5}, HasMore: false}, nil
	})
	source2 := DataSourceFunc[int](func(ctx context.Context, cursor Cursor, limit int) (ListResult[int], error) {
		return ListResult[int]{Items: []int{2, 4, 6}, HasMore: false}, nil
	})

	// Create merge pager with ascending order
	cmp := CompareBy[int](func(n int) int { return n })
	pager := NewMergePager[int]([]DataSource[int]{source1, source2}, cmp)

	// Get merged results
	result, _ := pager.List(context.Background(), "", 10)
	fmt.Println(result.Items)
	// Output: [1 2 3 4 5 6]
}
