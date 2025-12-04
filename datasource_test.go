package distributedpager

import (
	"context"
	"testing"
)

func TestDataSourceFunc(t *testing.T) {
	items := []int{1, 2, 3, 4, 5}

	ds := DataSourceFunc[int](func(ctx context.Context, cursor Cursor, limit int) (ListResult[int], error) {
		return ListResult[int]{
			Items:      items,
			NextCursor: "",
			HasMore:    false,
		}, nil
	})

	result, err := ds.List(context.Background(), "", 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result.Items) != 5 {
		t.Errorf("expected 5 items, got %d", len(result.Items))
	}

	if result.HasMore {
		t.Error("expected HasMore to be false")
	}
}

func TestListResultPagination(t *testing.T) {
	allItems := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

	ds := DataSourceFunc[int](func(ctx context.Context, cursor Cursor, limit int) (ListResult[int], error) {
		startIdx := 0
		if cursor != "" {
			for i := range allItems {
				if cursor == string(rune('a'+i)) {
					startIdx = i
					break
				}
			}
		}

		endIdx := startIdx + limit
		if endIdx > len(allItems) {
			endIdx = len(allItems)
		}

		var nextCursor Cursor
		hasMore := endIdx < len(allItems)
		if hasMore {
			nextCursor = string(rune('a' + endIdx))
		}

		return ListResult[int]{
			Items:      allItems[startIdx:endIdx],
			NextCursor: nextCursor,
			HasMore:    hasMore,
		}, nil
	})

	// First page
	result, err := ds.List(context.Background(), "", 3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result.Items) != 3 {
		t.Errorf("expected 3 items, got %d", len(result.Items))
	}

	if !result.HasMore {
		t.Error("expected HasMore to be true")
	}

	if result.NextCursor == "" {
		t.Error("expected non-empty NextCursor")
	}
}
