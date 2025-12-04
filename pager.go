package distributedpager

import (
	"container/heap"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
)

// MergePager merges multiple sorted data sources into a single unified data source
// using a multi-way merge sort algorithm.
type MergePager[T any] struct {
	sources    []DataSource[T]
	comparator Comparator[T]
}

// NewMergePager creates a new MergePager with the given data sources and comparator.
// The comparator defines the sort order for merging items from different sources.
// If comparator is nil, items will be returned in round-robin order from sources.
func NewMergePager[T any](sources []DataSource[T], comparator Comparator[T]) *MergePager[T] {
	return &MergePager[T]{
		sources:    sources,
		comparator: comparator,
	}
}

// mergeState holds the internal state for paginated merge operations.
type mergeState struct {
	// SourceCursors holds the current cursor for each data source.
	SourceCursors []Cursor `json:"c"`
	// SourceExhausted indicates which sources have been fully consumed.
	SourceExhausted []bool `json:"e"`
	// Buffers holds buffered items for each source (items that were fetched but not yet returned).
	// We store these as JSON raw messages to handle generic types.
	Buffers []json.RawMessage `json:"b"`
}

// encodeCursor encodes the merge state into a cursor string.
func encodeCursor(state *mergeState) (Cursor, error) {
	data, err := json.Marshal(state)
	if err != nil {
		return "", fmt.Errorf("failed to encode cursor: %w", err)
	}
	return base64.URLEncoding.EncodeToString(data), nil
}

// decodeCursor decodes a cursor string back into merge state.
func decodeCursor(cursor Cursor) (*mergeState, error) {
	if cursor == "" {
		return nil, nil
	}
	data, err := base64.URLEncoding.DecodeString(cursor)
	if err != nil {
		return nil, fmt.Errorf("failed to decode cursor: %w", err)
	}
	var state mergeState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("failed to unmarshal cursor: %w", err)
	}
	return &state, nil
}

// sourceBuffer holds buffered items from a single source.
type sourceBuffer[T any] struct {
	sourceIndex int
	items       []T
	cursor      Cursor
	exhausted   bool
}

// mergeHeapItem represents an item in the priority queue for merge sort.
type mergeHeapItem[T any] struct {
	item        T
	sourceIndex int
	itemIndex   int // index within the source's buffer
}

// mergeHeap implements heap.Interface for multi-way merge.
type mergeHeap[T any] struct {
	items      []mergeHeapItem[T]
	comparator Comparator[T]
}

func (h *mergeHeap[T]) Len() int { return len(h.items) }

func (h *mergeHeap[T]) Less(i, j int) bool {
	return h.comparator(h.items[i].item, h.items[j].item) < 0
}

func (h *mergeHeap[T]) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
}

func (h *mergeHeap[T]) Push(x any) {
	h.items = append(h.items, x.(mergeHeapItem[T]))
}

func (h *mergeHeap[T]) Pop() any {
	old := h.items
	n := len(old)
	item := old[n-1]
	h.items = old[0 : n-1]
	return item
}

// List implements the DataSource interface, returning a merged page of items.
func (m *MergePager[T]) List(ctx context.Context, cursor Cursor, limit int) (ListResult[T], error) {
	if limit <= 0 {
		limit = 10 // default limit
	}

	numSources := len(m.sources)
	if numSources == 0 {
		return ListResult[T]{Items: []T{}, HasMore: false}, nil
	}

	// Initialize or restore state from cursor
	buffers := make([]*sourceBuffer[T], numSources)
	for i := range buffers {
		buffers[i] = &sourceBuffer[T]{
			sourceIndex: i,
			items:       nil,
			cursor:      "",
			exhausted:   false,
		}
	}

	// Decode cursor if provided
	if cursor != "" {
		state, err := decodeCursor(cursor)
		if err != nil {
			return ListResult[T]{}, fmt.Errorf("invalid cursor: %w", err)
		}
		if state != nil {
			for i := 0; i < numSources && i < len(state.SourceCursors); i++ {
				buffers[i].cursor = state.SourceCursors[i]
			}
			for i := 0; i < numSources && i < len(state.SourceExhausted); i++ {
				buffers[i].exhausted = state.SourceExhausted[i]
			}
			// Restore buffered items
			for i := 0; i < numSources && i < len(state.Buffers); i++ {
				if len(state.Buffers[i]) > 0 {
					var items []T
					if err := json.Unmarshal(state.Buffers[i], &items); err != nil {
						return ListResult[T]{}, fmt.Errorf("failed to unmarshal buffer: %w", err)
					}
					buffers[i].items = items
				}
			}
		}
	}

	// Fetch initial data for sources that have empty buffers and are not exhausted
	for i, buf := range buffers {
		if len(buf.items) == 0 && !buf.exhausted {
			result, err := m.sources[i].List(ctx, buf.cursor, limit)
			if err != nil {
				return ListResult[T]{}, fmt.Errorf("failed to fetch from source %d: %w", i, err)
			}
			buf.items = result.Items
			buf.cursor = result.NextCursor
			buf.exhausted = !result.HasMore && len(result.Items) == len(buf.items)
			if !result.HasMore && result.NextCursor == "" {
				buf.exhausted = true
			}
		}
	}

	var result []T

	if m.comparator == nil {
		// Round-robin merge without sorting
		result = m.roundRobinMerge(ctx, buffers, limit)
	} else {
		// Priority queue based merge sort
		result = m.heapMerge(ctx, buffers, limit)
	}

	// Check if there's more data
	hasMore := false
	for _, buf := range buffers {
		if len(buf.items) > 0 || !buf.exhausted {
			hasMore = true
			break
		}
	}

	// Build next cursor
	var nextCursor Cursor
	if hasMore {
		state := &mergeState{
			SourceCursors:   make([]Cursor, numSources),
			SourceExhausted: make([]bool, numSources),
			Buffers:         make([]json.RawMessage, numSources),
		}
		for i, buf := range buffers {
			state.SourceCursors[i] = buf.cursor
			state.SourceExhausted[i] = buf.exhausted
			if len(buf.items) > 0 {
				data, err := json.Marshal(buf.items)
				if err != nil {
					return ListResult[T]{}, fmt.Errorf("failed to marshal buffer: %w", err)
				}
				state.Buffers[i] = data
			}
		}
		var err error
		nextCursor, err = encodeCursor(state)
		if err != nil {
			return ListResult[T]{}, err
		}
	}

	return ListResult[T]{
		Items:      result,
		NextCursor: nextCursor,
		HasMore:    hasMore,
	}, nil
}

// roundRobinMerge performs a simple round-robin merge without sorting.
func (m *MergePager[T]) roundRobinMerge(ctx context.Context, buffers []*sourceBuffer[T], limit int) []T {
	result := make([]T, 0, limit)
	sourceIndex := 0
	numSources := len(buffers)

	for len(result) < limit {
		// Find next source with data
		foundData := false
		for attempts := 0; attempts < numSources; attempts++ {
			buf := buffers[sourceIndex]
			if len(buf.items) > 0 {
				result = append(result, buf.items[0])
				buf.items = buf.items[1:]
				foundData = true
				sourceIndex = (sourceIndex + 1) % numSources
				break
			}
			// Try to fetch more data if not exhausted
			if !buf.exhausted {
				fetchResult, err := m.sources[sourceIndex].List(ctx, buf.cursor, limit)
				if err == nil && len(fetchResult.Items) > 0 {
					buf.items = fetchResult.Items
					buf.cursor = fetchResult.NextCursor
					if !fetchResult.HasMore && fetchResult.NextCursor == "" {
						buf.exhausted = true
					}
					result = append(result, buf.items[0])
					buf.items = buf.items[1:]
					foundData = true
					sourceIndex = (sourceIndex + 1) % numSources
					break
				}
				buf.exhausted = true
			}
			sourceIndex = (sourceIndex + 1) % numSources
		}
		if !foundData {
			break
		}
	}

	return result
}

// heapMerge performs a heap-based multi-way merge sort.
func (m *MergePager[T]) heapMerge(ctx context.Context, buffers []*sourceBuffer[T], limit int) []T {
	result := make([]T, 0, limit)

	// Initialize the heap with the first item from each non-empty buffer
	h := &mergeHeap[T]{
		items:      make([]mergeHeapItem[T], 0, len(buffers)),
		comparator: m.comparator,
	}

	for i, buf := range buffers {
		if len(buf.items) > 0 {
			h.items = append(h.items, mergeHeapItem[T]{
				item:        buf.items[0],
				sourceIndex: i,
				itemIndex:   0,
			})
		}
	}
	heap.Init(h)

	for len(result) < limit && h.Len() > 0 {
		// Pop the smallest item
		minItem := heap.Pop(h).(mergeHeapItem[T])
		result = append(result, minItem.item)

		// Remove the used item from the buffer
		buf := buffers[minItem.sourceIndex]
		buf.items = buf.items[1:]

		// If buffer is empty but source not exhausted, fetch more
		if len(buf.items) == 0 && !buf.exhausted {
			fetchResult, err := m.sources[minItem.sourceIndex].List(ctx, buf.cursor, limit)
			if err == nil {
				buf.items = fetchResult.Items
				buf.cursor = fetchResult.NextCursor
				if !fetchResult.HasMore && fetchResult.NextCursor == "" {
					buf.exhausted = true
				}
			} else {
				buf.exhausted = true
			}
		}

		// Push the next item from the same source if available
		if len(buf.items) > 0 {
			heap.Push(h, mergeHeapItem[T]{
				item:        buf.items[0],
				sourceIndex: minItem.sourceIndex,
				itemIndex:   0,
			})
		}
	}

	return result
}
