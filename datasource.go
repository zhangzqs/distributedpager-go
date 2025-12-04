// Package distributedpager provides a generic distributed multi-way merge sort framework
// for combining multiple sorted data sources into a unified single data source.
package distributedpager

import "context"

// Cursor represents a pagination cursor/token for iterating through data sources.
// It can be any comparable type (string, int, etc.) depending on the use case.
type Cursor = string

// ListResult represents the result of a list operation from a data source.
type ListResult[T any] struct {
	// Items contains the data items returned by the list operation.
	Items []T
	// NextCursor is the cursor to use for the next page of results.
	// Empty string indicates no more data is available.
	NextCursor Cursor
	// HasMore indicates whether there are more items available after this page.
	HasMore bool
}

// DataSource is an abstract interface for paginated data sources.
// Each data source is expected to return items in a sorted order.
// The generic type T represents the element type stored in the data source.
type DataSource[T any] interface {
	// List retrieves a page of items from the data source.
	// - ctx: context for cancellation and timeout control
	// - cursor: pagination cursor (empty string for the first page)
	// - limit: maximum number of items to return
	// Returns the list result containing items and pagination info, or an error.
	List(ctx context.Context, cursor Cursor, limit int) (ListResult[T], error)
}

// DataSourceFunc is a function adapter that implements the DataSource interface.
// This allows using simple functions as data sources.
type DataSourceFunc[T any] func(ctx context.Context, cursor Cursor, limit int) (ListResult[T], error)

// List implements the DataSource interface for DataSourceFunc.
func (f DataSourceFunc[T]) List(ctx context.Context, cursor Cursor, limit int) (ListResult[T], error) {
	return f(ctx, cursor, limit)
}
