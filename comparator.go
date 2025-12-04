package distributedpager

// Comparator is a function type that compares two elements.
// It returns:
//   - negative value if a < b
//   - zero if a == b
//   - positive value if a > b
type Comparator[T any] func(a, b T) int

// ReverseComparator returns a new comparator that reverses the order of the original.
func ReverseComparator[T any](cmp Comparator[T]) Comparator[T] {
	return func(a, b T) int {
		return cmp(b, a)
	}
}

// CompareBy creates a comparator that extracts a comparable key from elements
// and compares them using the natural ordering of the key type.
// This is useful for sorting by a specific field of a struct.
func CompareBy[T any, K Ordered](keyFunc func(T) K) Comparator[T] {
	return func(a, b T) int {
		ka, kb := keyFunc(a), keyFunc(b)
		if ka < kb {
			return -1
		}
		if ka > kb {
			return 1
		}
		return 0
	}
}

// CompareByDesc creates a comparator that extracts a comparable key from elements
// and compares them in descending order.
func CompareByDesc[T any, K Ordered](keyFunc func(T) K) Comparator[T] {
	return ReverseComparator(CompareBy[T, K](keyFunc))
}

// Ordered is a constraint that permits any ordered type.
type Ordered interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~uintptr |
		~float32 | ~float64 |
		~string
}

// ChainComparators combines multiple comparators into one.
// It applies each comparator in order until one returns a non-zero result.
// This is useful for multi-field sorting (e.g., sort by name, then by age).
func ChainComparators[T any](comparators ...Comparator[T]) Comparator[T] {
	return func(a, b T) int {
		for _, cmp := range comparators {
			if result := cmp(a, b); result != 0 {
				return result
			}
		}
		return 0
	}
}
