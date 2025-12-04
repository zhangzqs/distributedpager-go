# distributedpager-go

基于Golang实现的，支持多数据源的分布式多路归并排序通用框架，用于将多数据源的有序List接口合并为统一的单一数据源

## 特性

- **泛型支持**: 使用Go泛型表示元素类型，支持任意数据类型
- **多数据源合并**: 支持将多个有序数据源合并为单一数据源
- **灵活排序**: 支持自定义比较函数，可按任意字段排序
- **分页支持**: 基于游标的分页机制，支持大数据量的分页查询
- **装饰器模式**: 提供预读装饰器等工具，优化数据源访问性能

## 安装

```bash
go get github.com/zhangzqs/distributedpager-go
```

## 快速开始

### 基本使用

```go
package main

import (
    "context"
    "fmt"

    dp "github.com/zhangzqs/distributedpager-go"
)

func main() {
    // 创建两个数据源
    source1 := dp.DataSourceFunc[int](func(ctx context.Context, cursor dp.Cursor, limit int) (dp.ListResult[int], error) {
        return dp.ListResult[int]{
            Items:   []int{1, 3, 5, 7, 9},
            HasMore: false,
        }, nil
    })

    source2 := dp.DataSourceFunc[int](func(ctx context.Context, cursor dp.Cursor, limit int) (dp.ListResult[int], error) {
        return dp.ListResult[int]{
            Items:   []int{2, 4, 6, 8, 10},
            HasMore: false,
        }, nil
    })

    // 创建比较函数（升序）
    cmp := dp.CompareBy[int](func(n int) int { return n })

    // 创建合并分页器
    pager := dp.NewMergePager[int]([]dp.DataSource[int]{source1, source2}, cmp)

    // 获取合并后的结果
    result, _ := pager.List(context.Background(), "", 10)
    fmt.Println(result.Items) // 输出: [1 2 3 4 5 6 7 8 9 10]
}
```

### 使用结构体和自定义排序

```go
package main

import (
    "context"
    "fmt"

    dp "github.com/zhangzqs/distributedpager-go"
)

type Event struct {
    ID        int
    Timestamp int64
    Source    string
}

func main() {
    // 数据源1: 来自服务A的事件
    source1 := dp.DataSourceFunc[Event](func(ctx context.Context, cursor dp.Cursor, limit int) (dp.ListResult[Event], error) {
        return dp.ListResult[Event]{
            Items: []Event{
                {ID: 1, Timestamp: 100, Source: "A"},
                {ID: 2, Timestamp: 300, Source: "A"},
            },
            HasMore: false,
        }, nil
    })

    // 数据源2: 来自服务B的事件
    source2 := dp.DataSourceFunc[Event](func(ctx context.Context, cursor dp.Cursor, limit int) (dp.ListResult[Event], error) {
        return dp.ListResult[Event]{
            Items: []Event{
                {ID: 3, Timestamp: 200, Source: "B"},
                {ID: 4, Timestamp: 400, Source: "B"},
            },
            HasMore: false,
        }, nil
    })

    // 按时间戳排序
    cmp := dp.CompareBy[Event](func(e Event) int64 { return e.Timestamp })
    
    pager := dp.NewMergePager[Event]([]dp.DataSource[Event]{source1, source2}, cmp)
    result, _ := pager.List(context.Background(), "", 10)
    
    for _, e := range result.Items {
        fmt.Printf("ID: %d, Timestamp: %d, Source: %s\n", e.ID, e.Timestamp, e.Source)
    }
    // 输出:
    // ID: 1, Timestamp: 100, Source: A
    // ID: 3, Timestamp: 200, Source: B
    // ID: 2, Timestamp: 300, Source: A
    // ID: 4, Timestamp: 400, Source: B
}
```

### 分页查询

```go
// 获取第一页
result1, _ := pager.List(ctx, "", 10)
fmt.Println(result1.Items)

// 获取下一页
if result1.HasMore {
    result2, _ := pager.List(ctx, result1.NextCursor, 10)
    fmt.Println(result2.Items)
}
```

### 使用预读装饰器

```go
// 包装数据源以启用预读
prefetchSource := dp.NewPrefetchDataSource(originalSource, 20)

// 使用预读数据源
result, _ := prefetchSource.List(ctx, "", 10)
// 后台会自动预读下一页数据
```

### 多字段排序

```go
// 先按年龄排序，年龄相同时按姓名排序
cmp := dp.ChainComparators(
    dp.CompareBy[Person](func(p Person) int { return p.Age }),
    dp.CompareBy[Person](func(p Person) string { return p.Name }),
)
```

### 降序排序

```go
// 降序排序
cmp := dp.CompareByDesc[int](func(n int) int { return n })
```

## API 文档

### 核心接口

#### DataSource[T]

数据源接口，定义了分页列举数据的方法。

```go
type DataSource[T any] interface {
    List(ctx context.Context, cursor Cursor, limit int) (ListResult[T], error)
}
```

#### ListResult[T]

列举操作的结果。

```go
type ListResult[T any] struct {
    Items      []T    // 返回的数据项
    NextCursor Cursor // 下一页的游标
    HasMore    bool   // 是否还有更多数据
}
```

### 合并分页器

#### NewMergePager[T]

创建一个新的合并分页器。

```go
func NewMergePager[T any](sources []DataSource[T], comparator Comparator[T]) *MergePager[T]
```

### 比较函数

#### CompareBy[T, K]

根据指定字段创建升序比较函数。

```go
func CompareBy[T any, K Ordered](keyFunc func(T) K) Comparator[T]
```

#### CompareByDesc[T, K]

根据指定字段创建降序比较函数。

```go
func CompareByDesc[T any, K Ordered](keyFunc func(T) K) Comparator[T]
```

#### ChainComparators[T]

链接多个比较函数，实现多字段排序。

```go
func ChainComparators[T any](comparators ...Comparator[T]) Comparator[T]
```

### 装饰器

#### PrefetchDataSource[T]

预读装饰器，在后台预读下一页数据以减少延迟。

```go
func NewPrefetchDataSource[T any](source DataSource[T], prefetchLimit int) *PrefetchDataSource[T]
```

#### BufferedDataSource[T]

缓冲装饰器，批量获取数据以减少请求次数。

```go
func NewBufferedDataSource[T any](source DataSource[T], bufferSize int) *BufferedDataSource[T]
```

## 许可证

MIT License