package utils

// Queue 结构体定义，使用泛型来支持任何类型
type Queue[T any] struct {
	items []T
}

// NewQueue 创建一个新的队列实例
func NewQueue[T any]() *Queue[T] {
	return &Queue[T]{}
}

// Enqueue 将元素添加到队列末尾
func (q *Queue[T]) Enqueue(item T) {
	q.items = append(q.items, item)
}

// Dequeue 从队列头部移除元素
func (q *Queue[T]) Dequeue() (T, bool) {
	var zero T
	if len(q.items) == 0 {
		return zero, false // 如果队列为空，返回零值和 false
	}
	item := q.items[0]
	q.items = q.items[1:]
	return item, true
}

// Peek 查看队列头部元素，但不移除它
func (q *Queue[T]) Peek() (T, bool) {
	var zero T
	if len(q.items) == 0 {
		return zero, false
	}
	return q.items[0], true
}

// IsEmpty 检查队列是否为空
func (q *Queue[T]) IsEmpty() bool {
	return len(q.items) == 0
}

// Size 返回队列中元素的数量
func (q *Queue[T]) Size() int {
	return len(q.items)
}
