package lru

import (
	"container/list"
)

/*
	    maxByte: 最大使用内存
		nbytes: 当前已经使用内存
		OnEvicted 数据删除的回调函数
		ll 双向链表
		cache map 映射  {string:list.Element}
*/
type Cache struct {
	maxBytes int64
	nbytes   int64
	ll       *list.List
	cache    map[string]*list.Element
	// 删除动作附加 函数
	OnEvicted func(key string, value Value)
}
type entry struct {
	key   string
	value Value
}

type Value interface {
	Len() int
}

// public 函数返回 Cache 对象，工厂模式的使用
func New(maxBytes int64, onEvicted func(string2 string, value Value)) *Cache {

	return &Cache{
		maxBytes:  maxBytes,
		ll:        list.New(),
		cache:     make(map[string]*list.Element),
		OnEvicted: onEvicted,
	}
}

// 访问内存（查找，存在更新节点位置到队尾）
func (c *Cache) Get(key string) (value Value, ok bool) {
	ele, ok := c.cache[key]
	if ok {
		c.ll.MoveToFront(ele)
		kv := ele.Value.(*entry)
		return kv.value, true
	}
	return
}

// 删除 执行LRU策略
// 删除最近最少访问的节点
// 更新使用空间
func (c *Cache) RemoveOldest() {
	ele := c.ll.Back()
	if ele != nil {
		c.ll.Remove(ele)
		kv := ele.Value.(*entry)
		delete(c.cache, kv.key)
		c.nbytes -= int64(len(kv.key)) + int64(kv.value.Len())
		if c.OnEvicted != nil {
			c.OnEvicted(kv.key, kv.value)
		}
	}
}

// 新增加

func (c *Cache) Add(key string, value Value) {

	// update
	if ele, ok := c.cache[key]; ok {
		c.ll.MoveToFront(ele)
		kv := ele.Value.(*entry)
		c.nbytes += int64(value.Len()) - int64(kv.value.Len())
		kv.value = value
	} else { // add new
		ele := c.ll.PushFront(&entry{key, value})
		c.cache[key] = ele
		c.nbytes += int64(len(key)) + int64(value.Len())
	}
	// cache 满了，进行淘汰
	for c.maxBytes != 0 && c.maxBytes < c.nbytes {
		c.RemoveOldest()
	}
}

func (c *Cache) Remove(key string) bool {
	ele, ok := c.cache[key]
	if ok {
		kv := ele.Value.(*entry)
		c.ll.Remove(ele)
		delete(c.cache, key)
		c.nbytes -= int64(len(kv.key)) + int64(kv.value.Len())
		if c.OnEvicted != nil {
			c.OnEvicted(kv.key, kv.value)
		}
		return true
	}
	return false
}
func (c *Cache) Len() int {
	return c.ll.Len()
}
