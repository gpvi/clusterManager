package singleFlight

import "sync"

// 代表正在进行中，或已经结束的请求。使用 sync.WaitGroup 锁避免重入。
type call struct {
	wg  sync.WaitGroup
	val interface{}
	err error
}

type Group struct {
	mu sync.Mutex
	m  map[string]*call
}

func (g *Group) Do(key string, fn func() (interface{}, error)) (interface{}, error) {
	g.mu.Lock()
	// 初始化
	if g.m == nil {
		g.m = make(map[string]*call)
	}
	if c, ok := g.m[key]; ok {
		g.mu.Unlock()
		c.wg.Wait()
		return c.val, c.err
	}

	c := new(call)

	c.wg.Add(1) // 加锁
	g.mu.Unlock()

	c.val, c.err = fn()
	c.wg.Done() // 请求结束
	// 删除加锁
	g.mu.Lock()
	delete(g.m, key)
	g.mu.Unlock()

	return c.val, c.err
}
