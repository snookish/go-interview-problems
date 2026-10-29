package main

import "sync"

type Client interface {
	Get(address string) (string, error)
}

type Cache struct {
	client Client
	// cache  map[string]*Result
	cache map[string]*Promise
	mu    sync.Mutex
}

type Result struct {
	Data string
	Err  error
}

func NewCache(client Client) *Cache {
	return &Cache{client: client, cache: make(map[string]*Promise)}
}

// Not Concurrent Safe, Not Async
// func (c *Cache) Get(address string) (string, error) {
// 	res, ok := c.cache[address]
// 	if !ok {
// 		res = &Result{}
// 		res.Data, res.Err = c.client.Get(address)
// 		c.cache[address] = res
// 	}

// 	return res.Data, res.Err
// }

// Concurrent Safe, But Not Async
// func (c *Cache) Get(address string) (string, error) {
// 	c.mu.Lock()
// 	defer c.mu.Unlock()

// 	res, ok := c.cache[address]
// 	if !ok {
// 		res = &Result{}
// 		res.Data, res.Err = c.client.Get(address)
// 		c.cache[address] = res
// 	}

// 	return res.Data, res.Err
// }

type Promise struct {
	result *Result
	done   chan struct{}
}

func (p *Promise) Await() (string, error) {
	<-p.done
	return p.result.Data, p.result.Err
}

func (p *Promise) Ready() bool {
	select {
	case <-p.done:
		return true
	default:
		return false
	}
}

func (c *Cache) Get(address string) (string, error) {
	return c.fetch(address).Await()
}

func (c *Cache) fetch(address string) *Promise {
	c.mu.Lock()
	promise, ok := c.cache[address]
	if promise != nil && ok {
		c.mu.Unlock()
		return promise
	}

	promise = &Promise{result: &Result{}, done: make(chan struct{})}
	c.cache[address] = promise
	c.mu.Unlock()

	go func() {
		promise.result.Data, promise.result.Err = c.client.Get(address)
		close(promise.done)
	}()

	return promise
}
