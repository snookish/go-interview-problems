package main

import (
	"errors"
	"sync"
)

type Fetcher interface {
	Fetch(url string) (body string, urls []string, err error)
}

type crawler struct {
	fetcher Fetcher
	mu      sync.Mutex
	visited map[string]struct{}
	sem     chan struct{}
}

func Crawl(url string, depth int, fetcher Fetcher) ([]string, error) {
	if depth <= 0 {
		return nil, nil
	}

	c := &crawler{
		fetcher: fetcher,
		visited: make(map[string]struct{}),
		sem:     make(chan struct{}, 10), // At most 10 concurrent fetches.
	}

	return c.crawl(url, depth)
}

func (c *crawler) crawl(url string, depth int) ([]string, error) {
	if depth <= 0 {
		return nil, nil
	}

	c.mu.Lock()
	if _, exists := c.visited[url]; exists {
		c.mu.Unlock()
		return nil, nil
	}
	c.visited[url] = struct{}{}
	c.mu.Unlock()

	c.sem <- struct{}{}
	body, urls, err := c.fetcher.Fetch(url)
	<-c.sem

	if err != nil {
		return nil, nil // fmt.Errorf("fetch %q: %w", url, err)
	}

	errs := make([]error, len(urls))
	results := make([][]string, len(urls))

	var wg sync.WaitGroup
	wg.Add(len(urls))

	for i, u := range urls {
		go func(i int, u string) {
			defer wg.Done()
			results[i], errs[i] = c.crawl(u, depth-1)
		}(i, u)
	}

	wg.Wait()

	result := []string{body}
	for _, childResults := range results {
		result = append(result, childResults...)
	}

	return result, errors.Join(errs...)
}
