package dialer

import (
	"net/netip"
	"sync"
	"time"
)

const concurrentWinnerCacheTTL = 5 * time.Minute

type concurrentWinnerEntry struct {
	address   netip.Addr
	expiresAt time.Time
}

// concurrentWinnerCache 记录每个域名上一次 TCP 并发竞速胜出的地址,
// 用于下一次同域名拨号时优先尝试该地址,减少全量并发竞速的开销。
type concurrentWinnerCache struct {
	mu      sync.Mutex
	entries map[string]concurrentWinnerEntry
}

var globalConcurrentWinnerCache = &concurrentWinnerCache{
	entries: make(map[string]concurrentWinnerEntry),
}

// get 返回缓存的胜出地址,如果缓存不存在、已过期,或不在 candidates 中(说明 DNS 结果已变化),
// 则返回 zero value 和 false,并清除该缓存项。
func (c *concurrentWinnerCache) get(domain string, candidates []netip.Addr) (netip.Addr, bool) {
	if domain == "" {
		return netip.Addr{}, false
	}
	c.mu.Lock()
	entry, loaded := c.entries[domain]
	c.mu.Unlock()
	if !loaded {
		return netip.Addr{}, false
	}
	if time.Now().After(entry.expiresAt) {
		c.mu.Lock()
		delete(c.entries, domain)
		c.mu.Unlock()
		return netip.Addr{}, false
	}
	found := false
	for _, addr := range candidates {
		if addr == entry.address {
			found = true
			break
		}
	}
	if !found {
		c.mu.Lock()
		delete(c.entries, domain)
		c.mu.Unlock()
		return netip.Addr{}, false
	}
	return entry.address, true
}

func (c *concurrentWinnerCache) set(domain string, address netip.Addr) {
	if domain == "" {
		return
	}
	c.mu.Lock()
	c.entries[domain] = concurrentWinnerEntry{
		address: address,
		expiresAt: time.Now().Add(concurrentWinnerCacheTTL),
	}
	c.mu.Unlock()
}