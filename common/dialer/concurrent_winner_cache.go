package dialer

import (
	"net/netip"
	"sync"
	"time"
)

// concurrentWinnerCacheTTL 缓存条目的有效期，超过该时间后缓存视为失效，
// 需要重新走全量并发竞速以适应可能的 DNS 记录变化。
const concurrentWinnerCacheTTL = 5 * time.Minute

// concurrentWinnerEntry 记录某个域名上一次并发竞速胜出的地址及更新时间。
type concurrentWinnerEntry struct {
	address   netip.Addr
	updatedAt time.Time
}

// concurrentWinnerCache 是一个按域名(FQDN)缓存"上次胜出 IP"的线程安全缓存，
// 用于 tcp_concurrent 场景下优先复用上次竞速胜出的地址，减少不必要的并发拨号。
type concurrentWinnerCache struct {
	musync.RWMutex
	entries map[string]concurrentWinnerEntry
}

var globalConcurrentWinnerCache = &concurrentWinnerCache{
	entries: make(map[string]concurrentWinnerEntry),
}

// get 返回 domain 对应的缓存地址；若不存在或已过期则返回 zero-value 和 false。
func (c *concurrentWinnerCache) get(domain string) (netip.Addr, bool) {
	if domain == "" {
		return netip.Addr{}, false
	}
	c.mu.RLock()
	entry, loaded := c.entries[domain]
	c.mu.RUnlock()
	if !loaded {
		return netip.Addr{}, false
	}
	if time.Since(entry.updatedAt) > concurrentWinnerCacheTTL {
		return netip.Addr{}, false
	}
	return entry.address, true
}

// set 记录 domain 对应的最新胜出地址。
func (c *concurrentWinnerCache) set(domain string, address netip.Addr) {
	if domain == "" {
		return
	}
	c.mu.Lock()
	c.entries[domain] = concurrentWinnerEntry{address: address, updatedAt: time.Now()}
	c.mu.Unlock()
}

// containsAddress 判断 address 是否仍然存在于 addresses 列表中，
// 用于避免使用已经不在最新 DNS 解析结果里的陈旧缓存地址。
func containsAddress(addresses []netip.Addr, address netip.Addr) bool {
	for _, a := range addresses {
		if a == address {
			return true
		}
	}
	return false
}