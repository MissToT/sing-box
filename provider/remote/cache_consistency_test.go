package remote

import (
	"context"
	"crypto/sha256"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/common/hash"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-box/protocol/socks"
	"github.com/sagernet/sing/service"

	"github.com/stretchr/testify/require"
)

type cacheFileStub struct {
	adapter.CacheFile
	saved *adapter.SavedBinary
}

func (c *cacheFileStub) LoadSubscription(string) *adapter.SavedBinary { return c.saved }

func (c *cacheFileStub) SaveSubscription(_ string, sub *adapter.SavedBinary) error {
	c.saved = sub
	return nil
}

type cacheTestOutboundManager struct{ adapter.DynamicOutboundManager }

func (cacheTestOutboundManager) Outbounds() []adapter.Outbound { return nil }

func (cacheTestOutboundManager) Outbound(string) (adapter.Outbound, bool) { return nil, false }

func (cacheTestOutboundManager) Default() adapter.Outbound { return nil }

func (cacheTestOutboundManager) Create(context.Context, adapter.Router, log.ContextLogger, string, string, any) error {
	return nil
}

func (cacheTestOutboundManager) Start(adapter.StartStage, *adapter.Scope) error { return nil }

func (cacheTestOutboundManager) Replace(context.Context, adapter.Router, log.ContextLogger, string, string, any) error {
	return nil
}

func (cacheTestOutboundManager) Remove(string) error { return nil }

type cacheTestEndpointManager struct{ adapter.DynamicEndpointManager }

func (cacheTestEndpointManager) Endpoints() []adapter.Endpoint { return nil }

func (cacheTestEndpointManager) Get(string) (adapter.Endpoint, bool) { return nil, false }

func (cacheTestEndpointManager) Create(context.Context, adapter.Router, log.ContextLogger, string, string, any) error {
	return nil
}

func (cacheTestEndpointManager) Start(adapter.StartStage, *adapter.Scope) error { return nil }

func (cacheTestEndpointManager) Replace(context.Context, adapter.Router, log.ContextLogger, string, string, any) error {
	return nil
}

func (cacheTestEndpointManager) Remove(string) error { return nil }

const (
	cacheTestProviderURL = "https://example.com/provider"
	cacheTestBody        = `{"outbounds":[{"type":"socks","tag":"node1","server":"127.0.0.1","server_port":1080}]}`
)

func newCacheTestContext() context.Context {
	registry := outbound.NewRegistry()
	socks.RegisterOutbound(registry)
	ctx := context.Background()
	ctx = service.ContextWith[option.OutboundOptionsRegistry](ctx, registry)
	ctx = service.ContextWith[adapter.OutboundRegistry](ctx, registry)
	ctx = service.ContextWith[adapter.OutboundManager](ctx, cacheTestOutboundManager{})
	ctx = service.ContextWith[adapter.EndpointManager](ctx, cacheTestEndpointManager{})
	return ctx
}

func newCacheTestProvider(t *testing.T, ctx context.Context, cachePath string, cache adapter.CacheFile) *ProviderRemote {
	t.Helper()
	created, err := NewProviderRemote(ctx, nil, log.NewNOPFactory(), "test", option.ProviderRemoteOptions{
		URL:  cacheTestProviderURL,
		Path: cachePath,
	})
	require.NoError(t, err)
	provider := created.(*ProviderRemote)
	provider.cacheFile = cache
	return provider
}

func cacheTestSaved(hashType hash.HashType) *adapter.SavedBinary {
	urlHash := sha256.Sum256([]byte(cacheTestProviderURL))
	return &adapter.SavedBinary{
		Hash:     hashType,
		LastEtag: "etag-1",
		URLHash:  urlHash[:],
	}
}

func notModifiedClient(info string) *http.Client {
	return &http.Client{Transport: resourceDownloadRoundTripper(func(r *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusNotModified,
			Header:     http.Header{"Subscription-Userinfo": []string{info}},
			Body:       io.NopCloser(strings.NewReader("")),
			Request:    r,
		}, nil
	})}
}

func writeCacheTestFile(t *testing.T, path string, info string) []byte {
	t.Helper()
	content := []byte(info + "\n" + cacheTestBody)
	require.NoError(t, os.WriteFile(path, content, 0o666))
	return content
}

// A 304 response rewrites the cache file, and the database entry must describe
// the file that was actually written.
func TestProviderRemoteCacheHashAfterNotModified(t *testing.T) {
	t.Parallel()
	ctx := newCacheTestContext()
	cachePath := filepath.Join(t.TempDir(), "provider.json")
	initial := writeCacheTestFile(t, cachePath, "# upload=1; download=2; total=100; expire=200;")

	cache := &cacheFileStub{saved: cacheTestSaved(hash.MakeHash(initial))}
	provider := newCacheTestProvider(t, ctx, cachePath, cache)
	loaded, err := provider.loadCacheFile()
	require.NoError(t, err)
	require.True(t, loaded)

	provider.httpClient = notModifiedClient("upload=9; download=9; total=100; expire=200;")
	require.NoError(t, provider.fetch(ctx, false))

	onDisk, err := os.ReadFile(cachePath)
	require.NoError(t, err)
	require.Equal(t, hash.MakeHash(onDisk), cache.saved.Hash,
		"the hash stored after a 304 must describe the cache file on disk")
}

// After a 304 update the next startup must still be able to use the local cache,
// even when the network is unavailable.
func TestProviderRemoteNextStartupReusesCacheAfterNotModified(t *testing.T) {
	t.Parallel()
	ctx := newCacheTestContext()
	cachePath := filepath.Join(t.TempDir(), "provider.json")
	initial := writeCacheTestFile(t, cachePath, "# upload=1; download=2; total=100; expire=200;")

	cache := &cacheFileStub{saved: cacheTestSaved(hash.MakeHash(initial))}

	first := newCacheTestProvider(t, ctx, cachePath, cache)
	loaded, err := first.loadCacheFile()
	require.NoError(t, err)
	require.True(t, loaded)
	first.httpClient = notModifiedClient("upload=9; download=9; total=100; expire=200;")
	require.NoError(t, first.fetch(ctx, false))

	second := newCacheTestProvider(t, ctx, cachePath, cache)
	loaded, err = second.loadCacheFile()
	require.NoError(t, err)
	require.True(t, loaded, "the next startup must reuse the local cache instead of refetching")
}

// A database entry written before the provider had a cache path carries no hash;
// that must not invalidate a perfectly readable cache file.
func TestProviderRemoteLoadsCacheWithoutStoredHash(t *testing.T) {
	t.Parallel()
	ctx := newCacheTestContext()
	cachePath := filepath.Join(t.TempDir(), "provider.json")
	writeCacheTestFile(t, cachePath, "# upload=1; download=2; total=100; expire=200;")

	cache := &cacheFileStub{saved: cacheTestSaved(hash.HashType{})}
	provider := newCacheTestProvider(t, ctx, cachePath, cache)
	loaded, err := provider.loadCacheFile()
	require.NoError(t, err)
	require.True(t, loaded, "a missing hash must not discard a readable cache file")
	require.True(t, cache.saved.Hash.IsValid(), "the database entry must be repaired with the file hash")
}

// A cache file that genuinely cannot be used still falls back to a refetch.
func TestProviderRemoteRejectsUnreadableCacheFile(t *testing.T) {
	t.Parallel()
	ctx := newCacheTestContext()
	cachePath := filepath.Join(t.TempDir(), "provider.json")
	require.NoError(t, os.WriteFile(cachePath, []byte("not a subscription"), 0o666))

	cache := &cacheFileStub{saved: cacheTestSaved(hash.MakeHash([]byte("not a subscription")))}
	provider := newCacheTestProvider(t, ctx, cachePath, cache)
	loaded, err := provider.loadCacheFile()
	require.Error(t, err)
	require.False(t, loaded)
}
