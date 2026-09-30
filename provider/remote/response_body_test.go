package remote

import (
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"

	"github.com/stretchr/testify/require"
)

type closeTrackingBody struct {
	io.Reader
	closed atomic.Bool
}

func (b *closeTrackingBody) Close() error {
	b.closed.Store(true)
	return nil
}

// Every response has to be closed, including the ones the update path returns
// from early, otherwise each 304 leaks a connection.
func TestProviderRemoteClosesResponseBody(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name   string
		status int
	}{
		{"OK", http.StatusOK},
		{"NotModified", http.StatusNotModified},
		{"UnexpectedStatus", http.StatusInternalServerError},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			ctx := newCacheTestContext()
			body := &closeTrackingBody{Reader: strings.NewReader(cacheTestBody)}
			created, err := NewProviderRemote(ctx, nil, log.NewNOPFactory(), "test", option.ProviderRemoteOptions{
				URL: cacheTestProviderURL,
			})
			require.NoError(t, err)
			provider := created.(*ProviderRemote)
			provider.httpClient = &http.Client{Transport: resourceDownloadRoundTripper(func(r *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: testCase.status,
					Header:     http.Header{},
					Body:       body,
					Request:    r,
				}, nil
			})}

			_ = provider.fetch(ctx, false)

			require.True(t, body.closed.Load(), "the response body must be closed on every path")
		})
	}
}
