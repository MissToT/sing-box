package group

import (
	"context"
	"testing"

	"github.com/sagernet/sing-box/common/urltest"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing/service"

	"github.com/stretchr/testify/require"
)

func TestDefaultTestLink(t *testing.T) {
	require.Equal(t, "https://captive.apple.com/generate_204", urltest.DefaultTestLink)
}

// A group without a configured url falls back to the shared default link.
func TestLoadBalanceUsesDefaultTestLink(t *testing.T) {
	ctx := service.ContextWithPtr[urltest.HistoryStorage](context.Background(), urltest.NewHistoryStorage())
	group, err := NewLoadBalanceGroup(ctx, nil, log.NewNOPFactory().Logger(), nil, "", 0, 0, 0, "")
	require.NoError(t, err)
	require.Equal(t, urltest.DefaultTestLink, group.link)
}

func TestLoadBalanceKeepsConfiguredLink(t *testing.T) {
	ctx := service.ContextWithPtr[urltest.HistoryStorage](context.Background(), urltest.NewHistoryStorage())
	group, err := NewLoadBalanceGroup(ctx, nil, log.NewNOPFactory().Logger(), nil, "https://example.com/204", 0, 0, 0, "")
	require.NoError(t, err)
	require.Equal(t, "https://example.com/204", group.link)
}
