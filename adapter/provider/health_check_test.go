package provider

import (
	"context"
	"testing"

	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"

	"github.com/stretchr/testify/require"
)

// An absent health_check leaves Enabled nil, which has to mean enabled.
func TestNewAdapterHealthCheckDefaultsToEnabled(t *testing.T) {
	newAdapter := func(options option.ProviderHealthCheckOptions) Adapter {
		return NewAdapter(
			context.Background(),
			nil,
			nil,
			nil,
			log.NewNOPFactory(),
			log.NewNOPFactory().NewLogger("test"),
			"tag",
			"remote",
			options,
		)
	}

	require.True(t, newAdapter(option.ProviderHealthCheckOptions{}).enabled)

	disabled := false
	require.False(t, newAdapter(option.ProviderHealthCheckOptions{Enabled: &disabled}).enabled)

	enabled := true
	require.True(t, newAdapter(option.ProviderHealthCheckOptions{Enabled: &enabled}).enabled)

	// configured but without an explicit enabled still defaults to on
	require.True(t, newAdapter(option.ProviderHealthCheckOptions{URL: "https://example.com/204"}).enabled)
}
