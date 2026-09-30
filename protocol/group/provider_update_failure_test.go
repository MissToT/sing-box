package group

import (
	"context"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/interrupt"
	U "github.com/sagernet/sing-box/common/urltest"

	"github.com/stretchr/testify/require"
)

// When a provider update cannot be applied, the group keeps the outbounds it
// already had instead of dropping them.
func TestSelectorKeepsOutboundsWhenProviderUpdateFails(t *testing.T) {
	outbound := &preMatchTestOutbound{tag: "first/outbound"}
	providers := map[string]adapter.Provider{
		"first": &providerUpdateTestProvider{
			tag:       "first",
			outbounds: []adapter.Outbound{outbound},
		},
	}
	outboundManager := &providerUpdateTestOutboundManager{
		outbounds: map[string]adapter.Outbound{outbound.Tag(): outbound},
	}
	selector := &Selector{
		ctx:            context.Background(),
		outbound:       outboundManager,
		tags:           []string{outbound.Tag()},
		outbounds:      outboundManager.outbounds,
		interruptGroup: interrupt.NewGroup(),
		providers:      providers,
		providerTags:   []string{"first"},
		outboundsCache: make(map[string][]adapter.Outbound),
	}
	selector.selected.Store(outbound)

	// the provider reported by the callback is not registered, so the rebuild fails
	require.Error(t, selector.onProviderUpdated("missing"))

	require.Equal(t, []string{outbound.Tag()}, selector.All())
	require.Equal(t, outbound, selector.Selected("tcp"))
	require.Equal(t, outbound, selector.Selected("udp"))
}

// The same holds for the other group types.
func TestURLTestKeepsOutboundsWhenProviderUpdateFails(t *testing.T) {
	outbound := &preMatchTestOutbound{tag: "first/outbound"}
	providers := map[string]adapter.Provider{
		"first": &providerUpdateTestProvider{
			tag:       "first",
			outbounds: []adapter.Outbound{outbound},
		},
	}
	outboundManager := &providerUpdateTestOutboundManager{
		outbounds: map[string]adapter.Outbound{outbound.Tag(): outbound},
	}
	group := &URLTestGroup{
		history:        U.NewHistoryStorage(),
		interruptGroup: interrupt.NewGroup(),
	}
	group.storeOutbounds([]adapter.Outbound{outbound})
	urlTest := &URLTest{
		outbound:       outboundManager,
		group:          group,
		providers:      providers,
		providerTags:   []string{"first"},
		outboundsCache: make(map[string][]adapter.Outbound),
	}

	require.Error(t, urlTest.onProviderUpdated("missing"))

	require.Equal(t, []string{outbound.Tag()}, urlTest.All())
}
