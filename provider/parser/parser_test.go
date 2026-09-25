package parser

import (
	"testing"

	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common"

	"github.com/stretchr/testify/require"
)

func TestOverrideAnyTLSOptions(t *testing.T) {
	testCases := []struct {
		name                   string
		clientMetadata         *string
		disableReuse           bool
		override               *option.OverrideAnyTLSOptions
		expectedClientMetadata *string
		expectedDisableReuse   bool
	}{
		{
			name: "preserve unset",
		},
		{
			name:                   "preserve value",
			clientMetadata:         common.Ptr("original-client/1.0"),
			disableReuse:           true,
			expectedClientMetadata: common.Ptr("original-client/1.0"),
			expectedDisableReuse:   true,
		},
		{
			name:           "clear",
			clientMetadata: common.Ptr("original-client/1.0"),
			disableReuse:   true,
			override: &option.OverrideAnyTLSOptions{
				ClientMetadata: common.Ptr(""),
				DisableReuse:   common.Ptr(false),
			},
			expectedClientMetadata: common.Ptr(""),
		},
		{
			name: "replace",
			override: &option.OverrideAnyTLSOptions{
				ClientMetadata: common.Ptr("custom-client/1.0"),
				DisableReuse:   common.Ptr(true),
			},
			expectedClientMetadata: common.Ptr("custom-client/1.0"),
			expectedDisableReuse:   true,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			outbounds := overrideOutbounds([]option.Outbound{{
				Type: C.TypeAnyTLS,
				Options: &option.AnyTLSOutboundOptions{
					ClientMetadata: testCase.clientMetadata,
					DisableReuse:   testCase.disableReuse,
				},
			}}, nil, nil, testCase.override, nil, "")
			options := outbounds[0].Options.(*option.AnyTLSOutboundOptions)
			require.Equal(t, testCase.expectedClientMetadata, options.ClientMetadata)
			require.Equal(t, testCase.expectedDisableReuse, options.DisableReuse)
		})
	}
}

func TestOverrideDialerOptionDetour(t *testing.T) {
	// In-subscription detours must be rewritten to the provider-prefixed final
	// tag of the referenced node, which uses the "[tag] " format.
	testCases := []struct {
		name        string
		detour      string
		tags        []string
		providerTag string
		expected    string
	}{
		{
			name:        "prefix in-subscription detour",
			detour:      "node1",
			tags:        []string{"node1", "node2"},
			providerTag: "myprov",
			expected:    "[myprov] node1",
		},
		{
			name:     "keep in-subscription detour without provider tag",
			detour:   "node1",
			tags:     []string{"node1", "node2"},
			expected: "node1",
		},
		{
			name:        "drop detour referencing unknown tag",
			detour:      "elsewhere",
			tags:        []string{"node1", "node2"},
			providerTag: "myprov",
			expected:    "",
		},
		{
			name:        "keep empty detour",
			tags:        []string{"node1"},
			providerTag: "myprov",
			expected:    "",
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			options := overrideDialerOption(option.DialerOptions{Detour: testCase.detour}, nil, testCase.tags, testCase.providerTag)
			require.Equal(t, testCase.expected, options.Detour)
		})
	}
}
