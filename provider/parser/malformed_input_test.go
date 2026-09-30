package parser

import (
	"context"
	"testing"

	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-box/protocol/socks"
	"github.com/sagernet/sing/service"

	"github.com/stretchr/testify/require"
)

// Building typed outbound options needs the options registry in the context.
func boxSubscriptionContext() context.Context {
	registry := outbound.NewRegistry()
	socks.RegisterOutbound(registry)
	return service.ContextWith[option.OutboundOptionsRegistry](context.Background(), registry)
}

// Subscription content is untrusted, so a malformed document has to come back as
// an error instead of panicking.
func TestBoxSubscriptionRejectsMalformedOutbounds(t *testing.T) {
	cases := []struct {
		name    string
		content string
	}{
		{"outbounds is object", `{"outbounds":{}}`},
		{"outbounds is null", `{"outbounds":null}`},
		{"outbounds is string", `{"outbounds":"x"}`},
		{"outbounds is number", `{"outbounds":1}`},
		{"outbounds is bool", `{"outbounds":true}`},
		{"outbound element is string", `{"outbounds":["x"]}`},
		{"outbound element is number", `{"outbounds":[1]}`},
		{"outbound element is null", `{"outbounds":[null]}`},
		{"outbound element is array", `{"outbounds":[[]]}`},
		{"type is number", `{"outbounds":[{"type":123}]}`},
		{"type is null", `{"outbounds":[{"type":null}]}`},
		{"type is object", `{"outbounds":[{"type":{}}]}`},
		{"type is array", `{"outbounds":[{"type":[]}]}`},
		{"type is bool", `{"outbounds":[{"type":true}]}`},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			_, _, err := ParseBoxSubscription(context.Background(), testCase.content)
			require.Error(t, err)
		})
	}
}

func TestBoxSubscriptionAcceptsWellFormedDocument(t *testing.T) {
	outbounds, _, err := ParseBoxSubscription(boxSubscriptionContext(),
		`{"outbounds":[{"type":"socks","tag":"node1","server":"127.0.0.1","server_port":1080}]}`)
	require.NoError(t, err)
	require.Len(t, outbounds, 1)
	require.Equal(t, "node1", outbounds[0].Tag)
}

// Built-in types are still filtered out of a valid array.
func TestBoxSubscriptionFiltersBuiltInTypes(t *testing.T) {
	outbounds, _, err := ParseBoxSubscription(boxSubscriptionContext(),
		`{"outbounds":[{"type":"direct","tag":"builtin"},`+
			`{"type":"socks","tag":"node1","server":"127.0.0.1","server_port":1080}]}`)
	require.NoError(t, err)
	require.Len(t, outbounds, 1)
	require.Equal(t, "node1", outbounds[0].Tag)
}

// A null YAML document used to nil out the target pointer and fault on the
// field access that follows.
func TestClashSubscriptionHandlesNullDocument(t *testing.T) {
	for _, content := range []string{"null", "~", "---\n", "null\n"} {
		t.Run(content, func(t *testing.T) {
			outbounds, endpoints, err := ParseClashSubscription(context.Background(), content)
			require.NoError(t, err)
			require.Empty(t, outbounds)
			require.Empty(t, endpoints)
		})
	}
}

func TestClashSubscriptionHandlesEmptyDocument(t *testing.T) {
	for _, content := range []string{"", "proxies: []", "proxies: null"} {
		t.Run(content, func(t *testing.T) {
			outbounds, endpoints, err := ParseClashSubscription(context.Background(), content)
			require.NoError(t, err)
			require.Empty(t, outbounds)
			require.Empty(t, endpoints)
		})
	}
}

// The dispatcher must survive adversarial content: whichever parser rejects it,
// the call returns an error rather than panicking.
func TestParseSubscriptionHandlesAdversarialContent(t *testing.T) {
	inputs := []string{
		`{"outbounds":{}}`, `{"outbounds":null}`, `{"outbounds":"x"}`, `{"outbounds":1}`,
		`{"outbounds":true}`, `{"outbounds":[null]}`, `{"outbounds":[[]]}`, `{"outbounds":[1]}`,
		`{"outbounds":["x"]}`, `{"outbounds":[{"type":123}]}`, `{"outbounds":[{"type":null}]}`,
		`{"outbounds":[{"type":{}}]}`, `{"outbounds":[{"type":[]}]}`, `{"outbounds":[{"type":true}]}`,
		`{"outbounds":[{}]}`, `{"endpoints":{}}`, `{"endpoints":["x"]}`, `{"endpoints":[{"type":1}]}`,
		`null`, `~`, `---`, `[]`, `1`, `"x"`, ``, `{`, `}`, `{"outbounds":`,
		`proxies: {}`, `proxies: [null]`, `proxies: [1]`, `proxies: [{}]`,
		`proxies: [{name: a, type: ss}]`, `proxies: [{name: a, type: ss, port: -1}]`,
		`{"servers":{}}`, `{"servers":[null]}`, `{"servers":["x"]}`,
		`ss://`, `ss://@`, `ss://:@`, `ss://bad@base64#t`, `vmess://`, `vmess://!!!`,
		`vless://`, `trojan://`, `hysteria://`, `hysteria2://`, `tuic://`, `anytls://`,
		`\n\n\n`, `   `, `#comment`,
	}
	for _, content := range inputs {
		t.Run(content, func(t *testing.T) {
			require.NotPanics(t, func() {
				_, _, _ = ParseSubscription(context.Background(), content, nil, nil, nil, "provider")
			})
		})
	}
}
