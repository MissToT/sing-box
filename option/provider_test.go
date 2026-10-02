package option

import (
	"strconv"
	"testing"
	"time"

	"github.com/sagernet/sing/common/json"
	"github.com/sagernet/sing/common/json/badoption"

	"github.com/stretchr/testify/require"
)

func TestProviderRemotePathConflict(t *testing.T) {
	var options ProviderRemoteOptions
	err := json.Unmarshal([]byte(`{
		"url": "https://example.com/provider.json",
		"path": "provider.json",
		"initial_path": "initial-provider.json"
	}`), &options)
	require.ErrorContains(t, err, "path and initial_path are mutually exclusive")
}

func TestProviderRemoteSinglePath(t *testing.T) {
	for _, content := range []string{
		`{"url":"https://example.com/provider.json","path":"provider.json"}`,
		`{"url":"https://example.com/provider.json","initial_path":"initial-provider.json"}`,
	} {
		var options ProviderRemoteOptions
		require.NoError(t, json.Unmarshal([]byte(content), &options))
	}
}

func TestProviderHealthCheckBooleanShorthand(t *testing.T) {
	for _, testCase := range []struct {
		content string
		enabled bool
	}{
		{`{"url":"https://example.com/provider.json","health_check":true}`, true},
		{`{"url":"https://example.com/provider.json","health_check":false}`, false},
	} {
		var options ProviderRemoteOptions
		require.NoError(t, json.Unmarshal([]byte(testCase.content), &options))
		require.NotNil(t, options.HealthCheck.Enabled)
		require.Equal(t, testCase.enabled, *options.HealthCheck.Enabled)
	}
}

// An absent health_check, and an object form that omits enabled, both leave the
// field unset so the provider runs health checks by default.
func TestProviderHealthCheckDefaultsToUnset(t *testing.T) {
	for _, content := range []string{
		`{"url":"https://example.com/provider.json"}`,
		`{"url":"https://example.com/provider.json","health_check":{"url":"https://example.com/204"}}`,
		`{"url":"https://example.com/provider.json","health_check":{"interval":"10m"}}`,
	} {
		var options ProviderRemoteOptions
		require.NoError(t, json.Unmarshal([]byte(content), &options))
		require.Nil(t, options.HealthCheck.Enabled, content)
	}
}

func TestProviderHealthCheckObjectForm(t *testing.T) {
	var options ProviderRemoteOptions
	require.NoError(t, json.Unmarshal([]byte(`{
		"url": "https://example.com/provider.json",
		"health_check": {
			"enabled": false,
			"url": "https://captive.apple.com/generate_204",
			"interval": "10m",
			"timeout": "3s"
		}
	}`), &options))
	require.NotNil(t, options.HealthCheck.Enabled)
	require.False(t, *options.HealthCheck.Enabled)
	require.Equal(t, "https://captive.apple.com/generate_204", options.HealthCheck.URL)
	require.Equal(t, badoption.Duration(10*time.Minute), options.HealthCheck.Interval)
	require.Equal(t, badoption.Duration(3*time.Second), options.HealthCheck.Timeout)
}

func TestProviderHealthCheckRejectsUnknownField(t *testing.T) {
	var options ProviderRemoteOptions
	err := json.Unmarshal([]byte(`{
		"url": "https://example.com/provider.json",
		"health_check": {"enabled": true, "bogus": 1}
	}`), &options)
	require.Error(t, err)
}

// The shorthand survives a marshal round trip.
func TestProviderHealthCheckMarshalShorthand(t *testing.T) {
	for _, enabled := range []bool{true, false} {
		content, err := json.Marshal(ProviderHealthCheckOptions{Enabled: &enabled})
		require.NoError(t, err)
		require.Equal(t, strconv.FormatBool(enabled), string(content))

		var decoded ProviderHealthCheckOptions
		require.NoError(t, json.Unmarshal(content, &decoded))
		require.NotNil(t, decoded.Enabled)
		require.Equal(t, enabled, *decoded.Enabled)
	}
}

func TestProviderHealthCheckMarshalObjectWhenConfigured(t *testing.T) {
	content, err := json.Marshal(ProviderHealthCheckOptions{URL: "https://example.com/204"})
	require.NoError(t, err)
	require.Contains(t, string(content), "https://example.com/204")
}
