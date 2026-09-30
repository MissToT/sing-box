package provider

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	E "github.com/sagernet/sing/common/exceptions"

	"github.com/stretchr/testify/require"
)

// A group that cannot rebuild keeps its previous outbounds, so a failing callback
// has to reach the log instead of being dropped.
func TestUpdateGroupsReportsCallbackFailure(t *testing.T) {
	var buffer bytes.Buffer
	logFactory := log.NewDefaultFactory(
		context.Background(),
		log.Formatter{BaseTime: time.Now(), DisableColors: true},
		&buffer,
		"",
		nil,
		false,
	)
	require.NoError(t, logFactory.Start())
	adapter := NewAdapter(
		context.Background(),
		nil,
		nil,
		nil,
		logFactory,
		logFactory.NewLogger("test"),
		"my-provider",
		"remote",
		option.ProviderHealthCheckOptions{},
	)
	adapter.RegisterCallback(func(tag string) error {
		return E.New("group rebuild failed for ", tag)
	})

	adapter.UpdateGroups()

	require.Contains(t, buffer.String(), "group rebuild failed for my-provider")
	require.Contains(t, buffer.String(), "my-provider")
}

// A callback that succeeds stays silent.
func TestUpdateGroupsStaysSilentOnSuccess(t *testing.T) {
	var buffer bytes.Buffer
	logFactory := log.NewDefaultFactory(
		context.Background(),
		log.Formatter{BaseTime: time.Now(), DisableColors: true},
		&buffer,
		"",
		nil,
		false,
	)
	require.NoError(t, logFactory.Start())
	adapter := NewAdapter(
		context.Background(),
		nil,
		nil,
		nil,
		logFactory,
		logFactory.NewLogger("test"),
		"my-provider",
		"remote",
		option.ProviderHealthCheckOptions{},
	)
	called := 0
	adapter.RegisterCallback(func(tag string) error {
		called++
		require.Equal(t, "my-provider", tag)
		return nil
	})

	adapter.UpdateGroups()

	require.Equal(t, 1, called)
	require.Empty(t, buffer.String())
}
