package dialer

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	C "github.com/sagernet/sing-box/constant"
	boxLog "github.com/sagernet/sing-box/log"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/service"

	"github.com/stretchr/testify/require"
)

type resolveTestRouter struct {
	adapter.DNSRouter
	addresses []netip.Addr
	strategy  C.DomainStrategy
}

func (r *resolveTestRouter) Lookup(context.Context, string, adapter.DNSQueryOptions) ([]netip.Addr, error) {
	return r.addresses, nil
}

func (r *resolveTestRouter) ResolveLookupStrategy(options adapter.DNSQueryOptions) C.DomainStrategy {
	if options.LookupStrategy != C.DomainStrategyAsIS {
		return options.LookupStrategy
	}
	if options.Strategy != C.DomainStrategyAsIS {
		return options.Strategy
	}
	return r.strategy
}

type resolveTestNetworkManager struct {
	adapter.NetworkManager
	defaultOptions adapter.NetworkOptions
}

type resolveTestLogFactory struct {
	boxLog.Factory
	logger boxLog.ContextLogger
}

type resolveTestLogger struct {
	*concurrentTestLogger
}

func (l *resolveTestLogger) InfoContext(ctx context.Context, args ...any) {
	if boxLog.OverrideLevelFromContext(boxLog.LevelInfo, ctx) == boxLog.LevelDebug {
		l.DebugContext(ctx, args...)
		return
	}
	l.concurrentTestLogger.InfoContext(ctx, args...)
}

func (f *resolveTestLogFactory) NewLogger(string) boxLog.ContextLogger {
	return f.logger
}

func (m *resolveTestNetworkManager) DefaultOptions() adapter.NetworkOptions {
	return m.defaultOptions
}

func TestResolveDialerUsesConcurrentTCPDialFromNetworkOptions(t *testing.T) {
	slowAddress := netip.MustParseAddr("127.0.0.21")
	fastAddress := netip.MustParseAddr("127.0.0.22")
	testDialer := &concurrentTestDialer{behaviors: map[netip.Addr]concurrentTestBehavior{
		slowAddress: {delay: 50 * time.Millisecond},
		fastAddress: {delay: time.Millisecond},
	}}
	testLogger := &resolveTestLogger{newConcurrentTestLogger()}
	ctx := newResolveTestContext([]netip.Addr{slowAddress, fastAddress}, true)
	ctx = service.ContextWith[boxLog.Factory](ctx, &resolveTestLogFactory{logger: testLogger})
	resolveDialer := NewResolveDialer(ctx, testDialer, false, "", adapter.DNSQueryOptions{}, 0)

	conn, err := resolveDialer.DialContext(ctx, N.NetworkTCP, M.ParseSocksaddrHostPort("example.com", 443))
	require.NoError(t, err)
	require.Equal(t, "127.0.0.22:443", conn.RemoteAddr().String())
	require.GreaterOrEqual(t, testDialer.MaxActive(), 2)
	debugMessages, infoMessages := testLogger.Messages()
	require.Empty(t, debugMessages)
	require.Equal(t, []string{"concurrent dial winner 127.0.0.22 (example.com:443)"}, infoMessages)
	require.NoError(t, conn.Close())
}

func TestResolveDialerKeepsUDPDialSerialWhenConcurrentEnabled(t *testing.T) {
	firstAddress := netip.MustParseAddr("127.0.0.23")
	secondAddress := netip.MustParseAddr("127.0.0.24")
	testDialer := &concurrentTestDialer{behaviors: map[netip.Addr]concurrentTestBehavior{
		firstAddress:  {delay: time.Millisecond},
		secondAddress: {delay: time.Millisecond},
	}}
	ctx := newResolveTestContext([]netip.Addr{firstAddress, secondAddress}, true)
	resolveDialer := NewResolveDialer(ctx, testDialer, false, "", adapter.DNSQueryOptions{}, 0)

	conn, err := resolveDialer.DialContext(ctx, N.NetworkUDP, M.ParseSocksaddrHostPort("example.com", 443))
	require.NoError(t, err)
	require.Equal(t, "127.0.0.23:443", conn.RemoteAddr().String())
	require.Equal(t, 1, testDialer.MaxActive())
	require.NoError(t, conn.Close())
}

func newResolveTestContext(addresses []netip.Addr, concurrentDial bool) context.Context {
	ctx := service.ContextWith[adapter.DNSRouter](context.Background(), &resolveTestRouter{addresses: addresses})
	return service.ContextWith[adapter.NetworkManager](ctx, &resolveTestNetworkManager{
		defaultOptions: adapter.NetworkOptions{TCPConcurrentDial: concurrentDial},
	})
}

func TestResolveDialerConcurrentTCPDialAllCandidatesFailed(t *testing.T) {
	firstAddress := netip.MustParseAddr("127.0.0.31")
	secondAddress := netip.MustParseAddr("127.0.0.32")
	dialError := errors.New("dial failed")
	testDialer := &concurrentTestDialer{behaviors: map[netip.Addr]concurrentTestBehavior{
		firstAddress:  {err: dialError},
		secondAddress: {err: dialError},
	}}
	testLogger := &resolveTestLogger{newConcurrentTestLogger()}
	ctx := newResolveTestContext([]netip.Addr{firstAddress, secondAddress}, true)
	ctx = service.ContextWith[boxLog.Factory](ctx, &resolveTestLogFactory{logger: testLogger})
	resolveDialer := NewResolveDialer(ctx, testDialer, false, "", adapter.DNSQueryOptions{}, 0)

	conn, err := resolveDialer.DialContext(ctx, N.NetworkTCP, M.ParseSocksaddrHostPort("example.com", 443))
	require.Nil(t, conn)
	require.ErrorIs(t, err, dialError)
	warnMessages := testLogger.WarnMessages()
	require.Equal(t, []string{"concurrent dial all candidates failed (example.com:443)"}, warnMessages)
	debugMessages, infoMessages := testLogger.Messages()
	require.Empty(t, debugMessages)
	require.Empty(t, infoMessages)
}

func TestResolveDialerConcurrentTCPDialPrefersAddressFamily(t *testing.T) {
	// prefer_ipv4：只并发拨 IPv4，其中一条成功即返回，IPv6 不应被拨到
	// （并发度若为 4 就说明没有分批）。
	slowIPv4 := netip.MustParseAddr("192.0.2.11")
	fastIPv4 := netip.MustParseAddr("192.0.2.12")
	firstIPv6 := netip.MustParseAddr("2001:db8::11")
	secondIPv6 := netip.MustParseAddr("2001:db8::12")
	testDialer := &concurrentTestDialer{behaviors: map[netip.Addr]concurrentTestBehavior{
		slowIPv4:   {delay: 50 * time.Millisecond},
		fastIPv4:   {delay: time.Millisecond},
		firstIPv6:  {delay: time.Millisecond},
		secondIPv6: {delay: time.Millisecond},
	}}
	testLogger := &resolveTestLogger{newConcurrentTestLogger()}
	ctx := newResolveTestContext([]netip.Addr{firstIPv6, slowIPv4, secondIPv6, fastIPv4}, true)
	ctx = service.ContextWith[boxLog.Factory](ctx, &resolveTestLogFactory{logger: testLogger})
	resolveDialer := NewResolveDialer(ctx, testDialer, false, "", adapter.DNSQueryOptions{Strategy: C.DomainStrategyPreferIPv4}, 0)

	conn, err := resolveDialer.DialContext(ctx, N.NetworkTCP, M.ParseSocksaddrHostPort("example.com", 443))
	require.NoError(t, err)
	require.Equal(t, "192.0.2.12:443", conn.RemoteAddr().String())
	require.Equal(t, 2, testDialer.MaxActive(), "只应并发拨优先地址族")
	debugMessages, infoMessages := testLogger.Messages()
	require.Empty(t, debugMessages)
	require.Equal(t, []string{"concurrent dial winner 192.0.2.12 (example.com:443)"}, infoMessages)
	require.Empty(t, testLogger.WarnMessages())
	require.NoError(t, conn.Close())
}

func TestResolveDialerConcurrentTCPDialFallsBackToOtherFamily(t *testing.T) {
	// prefer_ipv4：IPv4 全部失败后才并发拨 IPv6。日志落点照搬实现：
	// 优先族那一批失败会记一条「全部候选失败」，最终成功再记一条 winner。
	failedIPv4First := netip.MustParseAddr("192.0.2.21")
	failedIPv4Second := netip.MustParseAddr("192.0.2.22")
	fastIPv6 := netip.MustParseAddr("2001:db8::21")
	slowIPv6 := netip.MustParseAddr("2001:db8::22")
	dialError := errors.New("ipv4 dial failed")
	testDialer := &concurrentTestDialer{behaviors: map[netip.Addr]concurrentTestBehavior{
		failedIPv4First:  {err: dialError},
		failedIPv4Second: {err: dialError},
		fastIPv6:         {delay: time.Millisecond},
		slowIPv6:         {delay: 50 * time.Millisecond},
	}}
	testLogger := &resolveTestLogger{newConcurrentTestLogger()}
	ctx := newResolveTestContext([]netip.Addr{failedIPv4First, fastIPv6, failedIPv4Second, slowIPv6}, true)
	ctx = service.ContextWith[boxLog.Factory](ctx, &resolveTestLogFactory{logger: testLogger})
	resolveDialer := NewResolveDialer(ctx, testDialer, false, "", adapter.DNSQueryOptions{Strategy: C.DomainStrategyPreferIPv4}, 0)

	conn, err := resolveDialer.DialContext(ctx, N.NetworkTCP, M.ParseSocksaddrHostPort("example.com", 443))
	require.NoError(t, err)
	require.Equal(t, "[2001:db8::21]:443", conn.RemoteAddr().String(), "优先族失败后应改用另一族")
	require.Equal(t, []string{"concurrent dial all candidates failed (example.com:443)"}, testLogger.WarnMessages())
	_, infoMessages := testLogger.Messages()
	require.Equal(t, []string{"concurrent dial winner 2001:db8::21 (example.com:443)"}, infoMessages)
	require.NoError(t, conn.Close())
}

func TestResolveDialerConcurrentTCPDialBothFamiliesFailed(t *testing.T) {
	// prefer_ipv4：两族都失败时，两批各自记一条「全部候选失败」（照搬实现的日志落点）。
	// 每族给两个地址，否则批内只有一个候选会退化成串行拨号，那条路径不记日志。
	failedIPv4First := netip.MustParseAddr("192.0.2.31")
	failedIPv4Second := netip.MustParseAddr("192.0.2.32")
	failedIPv6First := netip.MustParseAddr("2001:db8::31")
	failedIPv6Second := netip.MustParseAddr("2001:db8::32")
	dialError := errors.New("dial failed")
	testDialer := &concurrentTestDialer{behaviors: map[netip.Addr]concurrentTestBehavior{
		failedIPv4First:  {err: dialError},
		failedIPv4Second: {err: dialError},
		failedIPv6First:  {err: dialError},
		failedIPv6Second: {err: dialError},
	}}
	testLogger := &resolveTestLogger{newConcurrentTestLogger()}
	ctx := newResolveTestContext([]netip.Addr{failedIPv4First, failedIPv6First, failedIPv4Second, failedIPv6Second}, true)
	ctx = service.ContextWith[boxLog.Factory](ctx, &resolveTestLogFactory{logger: testLogger})
	resolveDialer := NewResolveDialer(ctx, testDialer, false, "", adapter.DNSQueryOptions{Strategy: C.DomainStrategyPreferIPv4}, 0)

	conn, err := resolveDialer.DialContext(ctx, N.NetworkTCP, M.ParseSocksaddrHostPort("example.com", 443))
	require.Nil(t, conn)
	require.ErrorIs(t, err, dialError)
	require.Equal(t, []string{
		"concurrent dial all candidates failed (example.com:443)",
		"concurrent dial all candidates failed (example.com:443)",
	}, testLogger.WarnMessages())
	debugMessages, infoMessages := testLogger.Messages()
	require.Empty(t, debugMessages)
	require.Empty(t, infoMessages)
}

type controlledResolveDialer struct {
	N.Dialer
	started chan netip.Addr
	results map[netip.Addr]chan error
}

func (d *controlledResolveDialer) DialContext(ctx context.Context, _ string, destination M.Socksaddr) (net.Conn, error) {
	d.started <- destination.Addr
	select {
	case err := <-d.results[destination.Addr]:
		if err != nil {
			return nil, err
		}
		return &concurrentTestConn{remoteAddr: concurrentTestAddr(destination.String())}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type controlledResolveInterfaceDialer struct {
	*controlledResolveDialer
}

func (d *controlledResolveInterfaceDialer) DialParallelInterface(ctx context.Context, network string, destination M.Socksaddr, _ *C.NetworkStrategy, _ []C.InterfaceType, _ []C.InterfaceType, _ time.Duration) (net.Conn, error) {
	return d.DialContext(ctx, network, destination)
}

func (d *controlledResolveInterfaceDialer) ListenSerialInterfacePacket(context.Context, M.Socksaddr, *C.NetworkStrategy, []C.InterfaceType, []C.InterfaceType, time.Duration) (net.PacketConn, error) {
	return nil, errors.New("unexpected ListenSerialInterfacePacket")
}

func TestResolveConcurrentAddressFamilies(t *testing.T) {
	for _, entry := range []string{"context", "interface"} {
		for _, preference := range []string{"ipv4", "ipv6", "mapped_ipv4", "default_ipv6", "lookup_ipv6"} {
			for _, outcome := range []string{"preferred_success", "fallback_success", "all_failed", "cancelled", "no_preferred", "no_fallback", "no_preference"} {
				t.Run(entry+"/"+preference+"/"+outcome, func(t *testing.T) {
					t.Parallel()
					primary := []netip.Addr{netip.MustParseAddr("192.0.2.1"), netip.MustParseAddr("192.0.2.2")}
					fallback := []netip.Addr{netip.MustParseAddr("2001:db8::1"), netip.MustParseAddr("2001:db8::2")}
					options := adapter.DNSQueryOptions{Strategy: C.DomainStrategyPreferIPv4}
					var defaultStrategy C.DomainStrategy
					if preference == "mapped_ipv4" {
						primary[0] = netip.MustParseAddr("::ffff:192.0.2.1")
					} else if preference != "ipv4" {
						primary, fallback = fallback, primary
						options.Strategy = C.DomainStrategyPreferIPv6
						if preference == "default_ipv6" {
							options.Strategy = C.DomainStrategyAsIS
							defaultStrategy = C.DomainStrategyPreferIPv6
						} else if preference == "lookup_ipv6" {
							options.Strategy = C.DomainStrategyPreferIPv4
							options.LookupStrategy = C.DomainStrategyPreferIPv6
						}
					}
					addresses := []netip.Addr{fallback[0], primary[0], fallback[1], primary[1]}
					expectedFirst := primary
					switch outcome {
					case "no_preferred":
						addresses, expectedFirst = fallback, fallback
					case "no_fallback":
						addresses = primary
					case "no_preference":
						options = adapter.DNSQueryOptions{}
						defaultStrategy = C.DomainStrategyAsIS
						expectedFirst = addresses
					}
					testDialer := &controlledResolveDialer{
						started: make(chan netip.Addr, len(addresses)),
						results: make(map[netip.Addr]chan error),
					}
					for _, address := range addresses {
						testDialer.results[address] = make(chan error, 1)
					}
					ctx, cancel := context.WithTimeout(newResolveTestContext(addresses, true), 5*time.Second)
					defer cancel()
					service.FromContext[adapter.DNSRouter](ctx).(*resolveTestRouter).strategy = defaultStrategy
					var upstream N.Dialer = testDialer
					if entry == "interface" {
						upstream = &controlledResolveInterfaceDialer{testDialer}
					}
					resolver := NewResolveDialer(ctx, upstream, true, "", options, time.Millisecond)
					type dialResult struct {
						conn net.Conn
						err  error
					}
					done := make(chan dialResult, 1)
					go func() {
						var result dialResult
						destination := M.ParseSocksaddrHostPort("example.com", 443)
						if entry == "interface" {
							result.conn, result.err = resolver.(ParallelInterfaceDialer).DialParallelInterface(ctx, N.NetworkTCP, destination, nil, nil, nil, time.Millisecond)
						} else {
							result.conn, result.err = resolver.DialContext(ctx, N.NetworkTCP, destination)
						}
						done <- result
					}()
					waitStarted := func(expected []netip.Addr) {
						t.Helper()
						var started []netip.Addr
						for range expected {
							select {
							case address := <-testDialer.started:
								started = append(started, address)
							case <-ctx.Done():
								t.Fatal("timed out waiting for concurrent dials")
							}
						}
						require.ElementsMatch(t, expected, started)
					}
					waitStarted(expectedFirst)
					firstErr := errors.New("first preferred failed")
					secondErr := errors.New("second preferred failed")
					fallbackErr := errors.New("fallback failed")
					winner := expectedFirst[1]
					switch outcome {
					case "preferred_success", "fallback_success", "all_failed", "cancelled":
						testDialer.results[primary[0]] <- firstErr
						select {
						case address := <-testDialer.started:
							t.Fatalf("fallback started before all preferred dials failed: %s", address)
						case <-time.After(20 * time.Millisecond):
						}
						if outcome == "cancelled" {
							cancel()
						} else if outcome == "preferred_success" {
							testDialer.results[winner] <- nil
						} else {
							testDialer.results[primary[1]] <- secondErr
							waitStarted(fallback)
							winner = fallback[1]
							testDialer.results[fallback[0]] <- fallbackErr
							if outcome == "all_failed" {
								testDialer.results[winner] <- fallbackErr
							} else {
								testDialer.results[winner] <- nil
							}
						}
					default:
						testDialer.results[winner] <- nil
					}
					select {
					case result := <-done:
						if outcome == "all_failed" {
							require.Nil(t, result.conn)
							require.ErrorIs(t, result.err, firstErr)
							require.ErrorIs(t, result.err, secondErr)
							require.ErrorIs(t, result.err, fallbackErr)
						} else if outcome == "cancelled" {
							require.Nil(t, result.conn)
							require.ErrorIs(t, result.err, context.Canceled)
						} else {
							require.NoError(t, result.err)
							require.Equal(t, M.SocksaddrFrom(winner, 443).String(), result.conn.RemoteAddr().String())
							require.NoError(t, result.conn.Close())
						}
					case <-time.After(5 * time.Second):
						t.Fatal("dial did not return")
					}
					select {
					case address := <-testDialer.started:
						t.Fatalf("unexpected dial: %s", address)
					default:
					}
				})
			}
		}
	}
}
