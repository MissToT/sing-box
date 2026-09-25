package route

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	R "github.com/sagernet/sing-box/route/rule"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/service"

	"github.com/stretchr/testify/require"
)

type testStatisticsRule struct {
	adapter.Rule
	name     string
	matched  bool
	disabled bool
	action   adapter.RuleAction

	hitCount  atomic.Uint64
	hitAt     atomic.Int64
	missCount atomic.Uint64
	missAt    atomic.Int64
}

func (r *testStatisticsRule) Match(*adapter.InboundContext) bool {
	return r.matched
}

func (r *testStatisticsRule) String() string {
	return r.name
}

func (r *testStatisticsRule) Disabled() bool {
	return r.disabled
}

func (r *testStatisticsRule) Action() adapter.RuleAction {
	return r.action
}

func (r *testStatisticsRule) HitCount() uint64 {
	return r.hitCount.Load()
}

func (r *testStatisticsRule) HitAt() time.Time {
	return time.Unix(0, r.hitAt.Load())
}

func (r *testStatisticsRule) MissCount() uint64 {
	return r.missCount.Load()
}

func (r *testStatisticsRule) MissAt() time.Time {
	return time.Unix(0, r.missAt.Load())
}

func (r *testStatisticsRule) RecordMatch(matched bool) {
	now := time.Now().UnixNano()
	if matched {
		r.hitCount.Add(1)
		r.hitAt.Store(now)
	} else {
		r.missCount.Add(1)
		r.missAt.Store(now)
	}
}

func (r *testStatisticsRule) ResetStatistics() {
	r.hitCount.Store(0)
	r.hitAt.Store(0)
	r.missCount.Store(0)
	r.missAt.Store(0)
}

func TestMatchRuleRecordsStatistics(t *testing.T) {
	missRule := &testStatisticsRule{name: "miss", action: &R.RuleActionRoute{Outbound: "direct"}}
	disabledRule := &testStatisticsRule{name: "disabled", matched: true, disabled: true, action: &R.RuleActionRoute{Outbound: "direct"}}
	hitRule := &testStatisticsRule{name: "hit", matched: true, action: &R.RuleActionRoute{Outbound: "direct"}}
	router := &Router{
		logger:   log.NewNOPFactory().NewLogger("router"),
		dns:      new(testL3DNSRouter),
		outbound: &testL3OutboundManager{},
		rules:    []adapter.Rule{missRule, disabledRule, hitRule},
	}
	metadata := &adapter.InboundContext{
		Network:     N.NetworkTCP,
		Source:      M.ParseSocksaddr("192.0.2.1:1234"),
		Destination: M.ParseSocksaddr("example.com:443"),
	}

	selectedRule, selectedIndex, _, _, err := router.matchRule(context.Background(), metadata, nil, nil)
	require.NoError(t, err)
	require.Equal(t, 2, selectedIndex)
	selected, isStatisticsRule := selectedRule.(*testStatisticsRule)
	require.True(t, isStatisticsRule)
	require.Same(t, hitRule, selected)

	require.Equal(t, uint64(0), missRule.HitCount())
	require.Equal(t, uint64(1), missRule.MissCount())
	require.Zero(t, missRule.HitAt().Unix())
	require.NotZero(t, missRule.MissAt().Unix())

	require.Equal(t, uint64(1), hitRule.HitCount())
	require.Equal(t, uint64(0), hitRule.MissCount())
	require.NotZero(t, hitRule.HitAt().Unix())
	require.Zero(t, hitRule.MissAt().Unix())

	require.Equal(t, uint64(0), disabledRule.HitCount())
	require.Equal(t, uint64(0), disabledRule.MissCount())
}

func newPreMatchTestRouter(rules ...adapter.Rule) *Router {
	return &Router{
		ctx:          context.Background(),
		logger:       log.NewNOPFactory().NewLogger("router"),
		dns:          new(testL3DNSRouter),
		dnsTransport: new(testL3DNSTransportManager),
		outbound:     &testL3OutboundManager{},
		rules:        rules,
	}
}

func newPreMatchTestRouterWithOutbound(outbound adapter.Outbound, rules ...adapter.Rule) *Router {
	router := newPreMatchTestRouter(rules...)
	router.outbound = &testL3OutboundManager{
		defaultOutbound: outbound,
		outbounds:       map[string]adapter.Outbound{"direct": outbound},
	}
	return router
}

func newPreMatchTestMetadata() adapter.InboundContext {
	return adapter.InboundContext{
		Network:     N.NetworkTCP,
		Source:      M.ParseSocksaddr("192.0.2.1:1234"),
		Destination: M.ParseSocksaddr("192.0.2.2:443"),
	}
}

type testNetworkManager struct {
	adapter.NetworkManager
	autoRedirectMark uint32
}

func (m *testNetworkManager) AutoRedirectOutputMark() uint32 {
	return m.autoRedirectMark
}

func TestPreMatchBypassWithoutAutoRedirectRecordsNothing(t *testing.T) {
	// bypass is only honoured by sing-tun on the auto-redirect path. Without an
	// auto-redirect session the packet falls back to the normal connection path, where
	// matchRule skips a bypass rule without an outbound, so nothing may be recorded here.
	missRule := &testStatisticsRule{name: "miss", action: &R.RuleActionRoute{Outbound: "direct"}}
	bypassRule := &testStatisticsRule{name: "bypass", matched: true, action: &R.RuleActionBypass{}}
	router := newPreMatchTestRouter(missRule, bypassRule)

	result := router.PreMatch(newPreMatchTestMetadata(), nil)
	require.Equal(t, adapter.PreMatchBypass, result.Action)
	require.Nil(t, result.NewTracker)

	require.Equal(t, uint64(0), bypassRule.HitCount())
	require.Equal(t, uint64(0), bypassRule.MissCount())
	require.Equal(t, uint64(0), missRule.HitCount())
	require.Equal(t, uint64(0), missRule.MissCount())
}

func TestPreMatchBypassWithAutoRedirectRecordsImmediately(t *testing.T) {
	// With an auto-redirect session active sing-tun honours the verdict on its nfqueue
	// path, which never calls NewTracker, so the pre-match pass has to record it.
	missRule := &testStatisticsRule{name: "miss", action: &R.RuleActionRoute{Outbound: "direct"}}
	bypassRule := &testStatisticsRule{name: "bypass", matched: true, action: &R.RuleActionBypass{}}
	router := newPreMatchTestRouter(missRule, bypassRule)
	router.ctx = service.ContextWith[adapter.NetworkManager](context.Background(), &testNetworkManager{autoRedirectMark: 1})

	result := router.PreMatch(newPreMatchTestMetadata(), nil)
	require.Equal(t, adapter.PreMatchBypass, result.Action)

	require.Equal(t, uint64(1), bypassRule.HitCount())
	require.Equal(t, uint64(0), bypassRule.MissCount())
	require.Equal(t, uint64(0), missRule.HitCount())
	require.Equal(t, uint64(1), missRule.MissCount())
}

func TestMatchRuleSkipsBypassWithoutOutbound(t *testing.T) {
	// matchRule skips a bypass rule without an outbound (continue match): it does not
	// decide the route, so it must not be counted, while the rule that does decide is.
	bypassRule := &testStatisticsRule{name: "bypass", matched: true, action: &R.RuleActionBypass{}}
	routeRule := &testStatisticsRule{name: "route", matched: true, action: &R.RuleActionRoute{Outbound: "direct"}}
	router := &Router{
		logger:   log.NewNOPFactory().NewLogger("router"),
		dns:      new(testL3DNSRouter),
		outbound: &testL3OutboundManager{},
		rules:    []adapter.Rule{bypassRule, routeRule},
	}
	metadata := &adapter.InboundContext{
		Network:     N.NetworkTCP,
		Source:      M.ParseSocksaddr("192.0.2.1:1234"),
		Destination: M.ParseSocksaddr("example.com:443"),
	}

	selectedRule, selectedIndex, _, _, err := router.matchRule(context.Background(), metadata, nil, nil)
	require.NoError(t, err)
	require.Equal(t, 1, selectedIndex)
	require.Same(t, routeRule, selectedRule)

	require.Equal(t, uint64(0), bypassRule.HitCount(), "skipped rule must not count as a hit")
	require.Equal(t, uint64(1), bypassRule.MissCount(), "skipped rule must still count as a miss")
	require.Equal(t, uint64(1), routeRule.HitCount())
	require.Equal(t, uint64(0), routeRule.MissCount())
}

func TestPreMatchRecordsStatisticsForDrop(t *testing.T) {
	dropRule := &testStatisticsRule{name: "drop", matched: true, action: &R.RuleActionReject{Method: C.RuleActionRejectMethodDrop}}
	router := newPreMatchTestRouter(dropRule)

	result := router.PreMatch(newPreMatchTestMetadata(), nil)
	require.Equal(t, adapter.PreMatchDrop, result.Action)

	require.Equal(t, uint64(1), dropRule.HitCount())
	require.Equal(t, uint64(0), dropRule.MissCount())
}

func TestPreMatchContinueRecordsNothing(t *testing.T) {
	// The flow continues to the normal stack, where RouteConnection performs its
	// own rule walk, so the pre-match pass must not record anything.
	routeRule := &testStatisticsRule{name: "route", matched: true, action: &R.RuleActionRoute{Outbound: "direct"}}
	router := newPreMatchTestRouter(routeRule)

	result := router.PreMatch(newPreMatchTestMetadata(), nil)
	require.Equal(t, adapter.PreMatchContinue, result.Action)

	require.Equal(t, uint64(0), routeRule.HitCount())
	require.Equal(t, uint64(0), routeRule.MissCount())
}

func TestPreMatchDisabledRuleRecordsNothing(t *testing.T) {
	// PreMatch currently evaluates disabled rules (pre-existing behaviour, left
	// untouched here), but statistics only track enabled rules, matching the
	// rule walk performed by matchRule.
	disabledRule := &testStatisticsRule{name: "disabled", matched: true, disabled: true, action: &R.RuleActionBypass{}}
	router := newPreMatchTestRouter(disabledRule)

	result := router.PreMatch(newPreMatchTestMetadata(), nil)

	// No enabled rule was evaluated, so there is nothing to defer either.
	require.Nil(t, result.NewTracker)
	require.Equal(t, uint64(0), disabledRule.HitCount())
	require.Equal(t, uint64(0), disabledRule.MissCount())
}

func TestPreMatchFlowRecordsStatisticsOnOffload(t *testing.T) {
	// A flow verdict is recorded from NewTracker: sing-tun calls it only right
	// before returning from a successful createFlow.
	flowOutbound := &testFlowOutbound{outboundType: "direct", tag: "direct"}
	routeRule := &testStatisticsRule{name: "route", matched: true, action: &R.RuleActionRoute{Outbound: "direct"}}
	router := newPreMatchTestRouterWithOutbound(flowOutbound, routeRule)

	result := router.PreMatch(newPreMatchTestMetadata(), nil)
	require.Equal(t, adapter.PreMatchFlow, result.Action)
	require.NotNil(t, result.NewTracker)

	require.Equal(t, uint64(0), routeRule.HitCount(), "offload is not confirmed yet")

	require.NotNil(t, result.NewTracker())

	require.Equal(t, uint64(1), routeRule.HitCount())
	require.Equal(t, uint64(0), routeRule.MissCount())
}

func TestPreMatchFlowFallbackRecordsStatisticsOnce(t *testing.T) {
	// A flow verdict is not necessarily offloaded: when sing-tun cannot set the flow
	// up it falls back to accepting the packet (the ActionFlow case in its
	// flow_dispatch.go), and the connection reaches matchRule. NewTracker is never
	// called then, so the statistics must be counted by matchRule alone.
	flowOutbound := &testFlowOutbound{outboundType: "direct", tag: "direct"}
	routeRule := &testStatisticsRule{name: "route", matched: true, action: &R.RuleActionRoute{Outbound: "direct"}}
	router := newPreMatchTestRouterWithOutbound(flowOutbound, routeRule)

	result := router.PreMatch(newPreMatchTestMetadata(), nil)
	require.Equal(t, adapter.PreMatchFlow, result.Action)
	require.Equal(t, uint64(0), routeRule.HitCount())

	// The offload failed, so the packet is handled by the normal connection path.
	metadata := newPreMatchTestMetadata()
	_, selectedIndex, _, _, err := router.matchRule(context.Background(), &metadata, nil, nil)
	require.NoError(t, err)
	require.Equal(t, 0, selectedIndex)
	require.Equal(t, uint64(1), routeRule.HitCount(), "must be counted exactly once")
	require.Equal(t, uint64(0), routeRule.MissCount())
}
