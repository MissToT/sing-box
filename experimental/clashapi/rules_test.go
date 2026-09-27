package clashapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"

	"github.com/stretchr/testify/require"
)

type testRuleAction struct{}

func (a testRuleAction) Type() string {
	return "route"
}

func (a testRuleAction) String() string {
	return "proxy"
}

type testRule struct {
	hitCount  atomic.Uint64
	hitAt     atomic.Int64
	missCount atomic.Uint64
	missAt    atomic.Int64
}

func (r *testRule) Match(*adapter.InboundContext) bool { return true }
func (r *testRule) RuleCount() uint64                  { return 1 }
func (r *testRule) String() string                     { return "domain_suffix=example.com" }
func (r *testRule) Start() error                       { return nil }
func (r *testRule) Close() error                       { return nil }
func (r *testRule) Disabled() bool                     { return false }
func (r *testRule) UUID() string                       { return "test-rule" }
func (r *testRule) ChangeStatus()                      {}
func (r *testRule) Type() string                       { return "default" }
func (r *testRule) Action() adapter.RuleAction         { return testRuleAction{} }

func (r *testRule) HitCount() uint64 {
	return r.hitCount.Load()
}

func (r *testRule) HitAt() time.Time {
	return time.Unix(0, r.hitAt.Load())
}

func (r *testRule) MissCount() uint64 {
	return r.missCount.Load()
}

func (r *testRule) MissAt() time.Time {
	return time.Unix(0, r.missAt.Load())
}

func (r *testRule) RecordMatch(matched bool) {
	now := time.Now().UnixNano()
	if matched {
		r.hitCount.Add(1)
		r.hitAt.Store(now)
	} else {
		r.missCount.Add(1)
		r.missAt.Store(now)
	}
}

func (r *testRule) ResetStatistics() {
	r.hitCount.Store(0)
	r.hitAt.Store(0)
	r.missCount.Store(0)
	r.missAt.Store(0)
}

type testRulesRouter struct {
	adapter.Router
	rules []adapter.Rule
}

func (r *testRulesRouter) Rules() []adapter.Rule {
	return r.rules
}

func (r *testRulesRouter) Rule(uuid string) (adapter.Rule, bool) {
	return nil, false
}

type testRulesDNSRouter struct {
	adapter.DNSRouter
	rules []adapter.DNSRule
}

func (r *testRulesDNSRouter) Rules() []adapter.DNSRule {
	return r.rules
}

func (r *testRulesDNSRouter) Rule(uuid string) (adapter.DNSRule, bool) {
	return nil, false
}

func TestRulesStatistics(t *testing.T) {
	rule := &testRule{}
	rule.RecordMatch(true)
	rule.RecordMatch(true)
	rule.RecordMatch(false)

	handler := ruleRouter(&testRulesRouter{rules: []adapter.Rule{rule}}, &testRulesDNSRouter{})

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/", nil))
	require.Equal(t, http.StatusOK, recorder.Code)

	var response struct {
		Rules []Rule `json:"rules"`
	}
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
	require.Len(t, response.Rules, 1)
	require.Equal(t, 0, response.Rules[0].Index)
	require.Equal(t, "default", response.Rules[0].Type)
	require.Equal(t, "domain_suffix=example.com", response.Rules[0].Payload)
	require.Equal(t, "proxy", response.Rules[0].Proxy)
	require.Equal(t, "test-rule", response.Rules[0].UUID)
	require.NotNil(t, response.Rules[0].Extra)
	require.Equal(t, uint64(2), response.Rules[0].Extra.HitCount)
	require.Equal(t, uint64(1), response.Rules[0].Extra.MissCount)
	require.False(t, response.Rules[0].Extra.HitAt.IsZero())
	require.False(t, response.Rules[0].Extra.MissAt.IsZero())

	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/statistics/reset", nil))
	require.Equal(t, http.StatusNoContent, recorder.Code)
	require.Equal(t, uint64(0), rule.HitCount())
	require.Equal(t, uint64(0), rule.MissCount())
	require.True(t, rule.HitAt().IsZero() || rule.HitAt().Unix() == 0)
}

func TestRulesRouterUUIDRoute(t *testing.T) {
	handler := ruleRouter(&testRulesRouter{}, &testRulesDNSRouter{})
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPut, "/missing-uuid", nil))
	require.Equal(t, http.StatusNotFound, recorder.Code)
}
