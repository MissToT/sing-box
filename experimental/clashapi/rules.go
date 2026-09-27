package clashapi

import (
	"context"
	"net/http"
	"time"

	"github.com/sagernet/sing-box/adapter"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/render"
)

func ruleRouter(router adapter.Router, dnsRouter adapter.DNSRouter) http.Handler {
	r := chi.NewRouter()
	r.Get("/", getRules(router, dnsRouter))
	r.Post("/statistics/reset", resetRulesStatistics(router, dnsRouter))
	r.Route("/{uuid}", func(r chi.Router) {
		r.Use(parseRuleUUID, findRuleByUUID(router, dnsRouter))
		r.Put("/", changeRuleStatus)
	})
	return r
}

type Rule struct {
	Index   int    `json:"index"`
	Type    string `json:"type"`
	Payload string `json:"payload"`
	Proxy   string `json:"proxy"`

	Disabled bool   `json:"disabled,omitempty"`
	UUID     string `json:"uuid,omitempty"`

	// Extra contains the rule match statistics, see [RuleExtra].
	Extra *RuleExtra `json:"extra,omitempty"`
}

// RuleExtra mirrors the `extra` object of mihomo's Clash API `/rules` endpoint,
// so that Clash dashboards can display rule hit statistics without changes.
type RuleExtra struct {
	Disabled  bool      `json:"disabled"`
	HitCount  uint64    `json:"hitCount"`
	HitAt     time.Time `json:"hitAt"`
	MissCount uint64    `json:"missCount"`
	MissAt    time.Time `json:"missAt"`
}

func getRules(router adapter.Router, dnsRouter adapter.DNSRouter) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		var rules []Rule
		appendRule := func(rule adapter.Rule) {
			rules = append(rules, Rule{
				Index:   len(rules),
				Type:    rule.Type(),
				Payload: rule.String(),
				Proxy:   rule.Action().String(),

				Disabled: rule.Disabled(),
				UUID:     rule.UUID(),

				Extra: buildRuleExtra(rule),
			})
		}
		for _, rule := range dnsRouter.Rules() {
			appendRule(rule)
		}
		for _, rule := range router.Rules() {
			appendRule(rule)
		}
		render.JSON(w, r, render.M{
			"rules": rules,
		})
	}
}

func buildRuleExtra(rule adapter.Rule) *RuleExtra {
	statistics, isStatistics := rule.(adapter.RuleStatistics)
	if !isStatistics {
		return nil
	}
	return &RuleExtra{
		Disabled:  rule.Disabled(),
		HitCount:  statistics.HitCount(),
		HitAt:     statistics.HitAt(),
		MissCount: statistics.MissCount(),
		MissAt:    statistics.MissAt(),
	}
}

func resetRulesStatistics(router adapter.Router, dnsRouter adapter.DNSRouter) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		resetRule := func(rule adapter.Rule) {
			statistics, isStatistics := rule.(adapter.RuleStatistics)
			if isStatistics {
				statistics.ResetStatistics()
			}
		}
		for _, rule := range dnsRouter.Rules() {
			resetRule(rule)
		}
		for _, rule := range router.Rules() {
			resetRule(rule)
		}
		render.NoContent(w, r)
	}
}

func parseRuleUUID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		uuid := getEscapeParam(r, "uuid")
		ctx := context.WithValue(r.Context(), CtxKeyRuleUUID, uuid)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func findRuleByUUID(router adapter.Router, dnsRouter adapter.DNSRouter) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			uuid := r.Context().Value(CtxKeyRuleUUID).(string)
			routeRule, exist := router.Rule(uuid)
			if exist {
				ctx := context.WithValue(r.Context(), CtxKeyRule, routeRule)
				next.ServeHTTP(w, r.WithContext(ctx))
				return
			}
			dnsRule, dnsExist := dnsRouter.Rule(uuid)
			if dnsExist {
				ctx := context.WithValue(r.Context(), CtxKeyRule, adapter.Rule(dnsRule))
				next.ServeHTTP(w, r.WithContext(ctx))
				return
			}
			render.Status(r, http.StatusNotFound)
			render.JSON(w, r, ErrNotFound)
		})
	}
}

func changeRuleStatus(w http.ResponseWriter, r *http.Request) {
	rule := r.Context().Value(CtxKeyRule).(adapter.Rule)
	rule.ChangeStatus()
	render.NoContent(w, r)
}
