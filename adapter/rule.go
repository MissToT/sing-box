package adapter

import (
	"time"

	C "github.com/sagernet/sing-box/constant"

	"github.com/miekg/dns"
)

type HeadlessRule interface {
	Match(metadata *InboundContext) bool
	RuleCount() uint64
	String() string
}

type Rule interface {
	HeadlessRule
	SimpleLifecycle
	Disabled() bool
	UUID() string
	ChangeStatus()
	Type() string
	Action() RuleAction
}

type DNSRule interface {
	Rule
	LegacyPreMatch(metadata *InboundContext) bool
	WithAddressLimit() bool
	MatchAddressLimit(metadata *InboundContext, response *dns.Msg) bool
	MatchResponseTag() string
	MatchResponseTags() []string
	MatchResponseAnonymous() bool
	Race() bool
}

type RuleAction interface {
	Type() string
	String() string
}

// RuleStatistics reports per-rule match statistics, following the semantics of
// mihomo's Clash API `/rules` endpoint.
//
// A rule is counted as a hit when it matched and decided the route, and as a miss
// otherwise: both when it was evaluated without matching, and when it matched but the
// router skipped it — a bypass action without an outbound — so that it never took effect.
// A skipped rule is still counted, so its counters do not stay at zero.
//
// Every routing decision is counted exactly once, including the TUN/L3 pre-match pass
// when it produces the final verdict. Evaluations that do not decide the route, such as
// the DNS response address limit check, are not counted.
type RuleStatistics interface {
	// HitCount returns how many times the rule matched.
	HitCount() uint64
	// HitAt returns when the rule matched last, or the zero time if never.
	HitAt() time.Time
	// MissCount returns how many times the rule was evaluated without matching.
	MissCount() uint64
	// MissAt returns when the rule was evaluated last without matching,
	// or the zero time if never.
	MissAt() time.Time
	// RecordMatch records a single rule evaluation performed by the router.
	RecordMatch(matched bool)
	// ResetStatistics clears all counters.
	ResetStatistics()
}

// RecordRuleMatch records a rule evaluation if the rule supports statistics.
func RecordRuleMatch(rule Rule, matched bool) {
	statistics, isStatistics := rule.(RuleStatistics)
	if !isStatistics {
		return
	}
	statistics.RecordMatch(matched)
}

func IsFinalAction(action RuleAction) bool {
	switch action.Type() {
	case C.RuleActionTypeSniff, C.RuleActionTypeSniffOverrideDestination, C.RuleActionTypeResolve, C.RuleActionTypeEvaluate:
		return false
	default:
		return true
	}
}
