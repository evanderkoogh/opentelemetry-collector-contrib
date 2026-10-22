// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package translator // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/datadogreceiver/internal/translator"

import (
	"strconv"
	"strings"

	pb "github.com/DataDog/datadog-agent/pkg/proto/pbgo/trace"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/sampling"
)

const (
	// The sampling rate of the Datadog Agent's priority sampler. It is exact, and only on the deciding local root.
	keyAgentPSR = "_dd.agent_psr"
	// The Knuth sampling rate, with 6 decimals, on the first span of every chunk.
	keyKnuthRate = "_dd.p.ksr"
	// The rate of the Datadog Agent's own probabilistic sampler, as a fraction. It is exact, and only on the root.
	keyProbabilisticRate = "_dd.prob_sr"
	// The rate of the tracer's sampling rule, and the rate of the limiter that applies to it. Some tracers set the
	// limit tag to the configured limit per second instead of a rate. That is above 1, so it is ignored.
	keyRulePSR  = "_dd.rule_psr"
	keyLimitPSR = "_dd.limit_psr"
	// The decision maker: the mechanism that made the sampling decision, written as a negative number like "-3".
	keyDecisionMaker = "_dd.p.dm"

	priorityAutoKeep = 1
	priorityUserKeep = 2
)

// decidedByRate returns whether a rate decided that Datadog kept a chunk, so that the rate is the probability that it
// was kept with. Priority 1 is the keep by rate. Priority 2 is a keep that a user forced, which is not decided by a
// rate, except for the sampling rules of a tracer, which also keep with priority 2. The decision maker tells these
// apart. A drop (0 or less, for example a chunk that was rescued because it has an error) was not decided by a rate.
func decidedByRate(priority float64, mechanism func() (uint32, bool)) bool {
	switch priority {
	case priorityAutoKeep:
		return true
	case priorityUserKeep:
		m, ok := mechanism()
		return ok && rateMechanisms[m]
	}
	return false
}

// rateMechanisms are the decision makers that keep a trace with a rate: the rate of the Agent, the rules and rates of
// the tracer and its remote configuration, and the probabilistic sampler of the Agent. The others, like a manual keep
// or AppSec, force a keep.
var rateMechanisms = map[uint32]bool{
	1:  true, // The rate that the Agent sent to the tracer.
	2:  true, // A remote automatic rate.
	3:  true, // A local sampling rule or rate.
	6:  true, // A remote user rate.
	9:  true, // The probabilistic sampler of the Agent.
	11: true, // A remote user rule.
	12: true, // A remote adaptive rule.
}

// parseMechanism parses a decision maker tag like "-3".
func parseMechanism(s string) (uint32, bool) {
	m, err := strconv.ParseUint(strings.TrimPrefix(s, "-"), 10, 32)
	return uint32(m), err == nil
}

// chunkMechanism returns the decision maker of a chunk in the legacy formats. It is a tag of the first span of the
// chunk or, in the v0.7 format, of the chunk.
func chunkMechanism(chunk *pb.TraceChunk) (uint32, bool) {
	if m, ok := parseMechanism(chunk.GetTags()[keyDecisionMaker]); ok {
		return m, true
	}
	for _, span := range chunk.GetSpans() {
		if m, ok := parseMechanism(span.Meta[keyDecisionMaker]); ok {
			return m, true
		}
	}
	return 0, false
}

// rateSource is how well a rate approximates the probability that Datadog sampled the trace with. A higher source is
// a better estimate.
type rateSource int

const (
	sourceRule rateSource = iota + 1
	sourceKnuth
	sourceAgent
)

// rateLookup returns the numeric value of a Datadog sampling tag.
type rateLookup func(key string) (float64, bool)

// samplingRate is the probability that Datadog kept a trace with and where it came from.
type samplingRate struct {
	probability float64
	source      rateSource
}

// probability returns the value of a tag when it is a valid sampling probability, in (0, 1].
func (lookup rateLookup) probability(key string) (float64, bool) {
	p, ok := lookup(key)
	return p, ok && p > 0 && p <= 1
}

// rate reads the sampling probability from tags. It prefers the exact `_dd.agent_psr` or `_dd.prob_sr`, then
// `_dd.p.ksr`, then `_dd.rule_psr` times `_dd.limit_psr`.
func (lookup rateLookup) rate() (samplingRate, bool) {
	if p, ok := lookup.probability(keyAgentPSR); ok {
		return samplingRate{p, sourceAgent}, true
	}
	if p, ok := lookup.probability(keyProbabilisticRate); ok {
		return samplingRate{p, sourceAgent}, true
	}
	if p, ok := lookup.probability(keyKnuthRate); ok {
		return samplingRate{p, sourceKnuth}, true
	}
	if rule, ok := lookup.probability(keyRulePSR); ok {
		limit, ok := lookup.probability(keyLimitPSR)
		if !ok {
			limit = 1
		}
		return samplingRate{rule * limit, sourceRule}, true
	}
	return samplingRate{}, false
}

// chunkSamplingRate returns the best sampling rate in a chunk. The tags are spread over the spans of a chunk, so a
// chunk has one rate, which is the most exact one of any of them. The legacy formats send the tags as span metrics,
// span meta or, in the v0.7 format, as tags of the chunk.
func chunkSamplingRate(chunk *pb.TraceChunk) (float64, bool) {
	var best samplingRate
	consider := func(lookup rateLookup) {
		if r, ok := lookup.rate(); ok && r.source > best.source {
			best = r
		}
	}
	consider(func(key string) (float64, bool) { return parseRate(chunk.GetTags()[key]) })
	for _, span := range chunk.GetSpans() {
		consider(func(key string) (float64, bool) {
			if v, ok := span.Metrics[key]; ok {
				return v, true
			}
			return parseRate(span.Meta[key])
		})
	}
	return best.probability, best.source != 0
}

func parseRate(s string) (float64, bool) {
	v, err := strconv.ParseFloat(s, 64)
	return v, err == nil
}

// setSamplingThreshold records the rate that Datadog sampled a span with as the threshold of the W3C tracestate of
// the span, so that components like spanmetrics can extrapolate counts from it. The other members of the tracestate
// are kept.
//
// The threshold of a tracestate is the combined probability of all earlier sampling stages, and a later stage can only
// raise it. Datadog's sampling is a later stage, so a threshold that is already there is multiplied with the rate, like
// a proportional downstream sampler does. A threshold that is inconsistent, or that cannot be represented, is erased,
// because the probability is then unknown. See https://opentelemetry.io/docs/specs/otel/trace/tracestate-probability-sampling/.
func setSamplingThreshold(logger *zap.Logger, span ptrace.Span, rate float64) {
	w3c, err := sampling.NewW3CTraceState(span.TraceState().AsRaw())
	if err != nil {
		logger.Debug("cannot parse the tracestate of the span", zap.Error(err))
		return
	}
	otel := w3c.OTelValue()

	probability := rate
	existing, hasExisting := otel.TValueThreshold()
	if hasExisting {
		// The span was kept, so an explicit randomness has to be at least the threshold.
		if rnd, ok := otel.RValueRandomness(); ok && !existing.ShouldSample(rnd) {
			otel.ClearTValue()
			writeTraceState(logger, span, &w3c)
			return
		}
		if rate == 1 {
			// Datadog kept every trace, so the exact earlier threshold stays.
			return
		}
		probability = rate * existing.Probability()
	}

	threshold, err := sampling.ProbabilityToThreshold(probability)
	if err != nil {
		// The probability is below the smallest one that a threshold can represent.
		logger.Debug("cannot convert the sampling rate to a threshold", zap.Float64("rate", probability), zap.Error(err))
		if hasExisting {
			otel.ClearTValue()
			writeTraceState(logger, span, &w3c)
		}
		return
	}
	if err := otel.UpdateTValueWithSampling(threshold); err != nil {
		// Rounding made the threshold lower than the earlier one, which is more precise, so that one stays.
		return
	}
	writeTraceState(logger, span, &w3c)
}

func writeTraceState(logger *zap.Logger, span ptrace.Span, w3c *sampling.W3CTraceState) {
	var sb strings.Builder
	if err := w3c.Serialize(&sb); err != nil {
		logger.Debug("cannot serialize the tracestate of the span", zap.Error(err))
		return
	}
	span.TraceState().FromRaw(sb.String())
}
