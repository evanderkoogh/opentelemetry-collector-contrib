// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package translator // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/datadogreceiver/internal/translator"

import (
	"net/http"
	"testing"

	"github.com/DataDog/datadog-agent/pkg/proto/pbgo/trace/idx"
	ddsampler "github.com/DataDog/datadog-agent/pkg/trace/sampler"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

func TestToTracesV1SamplingPriority(t *testing.T) {
	for name, tc := range map[string]struct {
		priority int32
		want     any
	}{
		"user keep":   {priority: int32(ddsampler.PriorityUserKeep), want: "2.000000"},
		"auto reject": {priority: int32(ddsampler.PriorityAutoDrop), want: "0.000000"},
		"user reject": {priority: int32(ddsampler.PriorityUserDrop), want: "-1.000000"},
		"unset":       {priority: int32(ddsampler.PriorityNone), want: nil},
	} {
		t.Run(name, func(t *testing.T) {
			strs := idx.NewStringTable()
			span := idx.NewInternalSpan(strs, &idx.Span{ServiceRef: strs.Add("svc"), SpanID: 1})
			payload := &idx.InternalTracerPayload{
				Strings: strs,
				Chunks:  []*idx.InternalTraceChunk{idx.NewInternalTraceChunk(strs, tc.priority, "", nil, []*idx.InternalSpan{span}, false, make([]byte, 16), 0)},
			}

			traces, err := ToTracesV1(zap.NewNop(), payload, &http.Request{Header: http.Header{}})
			require.NoError(t, err)

			attrs := traces.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).Attributes().AsRaw()
			assert.Equal(t, tc.want, attrs["sampling.priority"])
		})
	}
}

// A span that the agent converted from a v0.x format has the converter's marker. It has its links and events as JSON
// tags, and the internal kind when the tracer did not set one.
func TestToTracesV1ConvertedSpans(t *testing.T) {
	links := `[{"trace_id":"00000000000000000000000000000abc","span_id":"0000000000000def","flags":1}]`
	events := `[{"time_unix_nano":5,"name":"boom","attributes":{"k":"v"}}]`

	for name, tc := range map[string]struct {
		converted bool
		kind      idx.SpanKind
		spanType  string
		wantKind  ptrace.SpanKind
	}{
		"converted web span without span.kind":  {converted: true, kind: idx.SpanKind_SPAN_KIND_INTERNAL, spanType: "web", wantKind: ptrace.SpanKindServer},
		"converted http span without span.kind": {converted: true, kind: idx.SpanKind_SPAN_KIND_INTERNAL, spanType: "http", wantKind: ptrace.SpanKindClient},
		"converted internal span":               {converted: true, kind: idx.SpanKind_SPAN_KIND_INTERNAL, spanType: "custom", wantKind: ptrace.SpanKindInternal},
		"converted span with span.kind":         {converted: true, kind: idx.SpanKind_SPAN_KIND_CLIENT, spanType: "web", wantKind: ptrace.SpanKindClient},
		"native internal web span":              {converted: false, kind: idx.SpanKind_SPAN_KIND_INTERNAL, spanType: "web", wantKind: ptrace.SpanKindInternal},
	} {
		t.Run(name, func(t *testing.T) {
			strs := idx.NewStringTable()
			str := func(s string) *idx.AnyValue {
				return &idx.AnyValue{Value: &idx.AnyValue_StringValueRef{StringValueRef: strs.Add(s)}}
			}
			attrs := map[uint32]*idx.AnyValue{
				strs.Add("_dd.span_links"): str(links),
				strs.Add("events"):         str(events),
			}
			if tc.converted {
				attrs[strs.Add(attributeConvertedV1)] = str("v04")
			}
			span := idx.NewInternalSpan(strs, &idx.Span{ServiceRef: strs.Add("svc"), TypeRef: strs.Add(tc.spanType), Kind: tc.kind, SpanID: 2, Attributes: attrs})
			payload := &idx.InternalTracerPayload{
				Strings: strs,
				Chunks:  []*idx.InternalTraceChunk{idx.NewInternalTraceChunk(strs, 1, "", nil, []*idx.InternalSpan{span}, false, make([]byte, 16), 0)},
			}

			traces, err := ToTracesV1(zap.NewNop(), payload, &http.Request{Header: http.Header{}})
			require.NoError(t, err)

			got := traces.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
			assert.Equal(t, tc.wantKind, got.Kind())
			if !tc.converted {
				// Native spans keep the tags as they are.
				assert.Equal(t, 0, got.Links().Len())
				assert.Equal(t, 0, got.Events().Len())
				assert.Equal(t, links, got.Attributes().AsRaw()["_dd.span_links"])
				assert.Equal(t, events, got.Attributes().AsRaw()["events"])
				return
			}

			require.Equal(t, 1, got.Links().Len())
			link := got.Links().At(0)
			assert.Equal(t, pcommon.TraceID{15: 0xbc, 14: 0x0a}, link.TraceID())
			assert.Equal(t, pcommon.SpanID{7: 0xef, 6: 0x0d}, link.SpanID())
			assert.Equal(t, uint32(1), link.Flags())
			require.Equal(t, 1, got.Events().Len())
			assert.Equal(t, "boom", got.Events().At(0).Name())
			assert.Equal(t, pcommon.Timestamp(5), got.Events().At(0).Timestamp())
			assert.Equal(t, map[string]any{"k": "v"}, got.Events().At(0).Attributes().AsRaw())
			assert.NotContains(t, got.Attributes().AsRaw(), "_dd.span_links")
			assert.NotContains(t, got.Attributes().AsRaw(), "events")
			assert.Equal(t, "v04", got.Attributes().AsRaw()[attributeConvertedV1], "the marker stays")
		})
	}
}

func TestToTracesV1SamplingThreshold(t *testing.T) {
	type attrs struct {
		numbers map[string]float64
		strings map[string]string
	}
	// A chunk of a deciding root (span 1) and a child (span 2), like TestToTracesSamplingThreshold.
	tests := []struct {
		name     string
		priority int32
		chunk    attrs
		root     attrs
		child    attrs
		want     map[uint64]string
	}{
		{
			name:     "the agent rate is preferred over the Knuth rate on another span",
			priority: int32(ddsampler.PriorityAutoKeep),
			root:     attrs{numbers: map[string]float64{"_dd.agent_psr": 0.5}},
			child:    attrs{strings: map[string]string{"_dd.p.ksr": "0.25"}},
			want:     map[uint64]string{1: "ot=th:8", 2: "ot=th:8"},
		},
		{
			name:     "the Knuth rate as a string or a number on one span applies to the whole chunk",
			priority: int32(ddsampler.PriorityAutoKeep),
			child:    attrs{strings: map[string]string{"_dd.p.ksr": "0.25"}},
			want:     map[uint64]string{1: "ot=th:c", 2: "ot=th:c"},
		},
		{
			name:     "the Knuth rate as a number",
			priority: int32(ddsampler.PriorityAutoKeep),
			child:    attrs{numbers: map[string]float64{"_dd.p.ksr": 0.25}},
			want:     map[uint64]string{1: "ot=th:c", 2: "ot=th:c"},
		},
		{
			name:     "the rate can be an attribute of the chunk",
			priority: int32(ddsampler.PriorityAutoKeep),
			chunk:    attrs{strings: map[string]string{"_dd.p.ksr": "0.25"}},
			want:     map[uint64]string{1: "ot=th:c", 2: "ot=th:c"},
		},
		{
			name:     "the Knuth rate is preferred over the rule rate",
			priority: int32(ddsampler.PriorityAutoKeep),
			root:     attrs{numbers: map[string]float64{"_dd.rule_psr": 0.5}, strings: map[string]string{"_dd.p.ksr": "0.25"}},
			want:     map[uint64]string{1: "ot=th:c", 2: "ot=th:c"},
		},
		{
			name:     "the rule rate is multiplied by the limit rate",
			priority: int32(ddsampler.PriorityAutoKeep),
			root:     attrs{numbers: map[string]float64{"_dd.rule_psr": 0.5, "_dd.limit_psr": 0.5}},
			want:     map[uint64]string{1: "ot=th:c", 2: "ot=th:c"},
		},
		{
			name:     "a user keep is not decided by a rate",
			priority: int32(ddsampler.PriorityUserKeep),
			root:     attrs{numbers: map[string]float64{"_dd.agent_psr": 0.5}},
			want:     map[uint64]string{1: "", 2: ""},
		},
		{
			name:     "a dropped chunk, kept for its error, is not decided by a rate",
			priority: int32(ddsampler.PriorityAutoDrop),
			root:     attrs{numbers: map[string]float64{"_dd.agent_psr": 0.5}},
			want:     map[uint64]string{1: "", 2: ""},
		},
		{
			name:     "no priority",
			priority: int32(ddsampler.PriorityNone),
			root:     attrs{numbers: map[string]float64{"_dd.agent_psr": 0.5}},
			want:     map[uint64]string{1: "", 2: ""},
		},
		{
			name:     "no rate",
			priority: int32(ddsampler.PriorityAutoKeep),
			want:     map[uint64]string{1: "", 2: ""},
		},
		{
			name:     "rates that are not a probability are ignored",
			priority: int32(ddsampler.PriorityAutoKeep),
			root:     attrs{numbers: map[string]float64{"_dd.agent_psr": 1.5, "_dd.rule_psr": 0}},
			child:    attrs{strings: map[string]string{"_dd.p.ksr": "not a number"}},
			want:     map[uint64]string{1: "", 2: ""},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			strs := idx.NewStringTable()
			newSpan := func(id, parent uint64, a attrs) *idx.InternalSpan {
				span := idx.NewInternalSpan(strs, &idx.Span{ServiceRef: strs.Add("svc"), SpanID: id, ParentID: parent})
				for k, v := range a.numbers {
					span.SetFloat64Attribute(k, v)
				}
				for k, v := range a.strings {
					span.SetStringAttribute(k, v)
				}
				return span
			}
			chunkAttrs := map[uint32]*idx.AnyValue{}
			for k, v := range tt.chunk.strings {
				chunkAttrs[strs.Add(k)] = &idx.AnyValue{Value: &idx.AnyValue_StringValueRef{StringValueRef: strs.Add(v)}}
			}
			spans := []*idx.InternalSpan{newSpan(2, 1, tt.child), newSpan(1, 0, tt.root)}
			payload := &idx.InternalTracerPayload{
				Strings: strs,
				Chunks:  []*idx.InternalTraceChunk{idx.NewInternalTraceChunk(strs, tt.priority, "", chunkAttrs, spans, false, make([]byte, 16), 0)},
			}

			traces, err := ToTracesV1(zap.NewNop(), payload, &http.Request{Header: http.Header{}})
			require.NoError(t, err)
			got := map[uint64]string{}
			for _, rs := range traces.ResourceSpans().All() {
				for _, ss := range rs.ScopeSpans().All() {
					for _, span := range ss.Spans().All() {
						got[spanIDToUint64(span.SpanID())] = span.TraceState().AsRaw()
					}
				}
			}
			assert.Equal(t, tt.want, got)
		})
	}
}
