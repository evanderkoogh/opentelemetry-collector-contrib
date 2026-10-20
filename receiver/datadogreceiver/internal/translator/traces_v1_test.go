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

			traces, err := ToTracesV1(payload, &http.Request{Header: http.Header{}})
			require.NoError(t, err)

			attrs := traces.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).Attributes().AsRaw()
			assert.Equal(t, tc.want, attrs["sampling.priority"])
		})
	}
}
