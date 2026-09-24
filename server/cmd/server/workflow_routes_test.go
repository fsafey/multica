package main

import (
	"net/http"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/multica-ai/multica/server/internal/analytics"
	"github.com/multica-ai/multica/server/internal/events"
	"github.com/multica-ai/multica/server/internal/realtime"
)

func TestWorkflowRoutesRemainMounted(t *testing.T) {
	router := NewRouter(nil, realtime.NewHub(), events.New(), analytics.NoopClient{}, nil)
	base := "/api/workspaces/{id}"
	want := map[string]bool{
		"GET " + base + "/runtime-pools":                                       false,
		"GET " + base + "/workflow-runs":                                       false,
		"GET " + base + "/workflow-runs/{runId}":                               false,
		"POST " + base + "/runtime-pools":                                      false,
		"POST " + base + "/runtime-pools/{poolId}/runtimes":                    false,
		"DELETE " + base + "/runtime-pools/{poolId}/runtimes/{runtimeId}":      false,
		"POST " + base + "/runtime-pools/{poolId}/agents":                      false,
		"POST " + base + "/workflow-runs":                                      false,
		"POST " + base + "/workflow-runs/{runId}/pause":                        false,
		"POST " + base + "/workflow-runs/{runId}/resume":                       false,
		"POST " + base + "/workflow-runs/{runId}/cancel":                       false,
		"POST " + base + "/workflow-runs/{runId}/nodes/{nodeId}/gate-complete": false,
		"POST " + base + "/workflow-runs/{runId}/nodes/{nodeId}/retry":         false,
		"POST " + base + "/workflow-runs/{runId}/nodes/{nodeId}/cancel":        false,
	}
	if err := chi.Walk(router, func(method, route string, _ http.Handler, _ ...func(http.Handler) http.Handler) error {
		key := method + " " + route
		if _, ok := want[key]; ok {
			want[key] = true
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for route, mounted := range want {
		if !mounted {
			t.Errorf("missing workflow route %s", route)
		}
	}
}
