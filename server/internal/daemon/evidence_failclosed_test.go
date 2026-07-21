package daemon

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestRunTaskDoesNotLaunchProviderWhenEvidenceWriteFails(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns a provider fixture process")
	}
	marker := filepath.Join(t.TempDir(), "provider-launched")
	previousSchedule := executionEvidenceRetrySchedule
	executionEvidenceRetrySchedule = []time.Duration{time.Nanosecond, time.Nanosecond}
	t.Cleanup(func() { executionEvidenceRetrySchedule = previousSchedule })
	fakeBin := filepath.Join(t.TempDir(), "claude")
	script := `#!/bin/sh
touch "$MARKER_FILE"
IFS= read -r _
printf '%s\n' '{"type":"result","subtype":"success","is_error":false,"result":"done"}'
`
	if err := os.WriteFile(fakeBin, []byte(script), 0o755); err != nil {
		t.Fatalf("write provider fixture: %v", err)
	}

	var evidenceCalled atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/evidence") {
			evidenceCalled.Store(true)
			http.Error(w, "evidence store unavailable", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	d := &Daemon{
		client:         NewClient(srv.URL),
		logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
		workspaces:     make(map[string]*workspaceState),
		runtimeIndex:   map[string]Runtime{"runtime-1": {ID: "runtime-1", Provider: "claude"}},
		activeEnvRoots: make(map[string]int),
		cfg: Config{
			WorkspacesRoot: t.TempDir(),
			ServerBaseURL:  srv.URL,
			AgentTimeout:   5 * time.Second,
			Agents: map[string]AgentEntry{
				"claude": {Path: fakeBin, Model: "claude-sonnet-5"},
			},
		},
	}
	task := Task{
		ID:          "task-evidence-fail-closed",
		AgentID:     "agent-1",
		RuntimeID:   "runtime-1",
		IssueID:     "issue-1",
		WorkspaceID: "workspace-1",
		AuthToken:   "mat_evidence_fail_closed",
		Agent: &AgentData{
			ID:   "agent-1",
			Name: "evidence-test-agent",
			CustomEnv: map[string]string{
				"MARKER_FILE": marker,
			},
		},
	}

	_, err := d.runTask(context.Background(), task, "claude", 0, d.logger)
	if err == nil || !strings.Contains(err.Error(), "record task execution evidence") {
		t.Fatalf("runTask error = %v, want evidence write failure", err)
	}
	if !evidenceCalled.Load() {
		t.Fatal("daemon never attempted the evidence write")
	}
	if _, statErr := os.Stat(marker); !os.IsNotExist(statErr) {
		t.Fatalf("provider launched despite evidence failure; marker stat error = %v", statErr)
	}
}
