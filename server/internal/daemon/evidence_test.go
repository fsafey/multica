package daemon

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/multica-ai/multica/server/pkg/executionevidence"
)

func TestBuildExecutionSnapshotCapturesExactPromptWithoutStructuredSecretValues(t *testing.T) {
	t.Parallel()
	secret := strings.Join([]string{"sk-", "proj-0123456789abcdefghijklmnop"}, "")
	instructionSecret := strings.Join([]string{"instruction-", "private-value"}, "")
	contextSecret := strings.Join([]string{"context-", "private-value"}, "")
	snapshot, err := buildExecutionSnapshot(executionSnapshotParams{
		Task: Task{
			ID:               "task-1",
			AgentID:          "agent-1",
			RuntimeID:        "runtime-1",
			WorkspaceID:      "workspace-1",
			ProjectID:        "project-1",
			WorkspaceContext: "Authorization: Bearer " + contextSecret,
			PriorSessionID:   "session-1",
		},
		Provider:              "claude",
		InvocationModel:       "claude-sonnet-5",
		InvocationModelSource: "agent",
		ProviderCLIVersion:    "2.1.0",
		MulticaCLIVersion:     "v0.4.3",
		MulticaGitCommit:      "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Instructions:          "API_KEY=" + instructionSecret,
		Skills: []SkillData{
			{Name: "zeta", Hash: "sha256:z"},
			{Name: "alpha", Content: "skill content"},
		},
		ThinkingLevel:      "high",
		CustomArguments:    []string{"--api-key", secret, "--model", "claude-sonnet-5"},
		CustomEnvironment:  map[string]string{"TOKEN": secret, "SAFE_NAME": "value"},
		MCPConfiguration:   json.RawMessage(`{"mcpServers":{"private":{"headers":{"Authorization":"secret"}}}}`),
		SessionRequested:   true,
		RequestedSessionID: "session-1",
		WorkdirRequested:   true,
		WorkdirReused:      true,
	})
	if err != nil {
		t.Fatalf("buildExecutionSnapshot: %v", err)
	}

	if snapshot.InvocationModel != "claude-sonnet-5" || snapshot.Provider != "claude" {
		t.Fatalf("provider/model = %q/%q", snapshot.Provider, snapshot.InvocationModel)
	}
	if !reflect.DeepEqual(snapshot.CustomEnvironmentNames, []string{"SAFE_NAME", "TOKEN"}) {
		t.Fatalf("environment names = %#v", snapshot.CustomEnvironmentNames)
	}
	if !reflect.DeepEqual(snapshot.MCPServerNames, []string{"private"}) || snapshot.MCPConfigurationHash == "" {
		t.Fatalf("MCP metadata = %#v / %q", snapshot.MCPServerNames, snapshot.MCPConfigurationHash)
	}
	if !snapshot.SessionResume.Requested || !snapshot.SessionResume.Selected || !snapshot.WorkdirReuse.Selected {
		t.Fatalf("resume/reuse decisions = %#v / %#v", snapshot.SessionResume, snapshot.WorkdirReuse)
	}
	if len(snapshot.MountedSkills) != 2 || snapshot.MountedSkills[0].Name != "alpha" {
		t.Fatalf("mounted skills not sorted: %#v", snapshot.MountedSkills)
	}
	if snapshot.Instructions != "API_KEY="+instructionSecret ||
		snapshot.WorkspaceContext != "Authorization: Bearer "+contextSecret {
		t.Fatalf("claim-time prompt text was not preserved exactly: %#v", snapshot)
	}

	raw, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatalf("marshal snapshot: %v", err)
	}
	for _, forbidden := range []string{secret, `"value"`} {
		if strings.Contains(string(raw), forbidden) {
			t.Fatalf("snapshot leaked %q: %s", forbidden, raw)
		}
	}
}

func TestMCPMetadataHashExcludesCredentialValues(t *testing.T) {
	t.Parallel()
	first := json.RawMessage(`{"mcpServers":{"private":{"headers":{"Authorization":"first-secret"},"url":"https://mcp.example"}}}`)
	second := json.RawMessage(`{"mcpServers":{"private":{"headers":{"Authorization":"second-secret"},"url":"https://mcp.example"}}}`)
	firstNames, firstHash, err := executionevidence.MCPMetadata(first)
	if err != nil {
		t.Fatalf("first MCP metadata: %v", err)
	}
	secondNames, secondHash, err := executionevidence.MCPMetadata(second)
	if err != nil {
		t.Fatalf("second MCP metadata: %v", err)
	}
	if !reflect.DeepEqual(firstNames, secondNames) || firstHash != secondHash {
		t.Fatalf("credential-only MCP change affected exported metadata: %#v/%q %#v/%q", firstNames, firstHash, secondNames, secondHash)
	}
}

func TestResolveInvocationModelUsesEffectiveArgumentOverride(t *testing.T) {
	t.Parallel()
	model, source := resolveInvocationModel(
		"claude",
		"claude-sonnet-5",
		"agent",
		[]string{"--model", "claude-opus-4-8"},
		[]string{"--model=claude-sonnet-5-1"},
	)
	if model != "claude-sonnet-5-1" || source != "custom_arguments" {
		t.Fatalf("got model=%q source=%q", model, source)
	}
}

func TestResolveInvocationModelReadsCodexConfigOnlyWithoutExplicitThreadModel(t *testing.T) {
	t.Parallel()
	model, source := resolveInvocationModel(
		"codex", "", "provider_default", nil, []string{"-c", `model="gpt-5.6-codex"`},
	)
	if model != "gpt-5.6-codex" || source != "custom_arguments" {
		t.Fatalf("got model=%q source=%q", model, source)
	}

	model, source = resolveInvocationModel(
		"codex", "gpt-5.5", "agent", nil, []string{"-c", `model="gpt-5.6-codex"`},
	)
	if model != "gpt-5.5" || source != "agent" {
		t.Fatalf("explicit thread model should win, got model=%q source=%q", model, source)
	}
}

func TestResolveInvocationModelIgnoresBlockedProviderOverride(t *testing.T) {
	t.Parallel()
	model, source := resolveInvocationModel(
		"antigravity", "Claude Opus 4.6 (Thinking)", "agent", nil, []string{"--model", "blocked"},
	)
	if model != "Claude Opus 4.6 (Thinking)" || source != "agent" {
		t.Fatalf("blocked provider override changed evidence: model=%q source=%q", model, source)
	}
}
