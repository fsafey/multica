package executionevidence

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestSnapshotDigestIsStableAndCoversPayload(t *testing.T) {
	t.Parallel()
	snapshot := Snapshot{
		SchemaVersion:          CurrentSchemaVersion,
		TaskID:                 "task-1",
		Provider:               "claude",
		InvocationModel:        "claude-sonnet-5",
		MulticaCLIVersion:      "v1.2.3",
		MulticaGitCommit:       "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		AgentID:                "agent-1",
		RuntimeID:              "runtime-1",
		WorkspaceID:            "workspace-1",
		Instructions:           "do the work",
		CustomEnvironmentNames: []string{"A", "Z"},
	}

	first, err := snapshot.Digest()
	if err != nil {
		t.Fatalf("digest snapshot: %v", err)
	}
	second, err := snapshot.Digest()
	if err != nil {
		t.Fatalf("digest snapshot again: %v", err)
	}
	if first != second || !strings.HasPrefix(first, "sha256:") || len(first) != 71 {
		t.Fatalf("digest = %q, want stable sha256 digest", first)
	}

	snapshot.Instructions = "changed"
	changed, err := snapshot.Digest()
	if err != nil {
		t.Fatalf("digest changed snapshot: %v", err)
	}
	if changed == first {
		t.Fatal("digest did not cover instructions")
	}
}

func TestCanonicalizePayloadPreservesUnknownFieldsAndIgnoresKeyOrder(t *testing.T) {
	t.Parallel()
	first, err := CanonicalizePayload([]byte(`{"schema_version":1,"future":{"z":2,"a":1}}`))
	if err != nil {
		t.Fatalf("canonicalize first payload: %v", err)
	}
	second, err := CanonicalizePayload([]byte(`{"future":{"a":1,"z":2},"schema_version":1}`))
	if err != nil {
		t.Fatalf("canonicalize second payload: %v", err)
	}
	if string(first) != string(second) || !strings.Contains(string(first), `"future"`) {
		t.Fatalf("canonical payloads differ or lost unknown fields: %s / %s", first, second)
	}
}

func TestSnapshotCompletenessDoesNotPretendProviderDefaultIsResolved(t *testing.T) {
	t.Parallel()
	snapshot := Snapshot{
		TaskID:                "task-1",
		Provider:              "claude",
		InvocationModelSource: "provider_default",
		ProviderCLIVersion:    "2.1.0",
		MulticaCLIVersion:     "v1.2.3",
		MulticaGitCommit:      "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		AgentID:               "agent-1",
		RuntimeID:             "runtime-1",
		WorkspaceID:           "workspace-1",
	}
	if got := snapshot.CompletenessIssues(); !reflect.DeepEqual(got, []string{"invocation_model"}) {
		t.Fatalf("CompletenessIssues() = %#v", got)
	}
}

func TestSanitizeArgumentsRedactsSecretsAndPrivatePaths(t *testing.T) {
	t.Parallel()
	secret := strings.Join([]string{"sk-", "proj-0123456789abcdefghijklmnop"}, "")
	got := SanitizeArguments([]string{
		"--api-key", secret,
		"--api_key", "underscore-secret",
		"--access_key=access-secret",
		"--private_key", "private-secret",
		"--token=plain-secret",
		"--endpoint=https://mcp.example/sse?key=query-secret#private-fragment",
		"--add-dir", "/Users/alice/private-project",
		"--secret-mode",
		"--model", "claude-sonnet-5",
	})
	want := []string{
		"--api-key", RedactedValue,
		"--api_key", RedactedValue,
		"--access_key=" + RedactedValue,
		"--private_key", RedactedValue,
		"--token=" + RedactedValue,
		"--endpoint=https://mcp.example/sse",
		"--add-dir", RedactedPath,
		"--secret-mode",
		"--model", "claude-sonnet-5",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("SanitizeArguments() = %#v, want %#v", got, want)
	}
}

func TestSanitizeArgumentsMasksShortSecretFlags(t *testing.T) {
	t.Parallel()
	got := SanitizeArguments([]string{
		"-e", "PRIVATE_TOKEN=owner-secret-value",
		"-H", "Authorization: owner-secret-value",
		"--env-file", "/private/runtime.env",
	})
	want := []string{
		"-e", RedactedValue,
		"-H", RedactedValue,
		"--env-file", RedactedPath,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("sanitized short flags = %#v, want %#v", got, want)
	}
}

func TestSanitizeArgumentsPreservesCodexConfigOverrides(t *testing.T) {
	t.Parallel()
	got := SanitizeArguments([]string{
		"--config", "model=gpt-5-codex",
		"--config=model_reasoning_effort=high",
		"--config", "private/config.toml",
	})
	want := []string{
		"--config", "model=gpt-5-codex",
		"--config=model_reasoning_effort=high",
		"--config", RedactedPath,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("sanitized config flags = %#v, want %#v", got, want)
	}
}

func TestMCPMetadataReturnsNamesAndCanonicalHashOnly(t *testing.T) {
	t.Parallel()
	one := json.RawMessage(`{"mcpServers":{"zeta":{"headers":{"Authorization":"first-header-secret"},"apiKey":"first-key-secret","url":"https://alice:first-password@example.test","args":["--token","first-argument-secret","--model","safe-model"]},"alpha":{"command":"uvx"}}}`)
	two := json.RawMessage(`{"mcpServers":{"alpha":{"command":"uvx"},"zeta":{"args":["--token","second-argument-secret","--model","safe-model"],"url":"https://alice:second-password@example.test","apiKey":"second-key-secret","headers":{"Authorization":"second-header-secret"}}}}`)

	namesOne, hashOne, err := MCPMetadata(one)
	if err != nil {
		t.Fatalf("MCPMetadata(one): %v", err)
	}
	namesTwo, hashTwo, err := MCPMetadata(two)
	if err != nil {
		t.Fatalf("MCPMetadata(two): %v", err)
	}
	if !reflect.DeepEqual(namesOne, []string{"alpha", "zeta"}) || !reflect.DeepEqual(namesTwo, namesOne) {
		t.Fatalf("server names = %#v / %#v", namesOne, namesTwo)
	}
	if hashOne != hashTwo || !strings.HasPrefix(hashOne, "sha256:") {
		t.Fatalf("canonical hashes = %q / %q", hashOne, hashTwo)
	}
	if strings.Contains(hashOne, "secret") {
		t.Fatal("configuration hash exposed configuration content")
	}
}

func TestSequenceIntegrity(t *testing.T) {
	t.Parallel()
	good := CheckSequenceIntegrity([]int32{1, 2, 3})
	if !good.Valid || !good.StrictlyIncreasing || good.DuplicateCount != 0 {
		t.Fatalf("valid sequence reported invalid: %#v", good)
	}

	got := CheckSequenceIntegrity([]int32{2, 3, 5})
	if got.Valid || !got.StrictlyIncreasing || got.DuplicateCount != 0 {
		t.Fatalf("gap-bearing sequence integrity = %#v", got)
	}
	if !reflect.DeepEqual(got.Gaps, []SequenceGap{{After: 0, Before: 2}, {After: 3, Before: 5}}) {
		t.Fatalf("gaps = %#v", got.Gaps)
	}

	bad := CheckSequenceIntegrity([]int32{1, 1, 0})
	if bad.Valid || bad.StrictlyIncreasing || bad.DuplicateCount != 1 {
		t.Fatalf("invalid sequence reported valid: %#v", bad)
	}
}
