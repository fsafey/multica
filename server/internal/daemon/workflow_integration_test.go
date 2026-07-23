package daemon

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func workflowTestGit(t *testing.T, dir string, arguments ...string) string {
	t.Helper()
	command := exec.Command("git", append([]string{"-C", dir}, arguments...)...)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %s: %v", strings.Join(arguments, " "), output, err)
	}
	return strings.TrimSpace(string(output))
}

func workflowTestRepo(t *testing.T) (string, string) {
	t.Helper()
	repo := t.TempDir()
	workflowTestGit(t, repo, "init", "-b", "main")
	if err := os.WriteFile(filepath.Join(repo, "base.txt"), []byte("base\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	workflowTestGit(t, repo, "add", ".")
	workflowTestGit(
		t,
		repo,
		"-c",
		"user.name=Test",
		"-c",
		"user.email=test@example.invalid",
		"commit",
		"-m",
		"base",
	)
	return repo, workflowTestGit(t, repo, "rev-parse", "HEAD")
}

func workflowTestBundle(
	t *testing.T,
	repo, base, path, content, nodeID, attemptID string,
	epoch int64,
) (string, *WorkflowIntegrationJobData) {
	t.Helper()
	worktree := filepath.Join(t.TempDir(), "worktree")
	workflowTestGit(t, repo, "worktree", "add", "--detach", worktree, base)
	t.Cleanup(func() {
		workflowTestGit(t, repo, "worktree", "remove", "--force", worktree)
	})
	target := filepath.Join(worktree, filepath.FromSlash(path))
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(target, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	workflowTestGit(t, worktree, "add", ".")
	workflowTestGit(
		t,
		worktree,
		"-c",
		"user.name=Worker",
		"-c",
		"user.email=worker@example.invalid",
		"commit",
		"-m",
		"result",
	)
	result := workflowTestGit(t, worktree, "rev-parse", "HEAD")
	bundle := filepath.Join(t.TempDir(), attemptID+".bundle")
	workflowTestGit(t, worktree, "bundle", "create", bundle, "HEAD", "^"+base)
	bytes, err := os.ReadFile(bundle)
	if err != nil {
		t.Fatal(err)
	}
	digest := fmt.Sprintf("sha256:%x", sha256.Sum256(bytes))
	manifest, err := json.Marshal(workflowBundleManifest{
		RunID:        "run",
		PassageKey:   "passage",
		NodeID:       nodeID,
		NodeKey:      nodeID,
		Generation:   1,
		AttemptID:    attemptID,
		ClaimEpoch:   epoch,
		BaseCommit:   base,
		ResultCommit: result,
		AgentID:      "agent",
		RuntimeID:    "runtime",
		ChangedPaths: []string{path},
		BundleSHA256: digest,
	})
	if err != nil {
		t.Fatal(err)
	}
	contract, _ := json.Marshal(map[string]any{"allowed_paths": []string{path}})
	return bundle, &WorkflowIntegrationJobData{
		EventID:        "event-" + attemptID,
		EventType:      "workflow.artifact_submitted",
		RunID:          "run",
		NodeID:         nodeID,
		AttemptID:      attemptID,
		PassageKey:     "passage",
		NodeKey:        nodeID,
		Generation:     1,
		ClaimEpoch:     epoch,
		BaseCommit:     base,
		ResultCommit:   result,
		ArtifactDigest: digest,
		ArtifactSize:   int64(len(bytes)),
		Manifest:       manifest,
		OutputContract: contract,
	}
}

func TestIntegrateWorkflowBundleSerializesDisjointResultsFromSameBase(t *testing.T) {
	repo, base := workflowTestRepo(t)
	bundleA, jobA := workflowTestBundle(t, repo, base, "a.txt", "a\n", "node-a", "attempt-a", 1)
	bundleB, jobB := workflowTestBundle(t, repo, base, "b.txt", "b\n", "node-b", "attempt-b", 1)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	if _, err := integrateWorkflowBundle(context.Background(), repo, bundleA, filepath.Join(t.TempDir(), "integrate"), jobA, logger); err != nil {
		t.Fatalf("integrate A: %v", err)
	}
	if _, err := integrateWorkflowBundle(context.Background(), repo, bundleB, filepath.Join(t.TempDir(), "integrate"), jobB, logger); err != nil {
		t.Fatalf("integrate B: %v", err)
	}
	for _, path := range []string{"a.txt", "b.txt"} {
		if _, err := os.Stat(filepath.Join(repo, path)); err != nil {
			t.Fatalf("missing integrated %s: %v", path, err)
		}
	}
}

func TestIntegrateWorkflowBundleConflictLeavesCanonicalHeadUntouched(t *testing.T) {
	repo, base := workflowTestRepo(t)
	bundleA, jobA := workflowTestBundle(t, repo, base, "base.txt", "first\n", "node-a", "attempt-a", 1)
	bundleB, jobB := workflowTestBundle(t, repo, base, "base.txt", "second\n", "node-b", "attempt-b", 1)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	if _, err := integrateWorkflowBundle(context.Background(), repo, bundleA, filepath.Join(t.TempDir(), "integrate"), jobA, logger); err != nil {
		t.Fatalf("integrate A: %v", err)
	}
	headAfterA := workflowTestGit(t, repo, "rev-parse", "HEAD")
	if _, err := integrateWorkflowBundle(context.Background(), repo, bundleB, filepath.Join(t.TempDir(), "integrate"), jobB, logger); err == nil {
		t.Fatal("conflicting integration succeeded")
	}
	if got := workflowTestGit(t, repo, "rev-parse", "HEAD"); got != headAfterA {
		t.Fatalf("canonical head changed after conflict: got %s want %s", got, headAfterA)
	}
	if got, err := os.ReadFile(filepath.Join(repo, "base.txt")); err != nil || string(got) != "first\n" {
		t.Fatalf("canonical content changed after conflict: %q, %v", got, err)
	}
}

func TestIntegrateDeterministicWorkflowNodeCreatesReplayableNoopCheckpoint(t *testing.T) {
	repo, _ := workflowTestRepo(t)
	contract, _ := json.Marshal(deterministicWorkflowContract{
		Operation:      "repository_command_v1",
		Command:        []string{"true"},
		AllowedPaths:   []string{"issue-log.md"},
		TimeoutSeconds: 10,
	})
	digest := fmt.Sprintf("sha256:%x", sha256.Sum256(contract))
	manifest, _ := json.Marshal(deterministicWorkflowManifest{
		ExecutorKind:   "deterministic",
		Operation:      "repository_command_v1",
		NodeID:         "node",
		AttemptID:      "attempt",
		ClaimEpoch:     1,
		ContractDigest: digest,
	})
	job := &WorkflowIntegrationJobData{
		EventID:        "event",
		EventType:      "workflow.deterministic_ready",
		NodeID:         "node",
		AttemptID:      "attempt",
		PassageKey:     "passage",
		NodeKey:        "join",
		ClaimEpoch:     1,
		ArtifactDigest: digest,
		Manifest:       manifest,
		OutputContract: contract,
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	first, err := integrateDeterministicWorkflowNode(
		context.Background(),
		repo,
		filepath.Join(t.TempDir(), "integrate"),
		job,
		logger,
	)
	if err != nil {
		t.Fatalf("first deterministic integration: %v", err)
	}
	second, err := integrateDeterministicWorkflowNode(
		context.Background(),
		repo,
		filepath.Join(t.TempDir(), "integrate"),
		job,
		logger,
	)
	if err != nil {
		t.Fatalf("replayed deterministic integration: %v", err)
	}
	if first != second {
		t.Fatalf("replay commit = %s, want %s", second, first)
	}
}
