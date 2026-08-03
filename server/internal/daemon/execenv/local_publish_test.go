package execenv

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func commitTaskFile(t *testing.T, wt *IsolatedLocalWorktree, path, content string) string {
	t.Helper()
	full := filepath.Join(wt.WorkDir, filepath.FromSlash(path))
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	git(t, wt.WorkDir, "add", "--", path)
	git(t, wt.WorkDir, "commit", "-qm", "task change")
	return git(t, wt.WorkDir, "rev-parse", "HEAD")
}

func assertQuarantine(t *testing.T, repo, taskID, wantSHA string) {
	t.Helper()
	if got := git(t, repo, "rev-parse", QuarantineRefPrefix+taskID); got != wantSHA {
		t.Fatalf("quarantine ref = %s, want %s", got, wantSHA)
	}
}

func assertNoQuarantine(t *testing.T, repo, taskID string) {
	t.Helper()
	cmd := exec.Command("git", "-C", repo, "rev-parse", "--verify", QuarantineRefPrefix+taskID)
	if err := cmd.Run(); err == nil {
		t.Fatalf("empty quarantine ref %s unexpectedly exists", QuarantineRefPrefix+taskID)
	}
}

func assertWorktreeRemoved(t *testing.T, src string, wt *IsolatedLocalWorktree) {
	t.Helper()
	if list := worktreeList(t, src); strings.Contains(list, wt.WorkDir) {
		t.Fatalf("worktree survived finalization:\n%s", list)
	}
	if branches := listBranchesWithPrefix(src, worktreeBranchPrefix); len(branches) != 0 {
		t.Fatalf("task branch survived finalization: %v", branches)
	}
	if branches := listBranchesWithPrefix(src, publishBackBranchPrefix); len(branches) != 0 {
		t.Fatalf("publish-back branch survived finalization: %v", branches)
	}
}

func TestFirstPathOverlapFindsBothAncestorDirections(t *testing.T) {
	for _, tc := range []struct {
		name string
		a    map[string]struct{}
		b    map[string]struct{}
		want bool
	}{
		{name: "same", a: map[string]struct{}{"a/b": {}}, b: map[string]struct{}{"a/b": {}}, want: true},
		{name: "left below right", a: map[string]struct{}{"a/b/c": {}}, b: map[string]struct{}{"a/b": {}}, want: true},
		{name: "right below left", a: map[string]struct{}{"a/b": {}}, b: map[string]struct{}{"a/b/c": {}}, want: true},
		{name: "prefix is not ancestor", a: map[string]struct{}{"a/bb": {}}, b: map[string]struct{}{"a/b": {}}, want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, got := firstPathOverlap(tc.a, tc.b)
			if got != tc.want {
				t.Fatalf("overlap = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestWorkflowBundleBuildsFromOwnedTaskBranch(t *testing.T) {
	src := newSourceRepo(t)
	wt, err := prepareIsolatedLocalWorktree(
		src,
		isolatedWorkDir(t.TempDir()),
		"task-bundle-11111111",
		nil,
		true,
	)
	if err != nil {
		t.Fatal(err)
	}
	taskSHA := commitTaskFile(t, wt, "pipeline-output/passage/01-segmented.md", "segmented\n")

	artifact, err := wt.BuildWorkflowBundle(
		"codex",
		t.TempDir(),
		[]string{"pipeline-output/passage/01-segmented.md"},
	)
	if err != nil {
		t.Fatalf("build workflow bundle: %v", err)
	}
	if artifact.BaseCommit != wt.BaseCommit || artifact.ResultCommit != taskSHA {
		t.Fatalf(
			"bundle commits base=%s result=%s, want %s and %s",
			artifact.BaseCommit,
			artifact.ResultCommit,
			wt.BaseCommit,
			taskSHA,
		)
	}
	if len(artifact.ChangedPaths) != 1 || artifact.ChangedPaths[0] != "pipeline-output/passage/01-segmented.md" {
		t.Fatalf("bundle changed paths = %v", artifact.ChangedPaths)
	}
	heads := git(t, src, "bundle", "list-heads", artifact.Path)
	if !strings.Contains(heads, taskSHA+" refs/heads/"+wt.Branch) {
		t.Fatalf("bundle does not advertise the owned task branch:\n%s", heads)
	}
	if err := wt.FinalizeWorkflowBundle(true, nil); err != nil {
		t.Fatalf("finalize submitted bundle: %v", err)
	}
	assertWorktreeRemoved(t, src, wt)
}

func TestSerialPublishBack_FastForwardsAndPreservesUnrelatedDirt(t *testing.T) {
	src := newSourceRepo(t)
	base := git(t, src, "rev-parse", "HEAD")
	wt, err := PrepareIsolatedLocalWorktree(src, isolatedWorkDir(t.TempDir()), "task-11111111", nil)
	if err != nil {
		t.Fatal(err)
	}
	if wt.BaseCommit != base {
		t.Fatalf("BaseCommit = %s, want %s", wt.BaseCommit, base)
	}
	taskSHA := commitTaskFile(t, wt, "published.txt", "published\n")

	seedBytes := []byte("user dirt\n")
	noteBytes := []byte("untracked user note\n")
	if err := os.WriteFile(filepath.Join(src, "seed.txt"), seedBytes, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "notes.txt"), noteBytes, 0o644); err != nil {
		t.Fatal(err)
	}

	if err := wt.FinalizeSerialPublishBack(true, "claude", nil); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if got := git(t, src, "rev-parse", "HEAD"); got != taskSHA {
		t.Fatalf("source HEAD = %s, want %s", got, taskSHA)
	}
	if got, err := os.ReadFile(filepath.Join(src, "seed.txt")); err != nil || string(got) != string(seedBytes) {
		t.Fatalf("tracked dirt changed: bytes=%q err=%v", got, err)
	}
	if got, err := os.ReadFile(filepath.Join(src, "notes.txt")); err != nil || string(got) != string(noteBytes) {
		t.Fatalf("untracked dirt changed: bytes=%q err=%v", got, err)
	}
	if got, err := os.ReadFile(filepath.Join(src, "published.txt")); err != nil || string(got) != "published\n" {
		t.Fatalf("published file missing: bytes=%q err=%v", got, err)
	}
	assertWorktreeRemoved(t, src, wt)
}

func TestSerialPublishBack_SourceHeadMovedQuarantinesWithoutTouchingSource(t *testing.T) {
	src := newSourceRepo(t)
	wt, err := PrepareIsolatedLocalWorktree(src, isolatedWorkDir(t.TempDir()), "task-22222222", nil)
	if err != nil {
		t.Fatal(err)
	}
	taskSHA := commitTaskFile(t, wt, "task.txt", "task\n")
	if err := os.WriteFile(filepath.Join(src, "human.txt"), []byte("human\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	git(t, src, "add", "human.txt")
	git(t, src, "commit", "-qm", "human advance")
	sourceSHA := git(t, src, "rev-parse", "HEAD")

	err = wt.FinalizeSerialPublishBack(true, "claude", nil)
	if err == nil || !strings.Contains(err.Error(), "source HEAD moved") {
		t.Fatalf("error = %v, want source HEAD conflict", err)
	}
	if got := git(t, src, "rev-parse", "HEAD"); got != sourceSHA {
		t.Fatalf("source HEAD changed after conflict: %s != %s", got, sourceSHA)
	}
	assertQuarantine(t, src, wt.TaskID, taskSHA)
	assertWorktreeRemoved(t, src, wt)
}

func TestSerialPublishBack_StagedSourceQuarantinesAndPreservesIndex(t *testing.T) {
	src := newSourceRepo(t)
	base := git(t, src, "rev-parse", "HEAD")
	wt, err := PrepareIsolatedLocalWorktree(src, isolatedWorkDir(t.TempDir()), "task-33333333", nil)
	if err != nil {
		t.Fatal(err)
	}
	taskSHA := commitTaskFile(t, wt, "task.txt", "task\n")
	if err := os.WriteFile(filepath.Join(src, "staged.txt"), []byte("staged\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	git(t, src, "add", "staged.txt")
	indexBefore := git(t, src, "diff", "--cached", "--binary")

	err = wt.FinalizeSerialPublishBack(true, "claude", nil)
	if err == nil || !strings.Contains(err.Error(), "staged changes") {
		t.Fatalf("error = %v, want staged source refusal", err)
	}
	if got := git(t, src, "rev-parse", "HEAD"); got != base {
		t.Fatalf("source HEAD changed: %s != %s", got, base)
	}
	if got := git(t, src, "diff", "--cached", "--binary"); got != indexBefore {
		t.Fatal("source index changed during failed publish")
	}
	assertQuarantine(t, src, wt.TaskID, taskSHA)
	assertWorktreeRemoved(t, src, wt)
}

func TestSerialPublishBack_OverlappingSourceEditQuarantinesByteExact(t *testing.T) {
	src := newSourceRepo(t)
	base := git(t, src, "rev-parse", "HEAD")
	wt, err := PrepareIsolatedLocalWorktree(src, isolatedWorkDir(t.TempDir()), "task-44444444", nil)
	if err != nil {
		t.Fatal(err)
	}
	taskSHA := commitTaskFile(t, wt, "seed.txt", "task edit\n")
	humanBytes := []byte("human edit\n")
	if err := os.WriteFile(filepath.Join(src, "seed.txt"), humanBytes, 0o644); err != nil {
		t.Fatal(err)
	}

	err = wt.FinalizeSerialPublishBack(true, "claude", nil)
	if err == nil || !strings.Contains(err.Error(), "overlaps dirty source path") {
		t.Fatalf("error = %v, want overlap refusal", err)
	}
	if got := git(t, src, "rev-parse", "HEAD"); got != base {
		t.Fatalf("source HEAD changed: %s != %s", got, base)
	}
	if got, readErr := os.ReadFile(filepath.Join(src, "seed.txt")); readErr != nil || string(got) != string(humanBytes) {
		t.Fatalf("source bytes changed: bytes=%q err=%v", got, readErr)
	}
	assertQuarantine(t, src, wt.TaskID, taskSHA)
	assertWorktreeRemoved(t, src, wt)
}

func TestSerialPublishBack_OverlappingIgnoredSourceFileQuarantines(t *testing.T) {
	src := newSourceRepo(t)
	if err := os.WriteFile(filepath.Join(src, ".gitignore"), []byte("generated/\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	git(t, src, "add", ".gitignore")
	git(t, src, "commit", "-qm", "ignore generated files")
	base := git(t, src, "rev-parse", "HEAD")
	wt, err := PrepareIsolatedLocalWorktree(src, isolatedWorkDir(t.TempDir()), "task-45454545", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(wt.WorkDir, "generated"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(wt.WorkDir, "generated", "output.txt"), []byte("task output\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	git(t, wt.WorkDir, "add", "-f", "generated/output.txt")
	git(t, wt.WorkDir, "commit", "-qm", "task generated output")
	taskSHA := git(t, wt.WorkDir, "rev-parse", "HEAD")
	ignoredBytes := []byte("local generated output\n")
	if err := os.MkdirAll(filepath.Join(src, "generated"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "generated", "output.txt"), ignoredBytes, 0o644); err != nil {
		t.Fatal(err)
	}

	err = wt.FinalizeSerialPublishBack(true, "claude", nil)
	if err == nil || !strings.Contains(err.Error(), "overlaps ignored source path") {
		t.Fatalf("error = %v, want ignored overlap refusal", err)
	}
	if got := git(t, src, "rev-parse", "HEAD"); got != base {
		t.Fatalf("source HEAD changed: %s != %s", got, base)
	}
	if got, readErr := os.ReadFile(filepath.Join(src, "generated", "output.txt")); readErr != nil || string(got) != string(ignoredBytes) {
		t.Fatalf("ignored source bytes changed: bytes=%q err=%v", got, readErr)
	}
	assertQuarantine(t, src, wt.TaskID, taskSHA)
	assertWorktreeRemoved(t, src, wt)
}

// A task that produced no commit has nothing to publish, so leftover dirt in the
// disposable worktree must not be reported as a publish conflict. The dirt is
// discarded with the worktree and can never reach the source either way; the
// pre-VWO-511 refusal here made every run-only maintenance sweep unfailable-safe
// only by accident, and any scratch at all turned a correct run into `blocked`.
func TestSerialPublishBack_ZeroCommitStagingPublishesNothingAndSucceeds(t *testing.T) {
	src := newSourceRepo(t)
	base := git(t, src, "rev-parse", "HEAD")
	wt, err := PrepareIsolatedLocalWorktree(src, isolatedWorkDir(t.TempDir()), "task-55555555", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(wt.WorkDir, "staged.txt"), []byte("staged\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	git(t, wt.WorkDir, "add", "staged.txt")

	if err := wt.FinalizeSerialPublishBack(true, "claude", testLogger()); err != nil {
		t.Fatalf("error = %v, want a zero-commit run to succeed without publishing", err)
	}
	if got := git(t, src, "rev-parse", "HEAD"); got != base {
		t.Fatalf("source HEAD changed: %s != %s", got, base)
	}
	assertNoQuarantine(t, src, wt.TaskID)
	assertWorktreeRemoved(t, src, wt)
}

// The literal VWO-511 shape: a run-only sweep cloned the canonical repository
// into its own task worktree, leaving one untracked directory and no commit.
func TestSerialPublishBack_ZeroCommitUntrackedNestedCheckoutSucceeds(t *testing.T) {
	src := newSourceRepo(t)
	base := git(t, src, "rev-parse", "HEAD")
	wt, err := PrepareIsolatedLocalWorktree(src, isolatedWorkDir(t.TempDir()), "task-55555556", nil)
	if err != nil {
		t.Fatal(err)
	}
	nested := filepath.Join(wt.WorkDir, "nested-checkout")
	if err := os.MkdirAll(nested, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(nested, "sweep-scratch.txt"), []byte("scratch\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := wt.FinalizeSerialPublishBack(true, "claude", testLogger()); err != nil {
		t.Fatalf("error = %v, want a zero-commit sweep to succeed without publishing", err)
	}
	if got := git(t, src, "rev-parse", "HEAD"); got != base {
		t.Fatalf("source HEAD changed: %s != %s", got, base)
	}
	assertNoQuarantine(t, src, wt.TaskID)
	assertWorktreeRemoved(t, src, wt)
}

// The clean-tree gate still guards a real publication: once there IS a commit to
// fast-forward, leftover dirt means daemon cleanup may have altered the tree the
// commit was built from, so the publish is refused and the commit quarantined.
// The refusal must also name what was dirty: every VWO-511 failure comment was
// unactionable without reading the agent's rollout transcript.
func TestSerialPublishBack_DirtyWorktreeAlongsideCommitRefusesAndNamesPaths(t *testing.T) {
	src := newSourceRepo(t)
	base := git(t, src, "rev-parse", "HEAD")
	wt, err := PrepareIsolatedLocalWorktree(src, isolatedWorkDir(t.TempDir()), "task-55555557", nil)
	if err != nil {
		t.Fatal(err)
	}
	taskSHA := commitTaskFile(t, wt, "published.txt", "published\n")
	if err := os.WriteFile(filepath.Join(wt.WorkDir, "leftover.txt"), []byte("leftover\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	git(t, wt.WorkDir, "add", "leftover.txt")

	err = wt.FinalizeSerialPublishBack(true, "claude", testLogger())
	if err == nil || !strings.Contains(err.Error(), "staged or uncommitted") {
		t.Fatalf("error = %v, want dirty task refusal", err)
	}
	if !strings.Contains(err.Error(), "leftover.txt") {
		t.Fatalf("error = %v, want the offending path named", err)
	}
	if got := git(t, src, "rev-parse", "HEAD"); got != base {
		t.Fatalf("source HEAD changed: %s != %s", got, base)
	}
	assertQuarantine(t, src, wt.TaskID, taskSHA)
	assertWorktreeRemoved(t, src, wt)
	if note := wt.RecoveryNote(); !strings.Contains(note, QuarantineRefPrefix+wt.TaskID) {
		t.Fatalf("recovery note = %q, want the quarantine ref that was actually created", note)
	}
}

// The operator comment is built from the finalizer's own disposition, so a run
// whose tip needs no quarantine must not claim a ref that was never created.
func TestSerialPublishBack_RecoveryNoteReportsReachableTipInsteadOfMissingRef(t *testing.T) {
	src := newSourceRepo(t)
	wt, err := PrepareIsolatedLocalWorktree(src, isolatedWorkDir(t.TempDir()), "task-55555558", nil)
	if err != nil {
		t.Fatal(err)
	}

	if err := wt.FinalizeSerialPublishBack(false, "claude", testLogger()); err != nil {
		t.Fatal(err)
	}
	assertNoQuarantine(t, src, wt.TaskID)
	note := wt.RecoveryNote()
	if strings.Contains(note, QuarantineRefPrefix) {
		t.Fatalf("recovery note = %q, want no quarantine ref claim when none was created", note)
	}
	if !strings.Contains(note, "reachable from source HEAD") {
		t.Fatalf("recovery note = %q, want the reachability disposition", note)
	}
}

// Succeeding a zero-commit run makes the daemon log the only surviving trace of
// anything the task left behind, so the warning must actually reach a real
// logger on the production path, and it must name the agent's own leftovers
// without the daemon sidecars and the injected brief drowning them out.
func TestSerialPublishBack_ZeroCommitLogsDiscardedAgentWorkOnly(t *testing.T) {
	src := newSourceRepo(t)
	env, err := Prepare(PrepareParams{
		WorkspacesRoot: t.TempDir(),
		WorkspaceID:    "ws-publish-discard-report",
		TaskID:         "task-discard-report",
		Provider:       "claude",
		LocalWorkDir:   src,
		Isolate:        true,
		PublishBack:    true,
		Task:           TaskContextForEnv{IssueID: "issue-discard-report"},
	}, testLogger())
	if err != nil {
		t.Fatal(err)
	}
	wt := env.IsolatedWorktree
	if _, err := InjectRuntimeConfig(wt.WorkDir, "claude", TaskContextForEnv{IssueID: "issue-discard-report"}); err != nil {
		t.Fatal(err)
	}
	// What the agent itself left behind, uncommitted, in a run that never committed.
	if err := os.WriteFile(filepath.Join(wt.WorkDir, "regenerated-artifact.yaml"), []byte("resolved\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	var (
		mu      sync.Mutex
		records []slog.Record
	)
	logger := slog.New(capturingHandler{mu: &mu, records: &records})

	if err := wt.FinalizeSerialPublishBack(true, "claude", logger); err != nil {
		t.Fatalf("error = %v, want a zero-commit run to succeed", err)
	}

	var discard *slog.Record
	for i := range records {
		if strings.Contains(records[i].Message, "discarding uncommitted worktree changes") {
			discard = &records[i]
			break
		}
	}
	if discard == nil {
		t.Fatalf("no discard warning reached the logger; records = %v", records)
	}
	var entries string
	discard.Attrs(func(a slog.Attr) bool {
		if a.Key == "entries" {
			entries = a.Value.String()
		}
		return true
	})
	if !strings.Contains(entries, "regenerated-artifact.yaml") {
		t.Fatalf("entries = %q, want the agent's discarded artifact named", entries)
	}
	// CLAUDE.md carries the injected brief and .agent_context/ is the sidecar
	// tree. Cleanup runs before the report precisely so neither is reported as
	// lost work; if either shows up the warning is noise on every clean sweep.
	if strings.Contains(entries, "CLAUDE.md") || strings.Contains(entries, ".agent_context") {
		t.Fatalf("entries = %q, want daemon-managed material excluded from the discard report", entries)
	}
	assertWorktreeRemoved(t, src, wt)
}

// capturingHandler records every slog record at or above Warn so a test can
// assert on what an operator would actually see in the daemon log.
type capturingHandler struct {
	mu      *sync.Mutex
	records *[]slog.Record
}

func (h capturingHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= slog.LevelWarn
}

func (h capturingHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	*h.records = append(*h.records, r.Clone())
	return nil
}

func (h capturingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }

func (h capturingHandler) WithGroup(string) slog.Handler { return h }

func TestSummarizeDirtyEntriesCapsAndCounts(t *testing.T) {
	var entries []string
	for i := 0; i < maxReportedDirtyEntries+3; i++ {
		entries = append(entries, fmt.Sprintf("?? scratch-%02d.txt", i))
	}
	got := summarizeDirtyEntries(entries)
	if !strings.HasSuffix(got, "and 3 more") {
		t.Fatalf("summary = %q, want a trailing overflow count", got)
	}
	if strings.Contains(got, "scratch-10.txt") {
		t.Fatalf("summary = %q, want entries past the cap omitted", got)
	}
	short := summarizeDirtyEntries(entries[:2])
	if short != "?? scratch-00.txt; ?? scratch-01.txt" {
		t.Fatalf("summary = %q, want both entries joined without an overflow count", short)
	}
}

func TestSerialPublishBack_CommittedDaemonSidecarsQuarantine(t *testing.T) {
	src := newSourceRepo(t)
	base := git(t, src, "rev-parse", "HEAD")
	env, err := Prepare(PrepareParams{
		WorkspacesRoot: t.TempDir(),
		WorkspaceID:    "ws-publish-sidecar",
		TaskID:         "task-66666666",
		Provider:       "claude",
		LocalWorkDir:   src,
		Isolate:        true,
		Task:           TaskContextForEnv{IssueID: "issue-sidecar"},
	}, testLogger())
	if err != nil {
		t.Fatal(err)
	}
	wt := env.IsolatedWorktree
	if _, err := InjectRuntimeConfig(wt.WorkDir, "claude", TaskContextForEnv{IssueID: "issue-sidecar"}); err != nil {
		t.Fatal(err)
	}
	git(t, wt.WorkDir, "add", "-A")
	git(t, wt.WorkDir, "commit", "-qm", "incorrectly commit daemon sidecars")
	taskSHA := git(t, wt.WorkDir, "rev-parse", "HEAD")
	if err := os.Remove(filepath.Join(env.RootDir, sidecarManifestFile)); err != nil {
		t.Fatalf("remove sidecar manifest to simulate task tampering: %v", err)
	}

	err = wt.FinalizeSerialPublishBack(true, "claude", nil)
	if err == nil || (!strings.Contains(err.Error(), "daemon-managed runtime material") && !strings.Contains(err.Error(), "injected Multica brief")) {
		t.Fatalf("error = %v, want committed sidecar refusal", err)
	}
	if got := git(t, src, "rev-parse", "HEAD"); got != base {
		t.Fatalf("source HEAD changed: %s != %s", got, base)
	}
	assertQuarantine(t, src, wt.TaskID, taskSHA)
	assertWorktreeRemoved(t, src, wt)
}

func TestSerialPublishBack_CommittedFileUnderManagedSidecarDirectoryQuarantines(t *testing.T) {
	src := newSourceRepo(t)
	env, err := Prepare(PrepareParams{
		WorkspacesRoot: t.TempDir(),
		WorkspaceID:    "ws-publish-sidecar-child",
		TaskID:         "task-sidecar-child",
		Provider:       "claude",
		LocalWorkDir:   src,
		Isolate:        true,
		PublishBack:    true,
		Task:           TaskContextForEnv{IssueID: "issue-sidecar-child"},
	}, testLogger())
	if err != nil {
		t.Fatal(err)
	}
	wt := env.IsolatedWorktree
	path := filepath.Join(wt.WorkDir, ".agent_context", "agent-note.md")
	if err := os.WriteFile(path, []byte("agent-owned residue\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	git(t, wt.WorkDir, "add", ".agent_context/agent-note.md")
	git(t, wt.WorkDir, "commit", "-qm", "incorrectly commit inside sidecar directory")
	taskSHA := git(t, wt.WorkDir, "rev-parse", "HEAD")

	err = wt.FinalizeSerialPublishBack(true, "claude", nil)
	if err == nil || !strings.Contains(err.Error(), "daemon-managed runtime material") {
		t.Fatalf("error = %v, want managed-directory refusal", err)
	}
	assertQuarantine(t, src, wt.TaskID, taskSHA)
	assertWorktreeRemoved(t, src, wt)
}

func TestSerialPublishBack_CommittedInjectedRuntimeBriefQuarantines(t *testing.T) {
	src := newSourceRepo(t)
	env, err := Prepare(PrepareParams{
		WorkspacesRoot: t.TempDir(),
		WorkspaceID:    "ws-publish-runtime-brief",
		TaskID:         "task-runtime-brief",
		Provider:       "claude",
		LocalWorkDir:   src,
		Isolate:        true,
		PublishBack:    true,
		Task:           TaskContextForEnv{IssueID: "issue-runtime-brief"},
	}, testLogger())
	if err != nil {
		t.Fatal(err)
	}
	wt := env.IsolatedWorktree
	if _, err := InjectRuntimeConfig(wt.WorkDir, "claude", TaskContextForEnv{IssueID: "issue-runtime-brief"}); err != nil {
		t.Fatal(err)
	}
	git(t, wt.WorkDir, "add", "CLAUDE.md")
	git(t, wt.WorkDir, "commit", "-qm", "incorrectly commit runtime brief")
	taskSHA := git(t, wt.WorkDir, "rev-parse", "HEAD")

	err = wt.FinalizeSerialPublishBack(true, "claude", nil)
	if err == nil || !strings.Contains(err.Error(), "staged or uncommitted changes after sidecar cleanup") {
		t.Fatalf("error = %v, want committed runtime brief refusal", err)
	}
	assertQuarantine(t, src, wt.TaskID, taskSHA)
	assertWorktreeRemoved(t, src, wt)
}

func TestSerialPublishBack_LegitimateRuntimeFileEditPublishesAfterBriefRemoval(t *testing.T) {
	src := newSourceRepo(t)
	env, err := Prepare(PrepareParams{
		WorkspacesRoot: t.TempDir(),
		WorkspaceID:    "ws-publish-runtime-edit",
		TaskID:         "task-runtime-edit",
		Provider:       "claude",
		LocalWorkDir:   src,
		Isolate:        true,
		PublishBack:    true,
		Task:           TaskContextForEnv{IssueID: "issue-runtime-edit"},
	}, testLogger())
	if err != nil {
		t.Fatal(err)
	}
	wt := env.IsolatedWorktree
	if _, err := InjectRuntimeConfig(wt.WorkDir, "claude", TaskContextForEnv{IssueID: "issue-runtime-edit"}); err != nil {
		t.Fatal(err)
	}
	if err := CleanupRuntimeConfig(wt.WorkDir, "claude"); err != nil {
		t.Fatal(err)
	}
	const content = "# Repository instructions\n\nKeep this file.\n"
	if err := os.WriteFile(filepath.Join(wt.WorkDir, "CLAUDE.md"), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	git(t, wt.WorkDir, "add", "CLAUDE.md")
	git(t, wt.WorkDir, "commit", "-qm", "add repository instructions")
	taskSHA := git(t, wt.WorkDir, "rev-parse", "HEAD")

	if err := wt.FinalizeSerialPublishBack(true, "claude", nil); err != nil {
		t.Fatalf("publish legitimate runtime file edit: %v", err)
	}
	if got := git(t, src, "rev-parse", "HEAD"); got != taskSHA {
		t.Fatalf("source HEAD = %s, want %s", got, taskSHA)
	}
	if got, err := os.ReadFile(filepath.Join(src, "CLAUDE.md")); err != nil || string(got) != content {
		t.Fatalf("published CLAUDE.md = %q, %v", got, err)
	}
	assertWorktreeRemoved(t, src, wt)
}

func TestSerialPublishBack_NormalDaemonSidecarsAreRemovedBeforePublish(t *testing.T) {
	src := newSourceRepo(t)
	env, err := Prepare(PrepareParams{
		WorkspacesRoot: t.TempDir(),
		WorkspaceID:    "ws-publish-clean-sidecar",
		TaskID:         "task-67676767",
		Provider:       "claude",
		LocalWorkDir:   src,
		Isolate:        true,
		Task:           TaskContextForEnv{IssueID: "issue-clean-sidecar"},
	}, testLogger())
	if err != nil {
		t.Fatal(err)
	}
	wt := env.IsolatedWorktree
	if _, err := InjectRuntimeConfig(wt.WorkDir, "claude", TaskContextForEnv{IssueID: "issue-clean-sidecar"}); err != nil {
		t.Fatal(err)
	}
	taskSHA := commitTaskFile(t, wt, "work.txt", "durable work\n")

	if err := wt.FinalizeSerialPublishBack(true, "claude", nil); err != nil {
		t.Fatalf("publish with normal daemon sidecars: %v", err)
	}
	if got := git(t, src, "rev-parse", "HEAD"); got != taskSHA {
		t.Fatalf("source HEAD = %s, want %s", got, taskSHA)
	}
	if _, err := os.Stat(filepath.Join(src, ".agent_context")); !os.IsNotExist(err) {
		t.Fatalf("daemon sidecar leaked into source: %v", err)
	}
	assertWorktreeRemoved(t, src, wt)
}

func TestSerialPublishBack_NonCompletedDispositionQuarantines(t *testing.T) {
	src := newSourceRepo(t)
	base := git(t, src, "rev-parse", "HEAD")
	wt, err := PrepareIsolatedLocalWorktree(src, isolatedWorkDir(t.TempDir()), "task-77777777", nil)
	if err != nil {
		t.Fatal(err)
	}
	taskSHA := commitTaskFile(t, wt, "partial.txt", "partial\n")

	if err := wt.FinalizeSerialPublishBack(false, "claude", nil); err != nil {
		t.Fatalf("quarantine: %v", err)
	}
	if got := git(t, src, "rev-parse", "HEAD"); got != base {
		t.Fatalf("source HEAD changed: %s != %s", got, base)
	}
	assertQuarantine(t, src, wt.TaskID, taskSHA)
	assertWorktreeRemoved(t, src, wt)
}

func TestSerialPublishBack_NonCompletedWithoutTaskCommitSkipsEmptyQuarantine(t *testing.T) {
	src := newSourceRepo(t)
	wt, err := PrepareIsolatedLocalWorktree(src, isolatedWorkDir(t.TempDir()), "task-no-commit", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := wt.FinalizeSerialPublishBack(false, "claude", nil); err != nil {
		t.Fatalf("finalize without task commit: %v", err)
	}
	assertNoQuarantine(t, src, wt.TaskID)
	assertWorktreeRemoved(t, src, wt)
}
