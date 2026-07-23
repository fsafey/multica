package execenv

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
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

func TestSerialPublishBack_TaskStagingRefusesWithoutEmptyQuarantine(t *testing.T) {
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

	err = wt.FinalizeSerialPublishBack(true, "claude", nil)
	if err == nil || !strings.Contains(err.Error(), "staged or uncommitted") {
		t.Fatalf("error = %v, want dirty task refusal", err)
	}
	if got := git(t, src, "rev-parse", "HEAD"); got != base {
		t.Fatalf("source HEAD changed: %s != %s", got, base)
	}
	assertNoQuarantine(t, src, wt.TaskID)
	assertWorktreeRemoved(t, src, wt)
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
