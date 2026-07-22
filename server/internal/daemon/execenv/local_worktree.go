package execenv

import (
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
)

// local_worktree.go — per-task worktree isolation for local_directory tasks (VWO-367).
//
// Background. A local_directory project_resource binds a task's WorkDir to a
// user's live checkout so the agent edits that tree in place (execenv.Prepare,
// LocalWorkDir). To keep two tasks from corrupting each other's sidecars and git
// index in that one shared tree, the daemon serialises every local_directory
// task on a whole-task path mutex (LocalPathLocker, keyed on the canonical git
// repository root for git-backed directories).
// For a fleet whose agents all target one checkout that mutex collapses their
// designed concurrency to strictly one task at a time.
//
// Isolation. When the local_directory resource opts in (Isolate), the daemon
// instead cuts a *per-task git worktree* off the same repository: the task gets
// its own working tree and its own git index under the task's envRoot, sharing
// only the repository's object store and refs. Two tasks on the same checkout no
// longer share a working tree, an index, or a sidecar directory, so the
// whole-task mutex is unnecessary when the worktree is disposable — only the
// brief `git worktree add`/`remove` critical sections (which mutate shared .git
// metadata) need serialising, done here with a per-source-repo in-process lock.
// publish_back=serial_ff deliberately retains the whole-task path mutex because
// it advances the source checkout at the end of a completed run. Cross-process
// add/remove races are handled by git's lockfiles; two-daemon safety is
// VWO-365's single-owner lock.
//
// Ownership of the git lifecycle:
//   - create   : PrepareIsolatedLocalWorktree — `git worktree add -b
//     multica/worktree/<shortTaskID> <envRoot/workdir> HEAD`, off the checkout's
//     current HEAD. Sidecars are excluded per-worktree so `git add -A` can't
//     stage them.
//   - commit: the AGENT owns commits on its per-task branch. The daemon never
//     commits. Disposable isolation requires the agent to push durable work.
//     serial_ff publish-back accepts only a clean descendant of BaseCommit and
//     advances the source by exact fast-forward without rebasing.
//   - conflict : surfaced to the agent as an ordinary non-fast-forward / rebase
//     conflict in its own worktree; isolation turns a silent lost-update into a
//     visible conflict.
//   - cleanup  : disposable isolation calls Remove directly. serial_ff first
//     publishes or records refs/multica/quarantine/<task-id>, then removes the
//     worktree and branch. On daemon crash (no deferred cleanup) the worktree
//     dir is GC'd with its envRoot and PruneOrphanLocalWorktrees moves any
//     unique publish-back branch tip to its quarantine ref before reclaiming
//     the dangling registry entry and branch.
//
// Sidecar containment. The daemon writes provider sidecars (.claude/skills/,
// .agent_context/, .multica/, the runtime brief) into WorkDir because providers
// discover them relative to the working directory. In the isolated case WorkDir
// is this per-task worktree, so those sidecars are contained by construction:
// (1) they live on a disposable per-task branch, never on main; (2) Remove runs
// `git worktree remove --force`, discarding every uncommitted/untracked sidecar;
// (3) the fleet stages named paths at gate-park (`git add <paths>`), never
// `git add -A`. We deliberately do NOT write to <repo>/.git/info/exclude: for a
// linked worktree git reads excludes from the COMMON gitdir, so an effective
// exclude would be a write into the user's shared repository state — exactly
// what isolation must avoid. This matches the existing github_repo worktree flow.

// worktreeBranchPrefix namespaces per-task isolation branches so
// PruneOrphanLocalWorktrees and operators can identify daemon-created worktrees.
const worktreeBranchPrefix = "multica/worktree/"
const publishBackBranchPrefix = "multica/publish-back/"

// repoLocks serialises worktree add/remove/prune per source repository within
// this process. The critical section is brief (a single git call), never the
// whole task, so it does not reintroduce the whole-task serialization this
// change removes.
var (
	repoLocksMu sync.Mutex
	repoLocks   = map[string]*sync.Mutex{}
)

func lockForRepo(repoRoot string) *sync.Mutex {
	repoLocksMu.Lock()
	defer repoLocksMu.Unlock()
	m, ok := repoLocks[repoRoot]
	if !ok {
		m = &sync.Mutex{}
		repoLocks[repoRoot] = m
	}
	return m
}

// IsolatedLocalWorktree is a per-task git worktree cut from a user's live
// local_directory checkout.
type IsolatedLocalWorktree struct {
	SourceRepo string // git toplevel of the user's checkout (the worktree's parent)
	WorkDir    string // envRoot/workdir — the isolated working tree
	Branch     string // multica/worktree/<shortTaskID>
	TaskID     string // full task UUID, used for the durable quarantine ref
	BaseCommit string // immutable source HEAD recorded before worktree creation

	// managedPaths is the in-memory authority for daemon-owned files written
	// inside WorkDir. It deliberately does not rely on the on-disk sidecar
	// manifest, which the task process can edit or delete before finalization.
	managedPaths map[string]struct{}
}

// PrepareIsolatedLocalWorktree cuts an isolated worktree for taskID at workDir,
// branched off sourceDir's current HEAD. sourceDir is the user's checkout
// (local_directory local_path). Returns an error when sourceDir is not inside a
// git repository — isolation requires a repo to branch from; the caller should
// fall back to the in-place local_directory flow or fail the task with a clear
// message rather than silently running unisolated.
func PrepareIsolatedLocalWorktree(sourceDir, workDir, taskID string, logger *slog.Logger) (*IsolatedLocalWorktree, error) {
	return prepareIsolatedLocalWorktree(sourceDir, workDir, taskID, logger, false)
}

func prepareIsolatedLocalWorktree(sourceDir, workDir, taskID string, logger *slog.Logger, publishBack bool) (*IsolatedLocalWorktree, error) {
	gitRoot, ok := detectGitRepo(sourceDir)
	if !ok {
		return nil, fmt.Errorf("execenv: local_directory isolation requires a git repository, but %q is not inside one", sourceDir)
	}
	branch := worktreeBranchPrefix + shortID(taskID)
	if publishBack {
		candidate := publishBackBranchPrefix + taskID
		if ok, _ := gitSuccess(gitRoot, "check-ref-format", "refs/heads/"+candidate); !ok {
			return nil, fmt.Errorf("execenv: task ID %q cannot form a crash-recovery branch", taskID)
		}
		branch = candidate
	}

	lock := lockForRepo(gitRoot)
	lock.Lock()
	// Self-heal before adding ours: reclaim BOTH the registry entries and the
	// multica/worktree/* branches left behind by a prior daemon crash (envRoot
	// GC'd, no `git worktree remove` ran). This is what makes crash recovery
	// reachable from the normal task path — the fleet runs many tasks on one
	// repo, so orphans are reaped within one task cycle without any GC wiring.
	// It can never touch a live sibling: prune only drops entries whose dir is
	// missing, and reap skips branches still checked out in a worktree.
	reapOrphansLocked(gitRoot, logger)
	baseCommit, baseErr := gitOutput(gitRoot, "rev-parse", "--verify", "HEAD^{commit}")
	if baseErr != nil {
		lock.Unlock()
		return nil, fmt.Errorf("execenv: resolve local_directory base commit: %w", baseErr)
	}
	// A publish-back branch name carries the full task ID for crash recovery.
	// It must fail closed on collision rather than taking setupGitWorktree's
	// disposable-worktree timestamp suffix.
	actualBranch, err := setupGitWorktree(gitRoot, workDir, branch, baseCommit, !publishBack)
	lock.Unlock()
	if err != nil {
		return nil, fmt.Errorf("execenv: create isolated worktree: %w", err)
	}

	if logger != nil {
		logger.Info("execenv: prepared isolated local worktree", "source", gitRoot, "workdir", workDir, "branch", actualBranch, "base_commit", baseCommit)
	}
	return &IsolatedLocalWorktree{
		SourceRepo:   gitRoot,
		WorkDir:      workDir,
		Branch:       actualBranch,
		TaskID:       taskID,
		BaseCommit:   baseCommit,
		managedPaths: make(map[string]struct{}),
	}, nil
}

func (w *IsolatedLocalWorktree) recordManagedPaths(paths ...string) {
	if w == nil {
		return
	}
	if w.managedPaths == nil {
		w.managedPaths = make(map[string]struct{})
	}
	for _, path := range paths {
		rel, err := filepath.Rel(w.WorkDir, path)
		if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			continue
		}
		w.managedPaths[filepath.ToSlash(rel)] = struct{}{}
	}
}

// Remove tears the worktree down: `git worktree remove --force`, delete the
// per-task branch, and prune dangling registry entries. Idempotent — calling it
// after the worktree is already gone (or twice) is a no-op, not an error. Safe
// to call on the deferred cleanup path AND from GC.
func (w *IsolatedLocalWorktree) Remove(logger *slog.Logger) {
	if w == nil || w.SourceRepo == "" {
		return
	}
	lock := lockForRepo(w.SourceRepo)
	lock.Lock()
	defer lock.Unlock()
	removeGitWorktree(w.SourceRepo, w.WorkDir, w.Branch, orDiscardLogger(logger))
	pruneWorktrees(w.SourceRepo)
}

// PruneOrphanLocalWorktrees reclaims isolation worktrees whose working
// directory has already been removed (the daemon-crash path: the envRoot — and
// with it envRoot/workdir — is GC'd, but no `git worktree remove` ran, leaving a
// dangling entry in <sourceRepo>/.git/worktrees and a multica/worktree/* branch).
// `git worktree prune` drops the dangling registry entries; then disposable
// branches are deleted while publish-back branches are first copied to their
// durable quarantine refs. Safe to run repeatedly and safe against live
// worktrees (prune only removes entries whose directory is missing; branches
// still checked out cannot be deleted).
func PruneOrphanLocalWorktrees(sourceRepo string, logger *slog.Logger) error {
	if sourceRepo == "" {
		return nil
	}
	gitRoot, ok := detectGitRepo(sourceRepo)
	if !ok {
		return nil
	}
	lock := lockForRepo(gitRoot)
	lock.Lock()
	defer lock.Unlock()
	reapOrphansLocked(gitRoot, logger)
	return nil
}

// reapOrphansLocked prunes dangling worktree registry entries and reclaims any
// Multica isolation branch no longer backing a live worktree. Disposable
// multica/worktree/* branches are deleted. Crash-durable
// multica/publish-back/* branches are first copied to the task quarantine ref;
// the branch is deleted only after that ref exists. Caller MUST hold
// lockForRepo(gitRoot). Safe against live worktrees: `git worktree prune` only
// drops entries whose directory is missing, and a branch still checked out in a
// worktree is skipped (and git refuses to delete it anyway).
func reapOrphansLocked(gitRoot string, logger *slog.Logger) {
	pruneWorktrees(gitRoot)
	live := liveWorktreeBranches(gitRoot)
	livePaths := liveWorktreePaths(gitRoot)
	for _, br := range listBranchesWithPrefix(gitRoot, worktreeBranchPrefix) {
		if live[br] {
			continue
		}
		cmd := exec.Command("git", "-C", gitRoot, "branch", "-D", br)
		if out, err := cmd.CombinedOutput(); err != nil && logger != nil {
			logger.Warn("execenv: prune orphan worktree branch failed", "branch", br, "output", strings.TrimSpace(string(out)), "error", err)
		}
	}
	for _, br := range listBranchesWithPrefix(gitRoot, publishBackBranchPrefix) {
		taskID := strings.TrimPrefix(br, publishBackBranchPrefix)
		if live[br] || publishBackWorktreePathLive(livePaths, taskID) {
			continue
		}
		ref := QuarantineRefPrefix + taskID
		taskSHA, err := gitOutput(gitRoot, "rev-parse", "--verify", br+"^{commit}")
		if err != nil {
			if logger != nil {
				logger.Warn("execenv: resolve orphan publish-back branch failed; preserving branch", "branch", br, "error", err)
			}
			continue
		}
		reachable, reachErr := gitSuccess(gitRoot, "merge-base", "--is-ancestor", taskSHA, "HEAD")
		if reachErr != nil {
			if logger != nil {
				logger.Warn("execenv: inspect orphan publish-back reachability failed; preserving branch", "branch", br, "error", reachErr)
			}
			continue
		}
		if reachable {
			cmd := exec.Command("git", "-C", gitRoot, "branch", "-D", br)
			if out, err := cmd.CombinedOutput(); err != nil && logger != nil {
				logger.Warn("execenv: prune reachable orphan publish-back branch failed", "branch", br, "output", strings.TrimSpace(string(out)), "error", err)
			}
			continue
		}
		if ok, err := gitSuccess(gitRoot, "check-ref-format", ref); err != nil || !ok {
			if logger != nil {
				logger.Warn("execenv: invalid orphan publish-back quarantine ref; preserving branch", "branch", br, "ref", ref, "error", err)
			}
			continue
		}
		if out, err := exec.Command("git", "-C", gitRoot, "update-ref", ref, taskSHA).CombinedOutput(); err != nil {
			if logger != nil {
				logger.Warn("execenv: quarantine orphan publish-back branch failed; preserving branch", "branch", br, "ref", ref, "output", strings.TrimSpace(string(out)), "error", err)
			}
			continue
		}
		cmd := exec.Command("git", "-C", gitRoot, "branch", "-D", br)
		if out, err := cmd.CombinedOutput(); err != nil && logger != nil {
			logger.Warn("execenv: prune quarantined publish-back branch failed", "branch", br, "ref", ref, "output", strings.TrimSpace(string(out)), "error", err)
		}
	}
}

// liveWorktreePaths returns registered worktree directories that still exist.
// Publish-back tasks may detach HEAD or check out a different branch, so branch
// lines alone cannot prove that their worktree has gone away.
func liveWorktreePaths(repoRoot string) []string {
	out, err := exec.Command("git", "-C", repoRoot, "worktree", "list", "--porcelain").Output()
	if err != nil {
		return nil
	}
	var paths []string
	for _, line := range strings.Split(string(out), "\n") {
		path, ok := strings.CutPrefix(line, "worktree ")
		if !ok {
			continue
		}
		path = strings.TrimSpace(path)
		if info, statErr := os.Stat(path); statErr == nil && info.IsDir() {
			paths = append(paths, filepath.Clean(path))
		}
	}
	return paths
}

func publishBackWorktreePathLive(paths []string, taskID string) bool {
	taskRootName := shortID(taskID)
	for _, path := range paths {
		// Prepare's stable layout is <workspacesRoot>/<workspace>/<shortTaskID>/workdir.
		// A short-ID collision only delays cleanup, which is the safe direction.
		if filepath.Base(path) == "workdir" && filepath.Base(filepath.Dir(path)) == taskRootName {
			return true
		}
	}
	return false
}

// pruneWorktrees runs `git worktree prune` on repoRoot. Best-effort.
func pruneWorktrees(repoRoot string) {
	cmd := exec.Command("git", "-C", repoRoot, "worktree", "prune")
	_ = cmd.Run()
}

// liveWorktreeBranches returns the set of branch names currently checked out in
// a worktree of repoRoot (so pruning never deletes an in-use branch).
func liveWorktreeBranches(repoRoot string) map[string]bool {
	out, err := exec.Command("git", "-C", repoRoot, "worktree", "list", "--porcelain").Output()
	set := map[string]bool{}
	if err != nil {
		return set
	}
	for _, line := range strings.Split(string(out), "\n") {
		if b, ok := strings.CutPrefix(line, "branch refs/heads/"); ok {
			set[strings.TrimSpace(b)] = true
		}
	}
	return set
}

// listBranchesWithPrefix returns local branches under refs/heads/<prefix>.
func listBranchesWithPrefix(repoRoot, prefix string) []string {
	out, err := exec.Command("git", "-C", repoRoot, "for-each-ref", "--format=%(refname:short)", "refs/heads/"+prefix).Output()
	if err != nil {
		return nil
	}
	var branches []string
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if s := strings.TrimSpace(line); s != "" {
			branches = append(branches, s)
		}
	}
	return branches
}

// orDiscardLogger returns a non-nil logger so removeGitWorktree (which always
// logs) never nil-panics when the caller passed nil.
func orDiscardLogger(logger *slog.Logger) *slog.Logger {
	if logger != nil {
		return logger
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

// isolatedWorkDir is the path a task's isolated worktree lives at, under envRoot.
func isolatedWorkDir(envRoot string) string {
	return filepath.Join(envRoot, "workdir")
}
