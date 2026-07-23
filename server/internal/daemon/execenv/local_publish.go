package execenv

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"sort"
	"strings"
)

// QuarantineRefPrefix is the durable recovery namespace for task commits that
// were not safely published to a local_directory source checkout.
const QuarantineRefPrefix = "refs/multica/quarantine/"

const MaxWorkflowBundleBytes int64 = 100 << 20

type WorkflowBundleArtifact struct {
	Path         string
	BaseCommit   string
	ResultCommit string
	SHA256       string
	Size         int64
	ChangedPaths []string
}

type publishAppliedError struct {
	cause error
}

func (e *publishAppliedError) Error() string { return e.cause.Error() }
func (e *publishAppliedError) Unwrap() error { return e.cause }

// PublishBackApplied reports whether the exact fast-forward completed before a
// post-publish verification failure. Callers must not describe this state as a
// refused publication because the source checkout has already advanced.
func PublishBackApplied(err error) bool {
	var applied *publishAppliedError
	return errors.As(err, &applied)
}

// FinalizeSerialPublishBack completes the lifecycle of an isolated
// local_directory worktree that opted into publish_back=serial_ff.
//
// A true publish request is still fail closed: the source checkout advances
// only when the task worktree is clean after daemon-sidecar cleanup, its tip is
// a descendant of the immutable BaseCommit, source HEAD still equals that
// base, the source index is clean, and source dirt does not overlap task
// changes. The update is an exact --ff-only merge to the task SHA. No rebase,
// auto-commit, stash, reset, or clean is permitted.
//
// Every non-publish disposition and every publish conflict first records a task
// tip that differs from BaseCommit at refs/multica/quarantine/<task-id>.
// BaseCommit-only runs need no recovery ref because their tip remains reachable
// from the source history. Cleanup occurs only after the needed ref exists (or
// after a successful publish). If quarantine creation fails, the worktree and
// branch remain intact for manual recovery.
func (w *IsolatedLocalWorktree) FinalizeSerialPublishBack(publish bool, provider string, logger *slog.Logger) error {
	if w == nil {
		return nil
	}
	if w.SourceRepo == "" || w.WorkDir == "" || w.TaskID == "" || w.BaseCommit == "" {
		return errors.New("execenv: publish_back worktree metadata is incomplete")
	}

	lock := lockForRepo(w.SourceRepo)
	lock.Lock()
	defer lock.Unlock()

	taskSHA, err := gitOutput(w.WorkDir, "rev-parse", "--verify", "HEAD^{commit}")
	if err != nil {
		// There is no verified commit to quarantine. Preserve the entire worktree
		// and branch instead of destroying the only remaining recovery surface.
		return fmt.Errorf("execenv: publish_back resolve task commit: %w", err)
	}

	if !publish {
		if err := w.quarantineAndRemoveLocked(taskSHA, logger); err != nil {
			return err
		}
		return nil
	}

	if err := w.publishSerialFastForwardLocked(taskSHA, provider); err != nil {
		if quarantineErr := w.quarantineAndRemoveLocked(taskSHA, logger); quarantineErr != nil {
			return fmt.Errorf("%w; additionally failed to quarantine task commit: %v", err, quarantineErr)
		}
		return err
	}

	w.removeLocked(logger)
	return nil
}

func (w *IsolatedLocalWorktree) BuildWorkflowBundle(provider, outputDir string, allowedPaths []string) (WorkflowBundleArtifact, error) {
	if w == nil || w.SourceRepo == "" || w.WorkDir == "" || w.TaskID == "" || w.BaseCommit == "" {
		return WorkflowBundleArtifact{}, errors.New("execenv: workflow bundle worktree metadata is incomplete")
	}
	taskSHA, err := gitOutput(w.WorkDir, "rev-parse", "--verify", "HEAD^{commit}")
	if err != nil {
		return WorkflowBundleArtifact{}, fmt.Errorf("execenv: workflow bundle resolve result commit: %w", err)
	}
	if taskSHA == w.BaseCommit {
		return WorkflowBundleArtifact{}, errors.New("execenv: workflow bundle requires a result commit after the base commit")
	}
	bundleRef := "refs/heads/" + w.Branch
	branchSHA, err := gitOutput(w.WorkDir, "rev-parse", "--verify", bundleRef+"^{commit}")
	if err != nil {
		return WorkflowBundleArtifact{}, fmt.Errorf("execenv: workflow bundle resolve task branch: %w", err)
	}
	if branchSHA != taskSHA {
		return WorkflowBundleArtifact{}, fmt.Errorf(
			"execenv: workflow result commit %s does not match owned task branch %s at %s",
			taskSHA,
			bundleRef,
			branchSHA,
		)
	}
	ancestor, err := gitSuccess(w.WorkDir, "merge-base", "--is-ancestor", w.BaseCommit, taskSHA)
	if err != nil {
		return WorkflowBundleArtifact{}, fmt.Errorf("execenv: workflow bundle verify ancestry: %w", err)
	}
	if !ancestor {
		return WorkflowBundleArtifact{}, fmt.Errorf("execenv: workflow result commit %s is not a descendant of base %s", taskSHA, w.BaseCommit)
	}
	changed, err := nulPathSet(w.SourceRepo, "diff", "--name-only", "--no-renames", "-z", w.BaseCommit, taskSHA, "--")
	if err != nil {
		return WorkflowBundleArtifact{}, fmt.Errorf("execenv: workflow bundle list changed paths: %w", err)
	}
	if len(changed) == 0 {
		return WorkflowBundleArtifact{}, errors.New("execenv: workflow bundle result has no changed paths")
	}
	if taskPath, managedPath, overlap := firstPathOverlap(changed, w.managedPaths); overlap {
		return WorkflowBundleArtifact{}, fmt.Errorf("execenv: workflow bundle refused because committed task path %q is daemon-managed runtime material %q", taskPath, managedPath)
	}
	normalizedAllowed, err := normalizeWorkflowAllowedPaths(allowedPaths)
	if err != nil {
		return WorkflowBundleArtifact{}, err
	}
	changedPaths := make([]string, 0, len(changed))
	for changedPath := range changed {
		if !workflowPathAllowed(changedPath, normalizedAllowed) {
			return WorkflowBundleArtifact{}, fmt.Errorf("execenv: workflow bundle changed path %q outside the declared write set", changedPath)
		}
		changedPaths = append(changedPaths, changedPath)
	}
	sort.Strings(changedPaths)

	if err := CleanupRuntimeConfig(w.WorkDir, provider); err != nil {
		return WorkflowBundleArtifact{}, fmt.Errorf("execenv: workflow bundle cleanup runtime config: %w", err)
	}
	if err := CleanupSidecars(filepath.Dir(w.WorkDir)); err != nil {
		return WorkflowBundleArtifact{}, fmt.Errorf("execenv: workflow bundle cleanup sidecars: %w", err)
	}
	if status, err := gitOutputBytes(w.WorkDir, "status", "--porcelain=v1", "-z", "--untracked-files=all"); err != nil {
		return WorkflowBundleArtifact{}, fmt.Errorf("execenv: workflow bundle inspect worktree: %w", err)
	} else if len(status) != 0 {
		return WorkflowBundleArtifact{}, errors.New("execenv: workflow bundle refused because the task worktree is not clean after sidecar cleanup")
	}

	if err := os.MkdirAll(outputDir, 0o700); err != nil {
		return WorkflowBundleArtifact{}, fmt.Errorf("execenv: create workflow bundle output directory: %w", err)
	}
	file, err := os.CreateTemp(outputDir, "workflow-*.bundle")
	if err != nil {
		return WorkflowBundleArtifact{}, fmt.Errorf("execenv: create workflow bundle: %w", err)
	}
	bundlePath := file.Name()
	if err := file.Close(); err != nil {
		os.Remove(bundlePath)
		return WorkflowBundleArtifact{}, fmt.Errorf("execenv: close workflow bundle placeholder: %w", err)
	}
	if out, err := exec.Command("git", "-C", w.WorkDir, "bundle", "create", bundlePath, bundleRef, "^"+w.BaseCommit).CombinedOutput(); err != nil {
		os.Remove(bundlePath)
		return WorkflowBundleArtifact{}, fmt.Errorf("execenv: create workflow git bundle: %s: %w", strings.TrimSpace(string(out)), err)
	}
	if out, err := exec.Command("git", "-C", w.WorkDir, "bundle", "verify", bundlePath).CombinedOutput(); err != nil {
		os.Remove(bundlePath)
		return WorkflowBundleArtifact{}, fmt.Errorf("execenv: verify workflow git bundle: %s: %w", strings.TrimSpace(string(out)), err)
	}
	info, err := os.Stat(bundlePath)
	if err != nil {
		os.Remove(bundlePath)
		return WorkflowBundleArtifact{}, fmt.Errorf("execenv: stat workflow git bundle: %w", err)
	}
	if info.Size() < 1 || info.Size() > MaxWorkflowBundleBytes {
		os.Remove(bundlePath)
		return WorkflowBundleArtifact{}, fmt.Errorf("execenv: workflow bundle size %d exceeds limit %d", info.Size(), MaxWorkflowBundleBytes)
	}
	bundleFile, err := os.Open(bundlePath)
	if err != nil {
		os.Remove(bundlePath)
		return WorkflowBundleArtifact{}, fmt.Errorf("execenv: open workflow bundle for digest: %w", err)
	}
	hasher := sha256.New()
	if _, err := io.Copy(hasher, bundleFile); err != nil {
		bundleFile.Close()
		os.Remove(bundlePath)
		return WorkflowBundleArtifact{}, fmt.Errorf("execenv: hash workflow bundle: %w", err)
	}
	if err := bundleFile.Close(); err != nil {
		os.Remove(bundlePath)
		return WorkflowBundleArtifact{}, fmt.Errorf("execenv: close workflow bundle: %w", err)
	}
	return WorkflowBundleArtifact{
		Path:         bundlePath,
		BaseCommit:   w.BaseCommit,
		ResultCommit: taskSHA,
		SHA256:       hex.EncodeToString(hasher.Sum(nil)),
		Size:         info.Size(),
		ChangedPaths: changedPaths,
	}, nil
}

func normalizeWorkflowAllowedPaths(paths []string) ([]string, error) {
	if len(paths) == 0 {
		return nil, errors.New("execenv: workflow output contract has an empty write set")
	}
	out := make([]string, 0, len(paths))
	for _, raw := range paths {
		value := filepath.ToSlash(strings.TrimSpace(raw))
		value = strings.TrimPrefix(value, "./")
		if value == "" || strings.HasPrefix(value, "/") || value == ".." || strings.HasPrefix(value, "../") {
			return nil, fmt.Errorf("execenv: invalid workflow write-set path %q", raw)
		}
		out = append(out, value)
	}
	return out, nil
}

func workflowPathAllowed(changedPath string, allowedPaths []string) bool {
	changedPath = filepath.ToSlash(changedPath)
	for _, allowed := range allowedPaths {
		if changedPath == allowed {
			return true
		}
		if strings.HasSuffix(allowed, "/**") {
			prefix := strings.TrimSuffix(allowed, "**")
			if strings.HasPrefix(changedPath, prefix) {
				return true
			}
		}
		if strings.HasSuffix(allowed, "/") && strings.HasPrefix(changedPath, allowed) {
			return true
		}
		if matched, err := path.Match(allowed, changedPath); err == nil && matched {
			return true
		}
	}
	return false
}

func ValidateWorkflowChangedPaths(changedPaths, allowedPaths []string) error {
	normalized, err := normalizeWorkflowAllowedPaths(allowedPaths)
	if err != nil {
		return err
	}
	for _, changedPath := range changedPaths {
		if !workflowPathAllowed(changedPath, normalized) {
			return fmt.Errorf("execenv: workflow changed path %q outside the declared write set", changedPath)
		}
	}
	return nil
}

func (w *IsolatedLocalWorktree) FinalizeWorkflowBundle(submitted bool, logger *slog.Logger) error {
	if w == nil {
		return nil
	}
	lock := lockForRepo(w.SourceRepo)
	lock.Lock()
	defer lock.Unlock()
	taskSHA, err := gitOutput(w.WorkDir, "rev-parse", "--verify", "HEAD^{commit}")
	if err != nil {
		return fmt.Errorf("execenv: resolve workflow bundle task commit: %w", err)
	}
	if submitted {
		w.removeLocked(logger)
		return nil
	}
	return w.quarantineAndRemoveLocked(taskSHA, logger)
}

func (w *IsolatedLocalWorktree) publishSerialFastForwardLocked(taskSHA, provider string) error {
	ancestor, err := gitSuccess(w.WorkDir, "merge-base", "--is-ancestor", w.BaseCommit, taskSHA)
	if err != nil {
		return fmt.Errorf("execenv: publish_back verify task ancestry: %w", err)
	}
	if !ancestor {
		return fmt.Errorf("execenv: publish_back task commit %s is not a descendant of base %s", taskSHA, w.BaseCommit)
	}
	taskPaths, err := nulPathSet(w.SourceRepo, "diff", "--name-only", "--no-renames", "-z", w.BaseCommit, taskSHA, "--")
	if err != nil {
		return fmt.Errorf("execenv: publish_back list task paths: %w", err)
	}
	if taskPath, managedPath, overlap := firstPathOverlap(taskPaths, w.managedPaths); overlap {
		return fmt.Errorf("execenv: publish_back refused because committed task path %q is daemon-managed runtime material %q", taskPath, managedPath)
	}

	// Remove only daemon-owned runtime material before judging the task tree.
	// If the agent committed any of those files, cleanup makes the worktree
	// dirty relative to taskSHA and the clean-tree gate below rejects publish.
	if err := CleanupRuntimeConfig(w.WorkDir, provider); err != nil {
		return fmt.Errorf("execenv: publish_back cleanup runtime config: %w", err)
	}
	if err := CleanupSidecars(filepath.Dir(w.WorkDir)); err != nil {
		return fmt.Errorf("execenv: publish_back cleanup sidecars: %w", err)
	}
	if status, err := gitOutputBytes(w.WorkDir, "status", "--porcelain=v1", "-z", "--untracked-files=all"); err != nil {
		return fmt.Errorf("execenv: publish_back inspect task worktree: %w", err)
	} else if len(status) != 0 {
		return errors.New("execenv: publish_back refused because the task worktree has staged or uncommitted changes after sidecar cleanup")
	}

	sourceHEAD, err := gitOutput(w.SourceRepo, "rev-parse", "--verify", "HEAD^{commit}")
	if err != nil {
		return fmt.Errorf("execenv: publish_back resolve source HEAD: %w", err)
	}
	if sourceHEAD != w.BaseCommit {
		return fmt.Errorf("execenv: publish_back refused because source HEAD moved from %s to %s", w.BaseCommit, sourceHEAD)
	}

	indexClean, err := gitSuccess(w.SourceRepo, "diff", "--cached", "--quiet", "--")
	if err != nil {
		return fmt.Errorf("execenv: publish_back inspect source index: %w", err)
	}
	if !indexClean {
		return errors.New("execenv: publish_back refused because the source checkout has staged changes")
	}

	sourceDirty, err := sourceDirtyPaths(w.SourceRepo)
	if err != nil {
		return fmt.Errorf("execenv: publish_back inspect source dirt: %w", err)
	}
	sourceIgnored, err := ignoredTaskPathCollisions(w.SourceRepo, taskPaths)
	if err != nil {
		return fmt.Errorf("execenv: publish_back inspect ignored source paths: %w", err)
	}
	if taskPath, sourcePath, overlap := firstPathOverlap(taskPaths, sourceDirty); overlap {
		return fmt.Errorf("execenv: publish_back refused because task path %q overlaps dirty source path %q", taskPath, sourcePath)
	}
	if taskPath, sourcePath, overlap := firstPathOverlap(taskPaths, sourceIgnored); overlap {
		return fmt.Errorf("execenv: publish_back refused because task path %q overlaps ignored source path %q", taskPath, sourcePath)
	}
	before, err := fingerprintPaths(w.SourceRepo, sourceDirty)
	if err != nil {
		return fmt.Errorf("execenv: publish_back fingerprint source dirt: %w", err)
	}

	cmd := exec.Command("git", "-C", w.SourceRepo,
		"-c", "core.hooksPath=/dev/null",
		"merge", "--ff-only", "--no-edit", "--no-autostash", taskSHA,
	)
	if out, mergeErr := cmd.CombinedOutput(); mergeErr != nil {
		return fmt.Errorf("execenv: publish_back fast-forward failed: %s: %w", strings.TrimSpace(string(out)), mergeErr)
	}

	afterHEAD, err := gitOutput(w.SourceRepo, "rev-parse", "--verify", "HEAD^{commit}")
	if err != nil {
		return &publishAppliedError{cause: fmt.Errorf("execenv: publish_back advanced source but could not verify source HEAD: %w", err)}
	}
	if afterHEAD != taskSHA {
		return &publishAppliedError{cause: fmt.Errorf("execenv: publish_back advanced source but source HEAD %s does not equal task commit %s", afterHEAD, taskSHA)}
	}
	after, err := fingerprintPaths(w.SourceRepo, sourceDirty)
	if err != nil {
		return &publishAppliedError{cause: fmt.Errorf("execenv: publish_back advanced source but could not re-fingerprint source dirt: %w", err)}
	}
	for path, want := range before {
		if got := after[path]; got != want {
			return &publishAppliedError{cause: fmt.Errorf("execenv: publish_back advanced source but changed unrelated dirty source path %q", path)}
		}
	}
	return nil
}

func (w *IsolatedLocalWorktree) quarantineAndRemoveLocked(taskSHA string, logger *slog.Logger) error {
	reachable, err := gitSuccess(w.SourceRepo, "merge-base", "--is-ancestor", taskSHA, "HEAD")
	if err != nil {
		return fmt.Errorf("execenv: inspect task commit reachability before quarantine: %w", err)
	}
	if reachable {
		if logger != nil {
			logger.Info("execenv: isolated local worktree has no unique task commit to quarantine", "task_id", w.TaskID, "task_commit", taskSHA)
		}
		w.removeLocked(logger)
		return nil
	}
	ref := QuarantineRefPrefix + w.TaskID
	if ok, err := gitSuccess(w.SourceRepo, "check-ref-format", ref); err != nil {
		return fmt.Errorf("execenv: validate quarantine ref %q: %w", ref, err)
	} else if !ok {
		return fmt.Errorf("execenv: invalid quarantine ref %q", ref)
	}
	if out, err := exec.Command("git", "-C", w.SourceRepo, "update-ref", ref, taskSHA).CombinedOutput(); err != nil {
		return fmt.Errorf("execenv: create quarantine ref %q: %s: %w", ref, strings.TrimSpace(string(out)), err)
	}
	if logger != nil {
		logger.Info("execenv: quarantined isolated local worktree commit", "task_id", w.TaskID, "commit", taskSHA, "ref", ref)
	}
	w.removeLocked(logger)
	return nil
}

func (w *IsolatedLocalWorktree) removeLocked(logger *slog.Logger) {
	removeGitWorktree(w.SourceRepo, w.WorkDir, w.Branch, orDiscardLogger(logger))
	pruneWorktrees(w.SourceRepo)
}

func gitOutput(dir string, args ...string) (string, error) {
	out, err := gitOutputBytes(dir, args...)
	return strings.TrimSpace(string(out)), err
}

func gitOutputBytes(dir string, args ...string) ([]byte, error) {
	cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("git %s: %s: %w", strings.Join(args, " "), strings.TrimSpace(stderr.String()), err)
	}
	return out, nil
}

// gitSuccess distinguishes a normal predicate false (exit 1) from a command
// failure (exit >1), which is needed for git diff --quiet and merge-base.
func gitSuccess(dir string, args ...string) (bool, error) {
	cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
	out, err := cmd.CombinedOutput()
	if err == nil {
		return true, nil
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) && exitErr.ExitCode() == 1 {
		return false, nil
	}
	return false, fmt.Errorf("git %s: %s: %w", strings.Join(args, " "), strings.TrimSpace(string(out)), err)
}

func nulPathSet(dir string, args ...string) (map[string]struct{}, error) {
	out, err := gitOutputBytes(dir, args...)
	if err != nil {
		return nil, err
	}
	paths := make(map[string]struct{})
	for _, raw := range strings.Split(string(out), "\x00") {
		if raw != "" {
			paths[filepath.ToSlash(raw)] = struct{}{}
		}
	}
	return paths, nil
}

func sourceDirtyPaths(repo string) (map[string]struct{}, error) {
	tracked, err := nulPathSet(repo, "diff", "--name-only", "--no-renames", "-z", "--")
	if err != nil {
		return nil, err
	}
	untracked, err := nulPathSet(repo, "ls-files", "--others", "--exclude-standard", "-z", "--")
	if err != nil {
		return nil, err
	}
	for path := range untracked {
		tracked[path] = struct{}{}
	}
	return tracked, nil
}

func ignoredTaskPathCollisions(repo string, taskPaths map[string]struct{}) (map[string]struct{}, error) {
	candidates := make(map[string]struct{})
	for taskPath := range taskPaths {
		candidate := taskPath
		for candidate != "." && candidate != "" {
			full := filepath.Join(repo, filepath.FromSlash(candidate))
			if _, err := os.Lstat(full); err == nil {
				candidates[candidate] = struct{}{}
			} else if !errors.Is(err, fs.ErrNotExist) {
				return nil, fmt.Errorf("lstat %s: %w", candidate, err)
			}
			parent := pathParent(candidate)
			if parent == candidate {
				break
			}
			candidate = parent
		}
	}
	if len(candidates) == 0 {
		return map[string]struct{}{}, nil
	}
	var stdin bytes.Buffer
	for candidate := range candidates {
		stdin.WriteString(candidate)
		stdin.WriteByte(0)
	}
	cmd := exec.Command("git", "-C", repo, "check-ignore", "--stdin", "-z", "--no-index")
	cmd.Stdin = &stdin
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) && exitErr.ExitCode() == 1 {
			return map[string]struct{}{}, nil
		}
		return nil, fmt.Errorf("git check-ignore --stdin: %s: %w", strings.TrimSpace(stderr.String()), err)
	}
	result := make(map[string]struct{})
	for _, raw := range strings.Split(string(out), "\x00") {
		if raw != "" {
			result[filepath.ToSlash(raw)] = struct{}{}
		}
	}
	return result, nil
}

func firstPathOverlap(a, b map[string]struct{}) (string, string, bool) {
	// Index every ancestor represented by b once. A lookup then covers both
	// directions: left below right, and right below left, without a quadratic
	// scan across every pair of paths.
	bDescendant := make(map[string]string, len(b))
	for right := range b {
		candidate := right
		for candidate != "." && candidate != "" {
			if _, exists := bDescendant[candidate]; !exists {
				bDescendant[candidate] = right
			}
			parent := pathParent(candidate)
			if parent == candidate {
				break
			}
			candidate = parent
		}
	}
	for left := range a {
		candidate := left
		for candidate != "." && candidate != "" {
			if _, exists := b[candidate]; exists {
				return left, candidate, true
			}
			parent := pathParent(candidate)
			if parent == candidate {
				break
			}
			candidate = parent
		}
		if right, exists := bDescendant[left]; exists {
			return left, right, true
		}
	}
	return "", "", false
}

func pathParent(path string) string {
	return filepath.ToSlash(filepath.Dir(filepath.FromSlash(path)))
}

type fileFingerprint struct {
	Exists bool
	Mode   fs.FileMode
	Size   int64
	Digest [sha256.Size]byte
	Link   string
}

func fingerprintPaths(repo string, paths map[string]struct{}) (map[string]fileFingerprint, error) {
	result := make(map[string]fileFingerprint, len(paths))
	for path := range paths {
		full := filepath.Join(repo, filepath.FromSlash(path))
		info, err := os.Lstat(full)
		if errors.Is(err, fs.ErrNotExist) {
			result[path] = fileFingerprint{}
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("lstat %s: %w", path, err)
		}
		fp := fileFingerprint{Exists: true, Mode: info.Mode(), Size: info.Size()}
		switch {
		case info.Mode()&os.ModeSymlink != 0:
			fp.Link, err = os.Readlink(full)
		case info.Mode().IsRegular():
			var f *os.File
			f, err = os.Open(full)
			if err == nil {
				h := sha256.New()
				_, err = io.Copy(h, f)
				closeErr := f.Close()
				if err == nil {
					err = closeErr
				}
				copy(fp.Digest[:], h.Sum(nil))
			}
		}
		if err != nil {
			return nil, fmt.Errorf("fingerprint %s: %w", path, err)
		}
		result[path] = fp
	}
	return result, nil
}
