package daemon

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/multica-ai/multica/server/internal/daemon/execenv"
)

type workflowIntegrationConflictError struct {
	cause error
}

type deterministicWorkflowContract struct {
	Operation      string   `json:"operation"`
	Command        []string `json:"command"`
	AllowedPaths   []string `json:"allowed_paths"`
	TimeoutSeconds int      `json:"timeout_seconds,omitempty"`
}

type deterministicWorkflowManifest struct {
	ExecutorKind   string `json:"executor_kind"`
	Operation      string `json:"operation"`
	NodeID         string `json:"node_id"`
	AttemptID      string `json:"attempt_id"`
	ClaimEpoch     int64  `json:"claim_epoch"`
	ContractDigest string `json:"contract_digest"`
}

func (e *workflowIntegrationConflictError) Error() string { return e.cause.Error() }
func (e *workflowIntegrationConflictError) Unwrap() error { return e.cause }

func (d *Daemon) handleWorkflowIntegration(ctx context.Context, task Task, logger *slog.Logger) {
	job := task.Integration
	if job == nil {
		return
	}
	logger = logger.With(
		"workflow_event", shortID(job.EventID),
		"workflow_node", job.NodeKey,
		"passage", job.PassageKey,
	)
	assignment, err := localDirectoryAssignmentForTask(task, d.cfg.DaemonID)
	if err != nil {
		d.reportWorkflowIntegrationFailure(ctx, job, err, false, logger)
		return
	}
	if assignment == nil || assignment.Ref.PublishBack != localDirectoryPublishSubmitBundle {
		d.reportWorkflowIntegrationFailure(
			ctx,
			job,
			errors.New("workflow integration requires a matching local_directory with publish_back=submit_bundle"),
			false,
			logger,
		)
		return
	}
	if err := validateLocalPath(assignment.AbsPath); err != nil {
		d.reportWorkflowIntegrationFailure(ctx, job, err, false, logger)
		return
	}
	mutexKey, err := assignment.mutexKey()
	if err != nil {
		d.reportWorkflowIntegrationFailure(ctx, job, err, false, logger)
		return
	}
	release, err := d.localPathLocks.Acquire(ctx, mutexKey, "integration:"+job.EventID, nil)
	if err != nil {
		d.reportWorkflowIntegrationFailure(ctx, job, err, true, logger)
		return
	}
	defer release()

	integrationCtx, cancelIntegration := context.WithCancel(ctx)
	defer cancelIntegration()
	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-integrationCtx.Done():
				return
			case <-ticker.C:
				if err := d.client.RenewWorkflowIntegrationLease(integrationCtx, job.EventID); err != nil {
					logger.Warn("workflow integration lease renewal failed", "error", err)
				}
			}
		}
	}()

	tempRoot, err := os.MkdirTemp(d.cfg.WorkspacesRoot, "workflow-integration-")
	if err != nil {
		d.reportWorkflowIntegrationFailure(ctx, job, err, true, logger)
		return
	}
	defer os.RemoveAll(tempRoot)
	var canonicalCommit string
	if job.EventType == "workflow.deterministic_ready" {
		canonicalCommit, err = integrateDeterministicWorkflowNode(
			integrationCtx,
			assignment.AbsPath,
			filepath.Join(tempRoot, "worktree"),
			job,
			logger,
		)
	} else {
		bundlePath := filepath.Join(tempRoot, job.AttemptID+".bundle")
		if err := d.client.DownloadWorkflowIntegrationBundle(
			integrationCtx,
			job.EventID,
			bundlePath,
			job.ArtifactDigest,
		); err != nil {
			d.reportWorkflowIntegrationFailure(ctx, job, err, true, logger)
			return
		}
		canonicalCommit, err = integrateWorkflowBundle(
			integrationCtx,
			assignment.AbsPath,
			bundlePath,
			filepath.Join(tempRoot, "worktree"),
			job,
			logger,
		)
	}
	if err != nil {
		var conflict *workflowIntegrationConflictError
		d.reportWorkflowIntegrationFailure(ctx, job, err, !errors.As(err, &conflict), logger)
		return
	}
	if err := d.client.CompleteWorkflowIntegration(integrationCtx, job.EventID, canonicalCommit); err != nil {
		// The canonical commit already carries a durable provenance trailer.
		// A later lease owner will detect it and replay acceptance without
		// applying the diff twice.
		logger.Error("workflow integration acceptance callback failed", "canonical_commit", canonicalCommit, "error", err)
		return
	}
	logger.Info("workflow integration accepted", "canonical_commit", canonicalCommit)
}

func integrateDeterministicWorkflowNode(
	ctx context.Context,
	sourceRepo, worktreePath string,
	job *WorkflowIntegrationJobData,
	logger *slog.Logger,
) (string, error) {
	if job == nil {
		return "", errors.New("deterministic workflow job is nil")
	}
	var contract deterministicWorkflowContract
	if err := json.Unmarshal(job.OutputContract, &contract); err != nil {
		return "", &workflowIntegrationConflictError{cause: fmt.Errorf("invalid deterministic workflow contract: %w", err)}
	}
	if contract.Operation != "repository_command_v1" || len(contract.Command) == 0 {
		return "", &workflowIntegrationConflictError{cause: errors.New("unsupported deterministic workflow operation")}
	}
	if len(contract.AllowedPaths) == 0 {
		return "", &workflowIntegrationConflictError{cause: errors.New("deterministic workflow contract has no allowed paths")}
	}
	for _, argument := range contract.Command {
		if strings.TrimSpace(argument) == "" {
			return "", &workflowIntegrationConflictError{cause: errors.New("deterministic workflow command contains an empty argument")}
		}
	}
	timeout := contract.TimeoutSeconds
	if timeout == 0 {
		timeout = 120
	}
	if timeout < 1 || timeout > 300 {
		return "", &workflowIntegrationConflictError{cause: errors.New("deterministic workflow timeout must be between 1 and 300 seconds")}
	}
	digest := fmt.Sprintf("sha256:%x", sha256.Sum256(job.OutputContract))
	if digest != job.ArtifactDigest {
		return "", &workflowIntegrationConflictError{cause: errors.New("deterministic workflow contract digest does not match its attempt")}
	}
	var manifest deterministicWorkflowManifest
	if err := json.Unmarshal(job.Manifest, &manifest); err != nil {
		return "", &workflowIntegrationConflictError{cause: fmt.Errorf("invalid deterministic workflow manifest: %w", err)}
	}
	if manifest.ExecutorKind != "deterministic" ||
		manifest.Operation != contract.Operation ||
		manifest.NodeID != job.NodeID ||
		manifest.AttemptID != job.AttemptID ||
		manifest.ClaimEpoch != job.ClaimEpoch ||
		manifest.ContractDigest != digest {
		return "", &workflowIntegrationConflictError{cause: errors.New("deterministic workflow manifest does not match integration job")}
	}
	trailer := fmt.Sprintf(
		"Multica-Workflow-Result: %s/%d/%s",
		job.NodeID,
		job.ClaimEpoch,
		job.ArtifactDigest,
	)
	if existing, err := integrationGitOutput(
		ctx,
		sourceRepo,
		"log",
		"--fixed-strings",
		"--grep="+trailer,
		"--format=%H",
		"-n",
		"1",
	); err == nil && existing != "" {
		if ok, _ := integrationGitSuccess(ctx, sourceRepo, "merge-base", "--is-ancestor", existing, "HEAD"); ok {
			logger.Info("deterministic workflow integration already present", "canonical_commit", existing)
			return existing, nil
		}
	}
	if status, err := integrationGitOutputBytes(ctx, sourceRepo, "status", "--porcelain=v1", "-z", "--untracked-files=all"); err != nil {
		return "", fmt.Errorf("inspect canonical repository: %w", err)
	} else if len(status) != 0 {
		return "", &workflowIntegrationConflictError{cause: errors.New("canonical repository is not clean")}
	}
	canonicalBase, err := integrationGitOutput(ctx, sourceRepo, "rev-parse", "--verify", "HEAD^{commit}")
	if err != nil {
		return "", fmt.Errorf("resolve canonical head: %w", err)
	}
	if out, err := integrationGitCombined(ctx, sourceRepo, "worktree", "add", "--detach", worktreePath, canonicalBase); err != nil {
		return "", fmt.Errorf("create deterministic integration worktree: %s: %w", out, err)
	}
	defer func() {
		_, _ = integrationGitCombined(context.Background(), sourceRepo, "worktree", "remove", "--force", worktreePath)
		_, _ = integrationGitCombined(context.Background(), sourceRepo, "worktree", "prune")
	}()
	commandCtx, cancel := context.WithTimeout(ctx, time.Duration(timeout)*time.Second)
	defer cancel()
	command := exec.CommandContext(commandCtx, contract.Command[0], contract.Command[1:]...)
	command.Dir = worktreePath
	output, err := command.CombinedOutput()
	if err != nil {
		if errors.Is(commandCtx.Err(), context.DeadlineExceeded) {
			return "", &workflowIntegrationConflictError{cause: fmt.Errorf("deterministic workflow command timed out after %d seconds", timeout)}
		}
		return "", &workflowIntegrationConflictError{cause: fmt.Errorf(
			"deterministic workflow command failed: %s: %w",
			strings.TrimSpace(string(output)),
			err,
		)}
	}
	if out, err := integrationGitCombined(ctx, worktreePath, "add", "--all"); err != nil {
		return "", fmt.Errorf("stage deterministic workflow result: %s: %w", out, err)
	}
	changedPaths, err := integrationGitNULPaths(ctx, worktreePath, "diff", "--cached", "--name-only", "--no-renames", "-z", "--")
	if err != nil {
		return "", fmt.Errorf("list deterministic workflow paths: %w", err)
	}
	if err := execenv.ValidateWorkflowChangedPaths(changedPaths, contract.AllowedPaths); err != nil {
		return "", &workflowIntegrationConflictError{cause: err}
	}
	message := fmt.Sprintf("chore(workflow): reduce %s %s", job.PassageKey, job.NodeKey)
	if out, err := integrationGitCombined(
		ctx,
		worktreePath,
		"-c",
		"user.name=Multica Workflow Integrator",
		"-c",
		"user.email=workflow@multica.local",
		"commit",
		"--allow-empty",
		"-m",
		message,
		"-m",
		trailer,
		"-m",
		"Multica-Workflow-Attempt: "+job.AttemptID,
	); err != nil {
		return "", &workflowIntegrationConflictError{cause: fmt.Errorf("commit deterministic workflow result: %s: %w", out, err)}
	}
	integrationCommit, err := integrationGitOutput(ctx, worktreePath, "rev-parse", "--verify", "HEAD^{commit}")
	if err != nil {
		return "", fmt.Errorf("resolve deterministic integration commit: %w", err)
	}
	if current, err := integrationGitOutput(ctx, sourceRepo, "rev-parse", "--verify", "HEAD^{commit}"); err != nil {
		return "", fmt.Errorf("recheck canonical head: %w", err)
	} else if current != canonicalBase {
		return "", &workflowIntegrationConflictError{cause: fmt.Errorf("canonical head moved from %s to %s during deterministic integration", canonicalBase, current)}
	}
	if out, err := integrationGitCombined(
		ctx,
		sourceRepo,
		"-c",
		"core.hooksPath=/dev/null",
		"merge",
		"--ff-only",
		"--no-edit",
		"--no-autostash",
		integrationCommit,
	); err != nil {
		return "", &workflowIntegrationConflictError{cause: fmt.Errorf("advance canonical repository: %s: %w", out, err)}
	}
	after, err := integrationGitOutput(ctx, sourceRepo, "rev-parse", "--verify", "HEAD^{commit}")
	if err != nil {
		return "", fmt.Errorf("verify deterministic integration commit: %w", err)
	}
	if after != integrationCommit {
		return "", fmt.Errorf("canonical repository ended at %s instead of deterministic integration commit %s", after, integrationCommit)
	}
	return integrationCommit, nil
}

func (d *Daemon) reportWorkflowIntegrationFailure(
	ctx context.Context,
	job *WorkflowIntegrationJobData,
	err error,
	retryable bool,
	logger *slog.Logger,
) {
	logger.Error("workflow integration failed", "retryable", retryable, "error", err)
	if reportErr := d.client.FailWorkflowIntegration(ctx, job.EventID, err.Error(), retryable); reportErr != nil {
		logger.Error("workflow integration failure callback failed", "error", reportErr)
	}
}

func integrateWorkflowBundle(
	ctx context.Context,
	sourceRepo, bundlePath, worktreePath string,
	job *WorkflowIntegrationJobData,
	logger *slog.Logger,
) (string, error) {
	if job == nil {
		return "", errors.New("workflow integration job is nil")
	}
	var manifest workflowBundleManifest
	if err := json.Unmarshal(job.Manifest, &manifest); err != nil {
		return "", &workflowIntegrationConflictError{cause: fmt.Errorf("invalid workflow bundle manifest: %w", err)}
	}
	if manifest.NodeID != job.NodeID ||
		manifest.AttemptID != job.AttemptID ||
		manifest.ClaimEpoch != job.ClaimEpoch ||
		manifest.BaseCommit != job.BaseCommit ||
		manifest.ResultCommit != job.ResultCommit ||
		manifest.BundleSHA256 != job.ArtifactDigest {
		return "", &workflowIntegrationConflictError{cause: errors.New("workflow bundle manifest does not match integration job")}
	}
	allowedPaths, err := workflowAllowedPaths(job.OutputContract)
	if err != nil {
		return "", &workflowIntegrationConflictError{cause: err}
	}
	if err := execenv.ValidateWorkflowChangedPaths(manifest.ChangedPaths, allowedPaths); err != nil {
		return "", &workflowIntegrationConflictError{cause: err}
	}

	trailer := fmt.Sprintf(
		"Multica-Workflow-Result: %s/%d/%s",
		job.NodeID,
		job.ClaimEpoch,
		job.ArtifactDigest,
	)
	if existing, err := integrationGitOutput(
		ctx,
		sourceRepo,
		"log",
		"--fixed-strings",
		"--grep="+trailer,
		"--format=%H",
		"-n",
		"1",
	); err == nil && existing != "" {
		if ok, _ := integrationGitSuccess(ctx, sourceRepo, "merge-base", "--is-ancestor", existing, "HEAD"); ok {
			logger.Info("workflow integration already present", "canonical_commit", existing)
			return existing, nil
		}
	}

	if status, err := integrationGitOutputBytes(ctx, sourceRepo, "status", "--porcelain=v1", "-z", "--untracked-files=all"); err != nil {
		return "", fmt.Errorf("inspect canonical repository: %w", err)
	} else if len(status) != 0 {
		return "", &workflowIntegrationConflictError{cause: errors.New("canonical repository is not clean")}
	}
	canonicalBase, err := integrationGitOutput(ctx, sourceRepo, "rev-parse", "--verify", "HEAD^{commit}")
	if err != nil {
		return "", fmt.Errorf("resolve canonical head: %w", err)
	}
	if out, err := integrationGitCombined(ctx, sourceRepo, "bundle", "verify", bundlePath); err != nil {
		return "", &workflowIntegrationConflictError{cause: fmt.Errorf("verify Git bundle: %s: %w", out, err)}
	}
	if out, err := integrationGitCombined(ctx, sourceRepo, "worktree", "add", "--detach", worktreePath, canonicalBase); err != nil {
		return "", fmt.Errorf("create integration worktree: %s: %w", out, err)
	}
	defer func() {
		_, _ = integrationGitCombined(context.Background(), sourceRepo, "worktree", "remove", "--force", worktreePath)
		_, _ = integrationGitCombined(context.Background(), sourceRepo, "worktree", "prune")
	}()

	if out, err := integrationGitCombined(ctx, worktreePath, "fetch", "--no-tags", bundlePath, job.ResultCommit); err != nil {
		return "", &workflowIntegrationConflictError{cause: fmt.Errorf("fetch workflow result commit: %s: %w", out, err)}
	}
	fetched, err := integrationGitOutput(ctx, worktreePath, "rev-parse", "--verify", "FETCH_HEAD^{commit}")
	if err != nil {
		return "", &workflowIntegrationConflictError{cause: fmt.Errorf("resolve fetched workflow result: %w", err)}
	}
	if fetched != job.ResultCommit {
		return "", &workflowIntegrationConflictError{cause: fmt.Errorf("fetched result %s does not match manifest %s", fetched, job.ResultCommit)}
	}
	if ok, err := integrationGitSuccess(ctx, worktreePath, "merge-base", "--is-ancestor", job.BaseCommit, fetched); err != nil || !ok {
		return "", &workflowIntegrationConflictError{cause: errors.New("workflow bundle result is not descended from its declared base")}
	}
	actualChanged, err := integrationGitNULPaths(ctx, worktreePath, "diff", "--name-only", "--no-renames", "-z", job.BaseCommit, fetched, "--")
	if err != nil {
		return "", &workflowIntegrationConflictError{cause: fmt.Errorf("list workflow bundle paths: %w", err)}
	}
	expectedChanged := append([]string(nil), manifest.ChangedPaths...)
	sort.Strings(expectedChanged)
	if strings.Join(actualChanged, "\x00") != strings.Join(expectedChanged, "\x00") {
		return "", &workflowIntegrationConflictError{cause: errors.New("workflow bundle changed paths do not match its manifest")}
	}
	if err := execenv.ValidateWorkflowChangedPaths(actualChanged, allowedPaths); err != nil {
		return "", &workflowIntegrationConflictError{cause: err}
	}
	patch, err := integrationGitOutputBytes(ctx, worktreePath, "diff", "--binary", "--full-index", job.BaseCommit, fetched, "--")
	if err != nil {
		return "", &workflowIntegrationConflictError{cause: fmt.Errorf("build workflow patch: %w", err)}
	}
	apply := exec.CommandContext(ctx, "git", "-C", worktreePath, "apply", "--index", "--3way", "--whitespace=nowarn", "-")
	apply.Stdin = bytes.NewReader(patch)
	if output, err := apply.CombinedOutput(); err != nil {
		return "", &workflowIntegrationConflictError{cause: fmt.Errorf("apply workflow patch: %s: %w", strings.TrimSpace(string(output)), err)}
	}
	message := fmt.Sprintf("chore(workflow): integrate %s %s", job.PassageKey, job.NodeKey)
	if out, err := integrationGitCombined(
		ctx,
		worktreePath,
		"-c",
		"user.name=Multica Workflow Integrator",
		"-c",
		"user.email=workflow@multica.local",
		"commit",
		"-m",
		message,
		"-m",
		trailer,
		"-m",
		"Multica-Workflow-Attempt: "+job.AttemptID,
	); err != nil {
		return "", &workflowIntegrationConflictError{cause: fmt.Errorf("commit integrated workflow result: %s: %w", out, err)}
	}
	integrationCommit, err := integrationGitOutput(ctx, worktreePath, "rev-parse", "--verify", "HEAD^{commit}")
	if err != nil {
		return "", fmt.Errorf("resolve integration commit: %w", err)
	}
	if current, err := integrationGitOutput(ctx, sourceRepo, "rev-parse", "--verify", "HEAD^{commit}"); err != nil {
		return "", fmt.Errorf("recheck canonical head: %w", err)
	} else if current != canonicalBase {
		return "", &workflowIntegrationConflictError{cause: fmt.Errorf("canonical head moved from %s to %s during integration", canonicalBase, current)}
	}
	if out, err := integrationGitCombined(
		ctx,
		sourceRepo,
		"-c",
		"core.hooksPath=/dev/null",
		"merge",
		"--ff-only",
		"--no-edit",
		"--no-autostash",
		integrationCommit,
	); err != nil {
		return "", &workflowIntegrationConflictError{cause: fmt.Errorf("advance canonical repository: %s: %w", out, err)}
	}
	after, err := integrationGitOutput(ctx, sourceRepo, "rev-parse", "--verify", "HEAD^{commit}")
	if err != nil {
		return "", fmt.Errorf("verify canonical integration commit: %w", err)
	}
	if after != integrationCommit {
		return "", fmt.Errorf("canonical repository ended at %s instead of integration commit %s", after, integrationCommit)
	}
	return integrationCommit, nil
}

func integrationGitCombined(ctx context.Context, dir string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "git", append([]string{"-C", dir}, args...)...)
	out, err := cmd.CombinedOutput()
	return strings.TrimSpace(string(out)), err
}

func integrationGitOutput(ctx context.Context, dir string, args ...string) (string, error) {
	out, err := integrationGitOutputBytes(ctx, dir, args...)
	return strings.TrimSpace(string(out)), err
}

func integrationGitOutputBytes(ctx context.Context, dir string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "git", append([]string{"-C", dir}, args...)...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("git %s: %s: %w", strings.Join(args, " "), strings.TrimSpace(stderr.String()), err)
	}
	return out, nil
}

func integrationGitSuccess(ctx context.Context, dir string, args ...string) (bool, error) {
	cmd := exec.CommandContext(ctx, "git", append([]string{"-C", dir}, args...)...)
	err := cmd.Run()
	if err == nil {
		return true, nil
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) && exitErr.ExitCode() == 1 {
		return false, nil
	}
	return false, err
}

func integrationGitNULPaths(ctx context.Context, dir string, args ...string) ([]string, error) {
	out, err := integrationGitOutputBytes(ctx, dir, args...)
	if err != nil {
		return nil, err
	}
	parts := bytes.Split(out, []byte{0})
	paths := make([]string, 0, len(parts))
	for _, part := range parts {
		if len(part) > 0 {
			paths = append(paths, string(part))
		}
	}
	sort.Strings(paths)
	return paths, nil
}
