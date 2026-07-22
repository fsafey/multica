package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/multica-ai/multica/server/internal/daemon/execenv"
)

func lifecycleGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
	cmd.Env = append(os.Environ(),
		"GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@t",
		"GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@t",
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %s: %v", strings.Join(args, " "), strings.TrimSpace(string(out)), err)
	}
	return strings.TrimSpace(string(out))
}

func newLifecycleSourceRepo(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	lifecycleGit(t, dir, "init", "-q", "-b", "main")
	lifecycleGit(t, dir, "config", "user.name", "t")
	lifecycleGit(t, dir, "config", "user.email", "t@t")
	if err := os.WriteFile(filepath.Join(dir, "seed.txt"), []byte("seed\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	lifecycleGit(t, dir, "add", "seed.txt")
	lifecycleGit(t, dir, "commit", "-qm", "init")
	return dir
}

func TestTaskPrepareTimeoutResultPreservesPublishBackDisposition(t *testing.T) {
	wt := &execenv.IsolatedLocalWorktree{TaskID: "task-timeout"}
	got := taskPrepareTimeoutResult(TaskResult{
		Status:              "completed",
		Comment:             "must be discarded",
		PublishBackWorktree: wt,
		PublishBackProvider: "claude",
	})
	if got.PublishBackWorktree != wt || got.PublishBackProvider != "claude" {
		t.Fatalf("publish-back disposition was lost: %#v", got)
	}
	if got.Status != "" || got.Comment != "" {
		t.Fatalf("provider result leaked through prepare timeout: %#v", got)
	}
}

func TestHandleTask_SerialPublishBackFinalizesBeforeTerminalReportAndMutexRelease(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		resultStatus          string
		serverStatus          string
		serverStatusHTTP      int
		wantPublished         bool
		wantEndpoint          string
		wantUnconfirmedStatus bool
	}{
		{name: "completed publishes", resultStatus: "completed", serverStatus: "running", wantPublished: true, wantEndpoint: "/complete"},
		{name: "poisoned or blocked quarantines", resultStatus: "blocked", serverStatus: "running", wantPublished: false, wantEndpoint: "/fail"},
		{name: "unconfirmed final status quarantines", resultStatus: "completed", serverStatus: "dispatched", wantPublished: false, wantEndpoint: "/fail", wantUnconfirmedStatus: true},
		{name: "failed final status read quarantines", resultStatus: "completed", serverStatusHTTP: http.StatusServiceUnavailable, wantPublished: false, wantEndpoint: "/fail", wantUnconfirmedStatus: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src := newLifecycleSourceRepo(t)
			realSrc, err := filepath.EvalSymlinks(src)
			if err != nil {
				t.Fatal(err)
			}
			base := lifecycleGit(t, src, "rev-parse", "HEAD")
			const taskID = "task-lifecycle-1111"
			wt, err := execenv.PrepareIsolatedLocalWorktree(src, filepath.Join(t.TempDir(), "workdir"), taskID, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(wt.WorkDir, "task.txt"), []byte("task\n"), 0o644); err != nil {
				t.Fatal(err)
			}
			lifecycleGit(t, wt.WorkDir, "add", "task.txt")
			lifecycleGit(t, wt.WorkDir, "commit", "-qm", "task")
			taskSHA := lifecycleGit(t, wt.WorkDir, "rev-parse", "HEAD")

			var terminalCalls atomic.Int64
			var finalizedBeforeReport atomic.Bool
			var mutexHeldDuringReport atomic.Bool
			var terminalBody atomic.Value
			var d *Daemon
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case strings.HasSuffix(r.URL.Path, "/status"):
					if tc.serverStatusHTTP != 0 {
						w.WriteHeader(tc.serverStatusHTTP)
						return
					}
					w.Header().Set("Content-Type", "application/json")
					_, _ = fmt.Fprintf(w, `{"status":%q}`, tc.serverStatus)
				case strings.HasSuffix(r.URL.Path, tc.wantEndpoint):
					terminalCalls.Add(1)
					body, _ := io.ReadAll(r.Body)
					terminalBody.Store(string(body))
					if !strings.Contains(lifecycleGit(t, src, "worktree", "list", "--porcelain"), wt.WorkDir) {
						finalizedBeforeReport.Store(true)
					}
					if holder := d.localPathLocks.Holder(realSrc); holder == taskID {
						mutexHeldDuringReport.Store(true)
					}
					w.WriteHeader(http.StatusOK)
				default:
					w.WriteHeader(http.StatusOK)
				}
			}))
			t.Cleanup(srv.Close)

			d = &Daemon{
				cfg:                 Config{DaemonID: "d-lifecycle"},
				client:              NewClient(srv.URL),
				logger:              slog.New(slog.NewTextHandler(io.Discard, nil)),
				workspaces:          make(map[string]*workspaceState),
				runtimeIndex:        map[string]Runtime{"rt-1": {ID: "rt-1", Provider: "claude"}},
				activeEnvRoots:      make(map[string]int),
				deletingEnvRoots:    make(map[string]bool),
				activeCodexStores:   make(map[string]int),
				deletingCodexStores: make(map[string]bool),
				localPathLocks:      NewLocalPathLocker(),
				cancelPollInterval:  time.Hour,
			}
			d.runner = taskRunnerFunc(func(_ context.Context, _ Task, _ string, _ int, _ *slog.Logger) (TaskResult, error) {
				return TaskResult{
					Status:              tc.resultStatus,
					Comment:             tc.resultStatus,
					PublishBackWorktree: wt,
					PublishBackProvider: "claude",
				}, nil
			})
			raw, err := json.Marshal(localDirectoryRef{
				LocalPath:   src,
				DaemonID:    "d-lifecycle",
				Isolate:     true,
				PublishBack: localDirectoryPublishBackSerialFF,
			})
			if err != nil {
				t.Fatal(err)
			}
			task := Task{
				ID:          taskID,
				WorkspaceID: "ws-lifecycle",
				RuntimeID:   "rt-1",
				IssueID:     "issue-lifecycle",
				Agent:       &AgentData{Name: "test-agent"},
				ProjectResources: []ProjectResourceData{{
					ID: "r1", ResourceType: localDirectoryResourceType, ResourceRef: raw,
				}},
			}

			d.handleTask(context.Background(), task, 0)

			if terminalCalls.Load() != 1 {
				t.Fatalf("terminal calls = %d, want 1", terminalCalls.Load())
			}
			if !finalizedBeforeReport.Load() {
				t.Fatal("worktree cleanup did not finish before terminal report")
			}
			if !mutexHeldDuringReport.Load() {
				t.Fatal("local path mutex was released before terminal report")
			}
			if holder := d.localPathLocks.Holder(realSrc); holder != "" {
				t.Fatalf("mutex holder after handleTask = %q", holder)
			}
			if tc.wantUnconfirmedStatus {
				body, _ := terminalBody.Load().(string)
				if !strings.Contains(body, "local_directory_publish_status_unconfirmed") || !strings.Contains(body, "refs/multica/quarantine/"+taskID) {
					t.Fatalf("terminal report missing unconfirmed-status recovery detail: %s", body)
				}
			}
			if tc.wantPublished {
				if got := lifecycleGit(t, src, "rev-parse", "HEAD"); got != taskSHA {
					t.Fatalf("source HEAD = %s, want %s", got, taskSHA)
				}
			} else {
				if got := lifecycleGit(t, src, "rev-parse", "HEAD"); got != base {
					t.Fatalf("source HEAD = %s, want base %s", got, base)
				}
				if got := lifecycleGit(t, src, "rev-parse", fmt.Sprintf("refs/multica/quarantine/%s", taskID)); got != taskSHA {
					t.Fatalf("quarantine = %s, want %s", got, taskSHA)
				}
			}
		})
	}
}

func TestHandleTask_SerialPublishBackServerCancellationNeverPublishes(t *testing.T) {
	src := newLifecycleSourceRepo(t)
	base := lifecycleGit(t, src, "rev-parse", "HEAD")
	const taskID = "task-lifecycle-cancel"
	wt, err := execenv.PrepareIsolatedLocalWorktree(src, filepath.Join(t.TempDir(), "workdir"), taskID, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(wt.WorkDir, "task.txt"), []byte("task\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	lifecycleGit(t, wt.WorkDir, "add", "task.txt")
	lifecycleGit(t, wt.WorkDir, "commit", "-qm", "task")
	taskSHA := lifecycleGit(t, wt.WorkDir, "rev-parse", "HEAD")

	var completeCalls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/status"):
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"status":"cancelled"}`))
		case strings.HasSuffix(r.URL.Path, "/complete"):
			completeCalls.Add(1)
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	t.Cleanup(srv.Close)
	d := &Daemon{
		cfg:                 Config{DaemonID: "d-cancel"},
		client:              NewClient(srv.URL),
		logger:              slog.New(slog.NewTextHandler(io.Discard, nil)),
		workspaces:          make(map[string]*workspaceState),
		runtimeIndex:        map[string]Runtime{"rt-1": {ID: "rt-1", Provider: "claude"}},
		activeEnvRoots:      make(map[string]int),
		deletingEnvRoots:    make(map[string]bool),
		activeCodexStores:   make(map[string]int),
		deletingCodexStores: make(map[string]bool),
		localPathLocks:      NewLocalPathLocker(),
		cancelPollInterval:  time.Hour,
	}
	d.runner = taskRunnerFunc(func(_ context.Context, _ Task, _ string, _ int, _ *slog.Logger) (TaskResult, error) {
		return TaskResult{Status: "completed", PublishBackWorktree: wt, PublishBackProvider: "claude"}, nil
	})
	raw, err := json.Marshal(localDirectoryRef{
		LocalPath: src, DaemonID: "d-cancel", Isolate: true, PublishBack: localDirectoryPublishBackSerialFF,
	})
	if err != nil {
		t.Fatal(err)
	}
	d.handleTask(context.Background(), Task{
		ID: taskID, WorkspaceID: "ws", RuntimeID: "rt-1", IssueID: "issue", Agent: &AgentData{Name: "agent"},
		ProjectResources: []ProjectResourceData{{ID: "r1", ResourceType: localDirectoryResourceType, ResourceRef: raw}},
	}, 0)

	if completeCalls.Load() != 0 {
		t.Fatalf("complete calls = %d, want 0", completeCalls.Load())
	}
	if got := lifecycleGit(t, src, "rev-parse", "HEAD"); got != base {
		t.Fatalf("cancelled task published source HEAD %s", got)
	}
	if got := lifecycleGit(t, src, "rev-parse", "refs/multica/quarantine/"+taskID); got != taskSHA {
		t.Fatalf("quarantine = %s, want %s", got, taskSHA)
	}
}
