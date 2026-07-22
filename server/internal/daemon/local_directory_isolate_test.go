package daemon

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// VWO-367: a local_directory resource with isolate:true must NOT take the
// whole-task path mutex — the per-task worktree provides the safety instead.
// The throughput property under test: an isolated task proceeds immediately
// even while another task holds the path lock, and holds nothing itself.
func TestAcquireLocalDirectoryLock_IsolateSkipsPathMutex(t *testing.T) {
	t.Parallel()

	const daemonID = "d-mine"
	tmp := t.TempDir()
	isolatedRaw, err := json.Marshal(localDirectoryRef{LocalPath: tmp, DaemonID: daemonID, Isolate: true})
	if err != nil {
		t.Fatalf("marshal isolated ref: %v", err)
	}
	inPlaceRaw, err := json.Marshal(localDirectoryRef{LocalPath: tmp, DaemonID: daemonID})
	if err != nil {
		t.Fatalf("marshal in-place ref: %v", err)
	}

	d := &Daemon{
		cfg:            Config{DaemonID: daemonID},
		localPathLocks: NewLocalPathLocker(),
		logger:         slog.Default(),
	}

	// Control: the in-place (default) resource still takes the lock.
	inPlace := Task{
		ID:               "in-place-task",
		ProjectResources: []ProjectResourceData{{ID: "r1", ResourceType: localDirectoryResourceType, ResourceRef: inPlaceRaw}},
	}
	inPlaceAssignment, err := localDirectoryAssignmentForTask(inPlace, daemonID)
	if err != nil || inPlaceAssignment == nil {
		t.Fatalf("in-place assignment: %v %+v", err, inPlaceAssignment)
	}
	release, abort := d.acquireLocalDirectoryLockIfNeeded(context.Background(), inPlace, slog.Default())
	if abort || release == nil {
		t.Fatalf("in-place task should hold the path mutex (abort=%v release=%v)", abort, release == nil)
	}
	if got := d.localPathLocks.Holder(inPlaceAssignment.RealPath); got != inPlace.ID {
		t.Fatalf("holder = %q, want %q", got, inPlace.ID)
	}

	// The isolated task must proceed WITHOUT blocking on the held mutex and
	// without becoming a holder itself.
	isolated := Task{
		ID:               "isolated-task",
		ProjectResources: []ProjectResourceData{{ID: "r2", ResourceType: localDirectoryResourceType, ResourceRef: isolatedRaw}},
	}
	isoAssignment, err := localDirectoryAssignmentForTask(isolated, daemonID)
	if err != nil || isoAssignment == nil {
		t.Fatalf("isolated assignment: %v %+v", err, isoAssignment)
	}
	if !isoAssignment.Ref.Isolate {
		t.Fatal("isolate flag did not survive the resource_ref round-trip into the assignment")
	}
	isoRelease, isoAbort := d.acquireLocalDirectoryLockIfNeeded(context.Background(), isolated, slog.Default())
	if isoAbort {
		t.Fatal("isolated task aborted")
	}
	if isoRelease != nil {
		t.Fatal("isolated task returned a release callback — it must not take the path mutex")
	}
	// The original holder is undisturbed.
	if got := d.localPathLocks.Holder(isoAssignment.RealPath); got != inPlace.ID {
		t.Fatalf("holder after isolated acquire = %q, want %q", got, inPlace.ID)
	}
	release()
}

func TestAcquireLocalDirectoryLock_SerialPublishBackKeepsPathMutex(t *testing.T) {
	t.Parallel()

	const daemonID = "d-publish"
	tmp := t.TempDir()
	if out, err := exec.Command("git", "-C", tmp, "init", "-q").CombinedOutput(); err != nil {
		t.Fatalf("git init: %s: %v", out, err)
	}
	raw, err := json.Marshal(localDirectoryRef{
		LocalPath:   tmp,
		DaemonID:    daemonID,
		Isolate:     true,
		PublishBack: localDirectoryPublishBackSerialFF,
	})
	if err != nil {
		t.Fatal(err)
	}
	d := &Daemon{
		cfg:            Config{DaemonID: daemonID},
		localPathLocks: NewLocalPathLocker(),
		logger:         slog.Default(),
	}
	task := Task{
		ID:               "publish-task",
		ProjectResources: []ProjectResourceData{{ID: "r1", ResourceType: localDirectoryResourceType, ResourceRef: raw}},
	}
	assignment, err := localDirectoryAssignmentForTask(task, daemonID)
	if err != nil || assignment == nil {
		t.Fatalf("assignment: %v %+v", err, assignment)
	}
	release, abort := d.acquireLocalDirectoryLockIfNeeded(context.Background(), task, slog.Default())
	if abort || release == nil {
		t.Fatalf("serial publish-back must hold the path mutex (abort=%v release_nil=%v)", abort, release == nil)
	}
	if got := d.localPathLocks.Holder(assignment.RealPath); got != task.ID {
		t.Fatalf("holder = %q, want %q", got, task.ID)
	}
	release()
}

func TestLocalDirectoryMutexKey_CollapsesRepoSubdirectoriesAcrossModes(t *testing.T) {
	t.Parallel()

	repo := t.TempDir()
	if out, err := exec.Command("git", "-C", repo, "init", "-q").CombinedOutput(); err != nil {
		t.Fatalf("git init: %s: %v", out, err)
	}
	subA := filepath.Join(repo, "a")
	subB := filepath.Join(repo, "b")
	if err := os.MkdirAll(subA, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(subB, 0o755); err != nil {
		t.Fatal(err)
	}
	publishRef := localDirectoryRef{Isolate: true, PublishBack: localDirectoryPublishBackSerialFF}
	a := &localDirectoryAssignment{Ref: publishRef, AbsPath: subA, RealPath: subA}
	b := &localDirectoryAssignment{Ref: localDirectoryRef{}, AbsPath: subB, RealPath: subB}
	keyA, err := a.mutexKey()
	if err != nil {
		t.Fatal(err)
	}
	keyB, err := b.mutexKey()
	if err != nil {
		t.Fatal(err)
	}
	if keyA != keyB {
		t.Fatalf("same repository produced distinct mutex keys: %q != %q", keyA, keyB)
	}
}
