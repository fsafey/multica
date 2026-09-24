package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/multica-ai/multica/server/internal/cli"
	"github.com/multica-ai/multica/server/internal/daemon"
	"github.com/spf13/cobra"
)

// prepareDaemonOwnership makes sure it is safe to (re)start a daemon on this
// machine before one is spawned. At most one daemon may own the machine-global
// lock at cli.ProfileDir("")/daemon.lock, which spans every profile — so it
// catches a Desktop-spawned daemon running under a different profile that the
// per-profile health-port guard cannot see (VWO-365).
//
// It runs in the LAUNCHER purely to give a fast, actionable error (or perform a
// requested takeover); the spawned daemon's Run re-acquires the lock for real.
//
//   - break-glass (MULTICA_DAEMON_ALLOW_MULTIPLE set) → skip.
//   - lock free → nil.
//   - lock held, --takeover → ask the incumbent to stop, wait for release.
//   - lock held, no --takeover → the actionable conflict error.
//
// ownershipHandoffWait bounds how long the launcher waits for a shutting-down
// incumbent to release the machine lock. It covers the gap between a daemon's
// health port closing (at the start of shutdown) and its ownership lock
// releasing (after it finishes deregistering runtimes), so a `daemon restart`
// or a stop-then-start does not spuriously report a conflict.
// ownershipHandoffWait is deliberately longer than the normal local request
// timeout. A takeover first asks the incumbent to drain, then waits for active
// work to finish and the daemon to deregister its runtimes before its machine
// lock is released. It never force-kills the incumbent: a second owner must
// not begin writing shared checkouts while the first is still unwinding.
var ownershipHandoffWait = 45 * time.Second

func prepareDaemonOwnership(cmd *cobra.Command) error {
	if daemon.OwnershipBypassed() {
		return nil
	}
	baseDir, err := cli.ProfileDir("")
	if err != nil {
		return fmt.Errorf("resolve base config dir for daemon ownership lock: %w", err)
	}
	free, incumbent, ok, err := daemon.ProbeOwnership(baseDir)
	if err != nil {
		return err
	}
	if free {
		return nil
	}
	if takeover, _ := cmd.Flags().GetBool("takeover"); takeover {
		return takeoverDaemonOwner(baseDir, incumbent)
	}
	// The lock is held. If the incumbent is genuinely live (its health port
	// answers), reject immediately with an actionable error. If it is NOT
	// answering, it is shutting down — the daemon a `restart`/`stop` just asked
	// to exit, still finishing deregister before it releases the lock — so wait
	// a bounded time for the clean handoff rather than reject a legitimate
	// restart. A wedged daemon that never releases falls through to the same
	// conflict error after the wait.
	if ok && incumbent.HealthPort > 0 && daemonAliveOnPort(incumbent.HealthPort) {
		return ownershipConflictErr(baseDir, incumbent, ok)
	}
	if waitForOwnershipFree(baseDir, ownershipHandoffWait) {
		return nil
	}
	return ownershipConflictErr(baseDir, incumbent, ok)
}

// probeDaemonHealth is an indirection over checkDaemonHealthOnPort so tests can
// exercise the known-profile sweep without binding real well-known ports.
var probeDaemonHealth = func(port int) map[string]any {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return checkDaemonHealthOnPort(ctx, port)
}

// detectUnlockedDaemon scans the default profile and every known named profile
// before every launch, including `--foreground`. A lock-aware daemon acquires
// the machine lock before serving /health, so a live health endpoint while the
// lock is free can only be an older daemon or explicit break-glass process.
// Refusing here closes the rolling-upgrade window where two profiles could
// otherwise execute against the same checkout.
func detectUnlockedDaemon(baseDir string) error {
	if daemon.OwnershipBypassed() {
		return nil
	}
	ports := map[int]string{daemon.DefaultHealthPort: ""}
	if entries, err := os.ReadDir(filepath.Join(baseDir, "profiles")); err == nil {
		for _, entry := range entries {
			if entry.IsDir() {
				ports[healthPortForProfile(entry.Name())] = entry.Name()
			}
		}
	}
	for port, profile := range ports {
		health := probeDaemonHealth(port)
		if !daemonAlive(health) {
			continue
		}
		// A foreign OS visible through localhost forwarding is not this
		// machine's process owner. Do not give an unusable native stop command
		// for a WSL-like topology. Missing OS fails safe as a native legacy
		// daemon that must be stopped before a new native owner starts.
		if ownerOS, _ := health["os"].(string); ownerOS != "" && ownerOS != runtime.GOOS {
			continue
		}
		label := "the default profile"
		stopHint := "multica daemon stop"
		if profile != "" {
			label = fmt.Sprintf("profile %q", profile)
			stopHint = fmt.Sprintf("multica daemon stop --profile %s", profile)
		}
		return fmt.Errorf(
			"a daemon without the machine ownership lock is running for %s (health port %d); likely an older Multica release. Stop it with `%s` and retry, or set MULTICA_DAEMON_ALLOW_MULTIPLE=1 only for break-glass recovery",
			label, port, stopHint,
		)
	}
	return nil
}

func ownershipConflictErr(baseDir string, incumbent daemon.OwnerInfo, hasInfo bool) error {
	return &daemon.OwnershipConflict{Path: daemon.OwnershipLockPath(baseDir), Incumbent: incumbent, HasInfo: hasInfo}
}

// daemonAliveOnPort reports whether a daemon health endpoint answers on port.
func daemonAliveOnPort(port int) bool {
	return daemonAlive(probeDaemonHealth(port))
}

// waitForOwnershipFree polls until the machine lock is free or timeout elapses.
func waitForOwnershipFree(baseDir string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if free, _, _, err := daemon.ProbeOwnership(baseDir); err == nil && free {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}

// takeoverDaemonOwner asks the incumbent daemon to shut down (via its recorded
// health port — cross-platform, no OS signals) and waits until it releases the
// machine lock. The OS drops the incumbent's advisory lock the instant its
// process exits, so a successful shutdown frees the lock with no stale window.
func takeoverDaemonOwner(baseDir string, incumbent daemon.OwnerInfo) error {
	if incumbent.HealthPort <= 0 {
		return fmt.Errorf("cannot take over: owner health port unknown (owner pid %d); stop it manually with `multica daemon stop`", incumbent.PID)
	}
	fmt.Fprintf(os.Stderr, "Taking over from daemon pid %d (profile %q) on health port %d...\n",
		incumbent.PID, incumbent.Profile, incumbent.HealthPort)
	if _, err := requestDaemonDrain(incumbent.HealthPort); err != nil {
		fmt.Fprintf(os.Stderr, "Drain request not delivered (%v); waiting for the daemon to release the machine on its own.\n", err)
	}
	if waitForOwnershipFree(baseDir, ownershipHandoffWait) {
		fmt.Fprintln(os.Stderr, "Previous daemon released the machine; continuing.")
		return nil
	}
	stopHint := "multica daemon stop"
	if incumbent.Profile != "" {
		stopHint = fmt.Sprintf("multica daemon stop --profile %s", incumbent.Profile)
	}
	return fmt.Errorf("takeover timed out: daemon pid %d did not release the machine lock within %s. It may still be draining; retry after it finishes, or inspect it with `%s`", incumbent.PID, ownershipHandoffWait, stopHint)
}
