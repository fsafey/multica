package main

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/multica-ai/multica/server/internal/daemon"
)

// TestTakeoverDaemonOwner_DrainsAndWaits covers supported takeover: the
// launcher asks the incumbent to drain via its recorded health port, waits for
// it to deregister and release the machine lock, and then the lock is free for
// the successor.
// The incumbent is stood in for by a held same-process lock that a fake
// /drain endpoint releases (flock treats two opens of one file as
// contending, even in one process).
func TestTakeoverDaemonOwner_DrainsAndWaits(t *testing.T) {
	baseDir := t.TempDir()

	held, err := daemon.AcquireOwnership(baseDir, daemon.OwnerInfo{PID: 4242, Version: "0.4.2", StartedAt: time.Now()})
	if err != nil {
		t.Fatalf("seed incumbent lock: %v", err)
	}
	var releaseOnce sync.Once
	defer held.Release()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/drain" {
			releaseOnce.Do(func() { _ = held.Release() })
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"status":"draining","active_task_count":0,"claims_in_flight":0}`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	port := mustPort(t, srv.URL)
	if err := takeoverDaemonOwner(baseDir, daemon.OwnerInfo{PID: 4242, HealthPort: port}); err != nil {
		t.Fatalf("takeover should succeed once the incumbent releases: %v", err)
	}

	// The lock must now be free for the successor.
	free, _, _, err := daemon.ProbeOwnership(baseDir)
	if err != nil {
		t.Fatalf("probe after takeover: %v", err)
	}
	if !free {
		t.Fatal("lock should be free after a successful takeover")
	}
}

// TestTakeoverDaemonOwner_NoHealthPort covers the case where the incumbent
// recorded no reachable health port: takeover cannot proceed and must return an
// actionable error rather than hang.
func TestTakeoverDaemonOwner_NoHealthPort(t *testing.T) {
	baseDir := t.TempDir()
	err := takeoverDaemonOwner(baseDir, daemon.OwnerInfo{PID: 4242, HealthPort: 0})
	if err == nil {
		t.Fatal("takeover with no health port must error")
	}
}

// TestTakeoverDaemonOwner_DrainFailureNeverKills proves that a failed drain
// request leaves the incumbent lock held. The successor must time out with an
// actionable error rather than force-kill a daemon that could still be writing
// a shared checkout.
func TestTakeoverDaemonOwner_DrainFailureNeverKills(t *testing.T) {
	baseDir := t.TempDir()
	held, err := daemon.AcquireOwnership(baseDir, daemon.OwnerInfo{PID: 4242, Version: "0.4.2", StartedAt: time.Now()})
	if err != nil {
		t.Fatalf("seed incumbent lock: %v", err)
	}
	defer held.Release()

	oldWait := ownershipHandoffWait
	ownershipHandoffWait = 20 * time.Millisecond
	t.Cleanup(func() { ownershipHandoffWait = oldWait })

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/drain" {
			t.Errorf("request path = %s, want /drain", r.URL.Path)
		}
		http.Error(w, "drain unavailable", http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	startedAt := time.Now()
	err = takeoverDaemonOwner(baseDir, daemon.OwnerInfo{PID: 4242, HealthPort: mustPort(t, srv.URL)})
	if err == nil {
		t.Fatal("failed drain must not report successful takeover")
	}
	if elapsed := time.Since(startedAt); elapsed > time.Second {
		t.Fatalf("bounded takeover took %s", elapsed)
	}
	if !strings.Contains(err.Error(), "within 20ms") {
		t.Fatalf("timeout error missing bound: %v", err)
	}
	free, _, _, probeErr := daemon.ProbeOwnership(baseDir)
	if probeErr != nil {
		t.Fatalf("probe incumbent lock: %v", probeErr)
	}
	if free {
		t.Fatal("failed drain must leave incumbent ownership intact; takeover must never force-kill it")
	}
}

// TestDetectUnlockedDaemon covers the rolling-upgrade window where an older
// profile daemon has a health port but no machine lock. This is deliberately a
// stubbed health probe: no test touches a developer's real daemon ports.
func TestDetectUnlockedDaemon(t *testing.T) {
	restore := probeDaemonHealth
	t.Cleanup(func() { probeDaemonHealth = restore })

	nativeAlive := map[string]any{"status": "running", "os": runtime.GOOS}
	dead := map[string]any{}
	foreignOS := "windows"
	if runtime.GOOS == "windows" {
		foreignOS = "linux"
	}

	t.Run("refuses a known profile without lock", func(t *testing.T) {
		baseDir := t.TempDir()
		if err := os.MkdirAll(filepath.Join(baseDir, "profiles", "desktop-host"), 0o755); err != nil {
			t.Fatal(err)
		}
		legacyPort := healthPortForProfile("desktop-host")
		probeDaemonHealth = func(port int) map[string]any {
			if port == legacyPort {
				return nativeAlive
			}
			return dead
		}

		err := detectUnlockedDaemon(baseDir)
		if err == nil {
			t.Fatal("an unlocked daemon must block a new launch")
		}
		for _, want := range []string{"--profile desktop-host", "MULTICA_DAEMON_ALLOW_MULTIPLE"} {
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("error missing %q: %v", want, err)
			}
		}
	})

	t.Run("skips a foreign OS daemon", func(t *testing.T) {
		probeDaemonHealth = func(int) map[string]any {
			return map[string]any{"status": "running", "os": foreignOS}
		}
		if err := detectUnlockedDaemon(t.TempDir()); err != nil {
			t.Fatalf("foreign OS daemon must not receive a native stop instruction: %v", err)
		}
	})

	t.Run("break glass skips the sweep", func(t *testing.T) {
		t.Setenv("MULTICA_DAEMON_ALLOW_MULTIPLE", "1")
		probeDaemonHealth = func(int) map[string]any { return nativeAlive }
		if err := detectUnlockedDaemon(t.TempDir()); err != nil {
			t.Fatalf("break glass must bypass the sweep: %v", err)
		}
	})
}

func mustPort(t *testing.T, rawURL string) int {
	t.Helper()
	u, err := url.Parse(rawURL)
	if err != nil {
		t.Fatalf("parse %q: %v", rawURL, err)
	}
	p, err := strconv.Atoi(u.Port())
	if err != nil {
		t.Fatalf("port from %q: %v", rawURL, err)
	}
	return p
}
