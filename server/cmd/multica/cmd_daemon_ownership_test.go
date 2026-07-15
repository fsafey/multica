package main

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/multica-ai/multica/server/internal/daemon"
)

// TestTakeoverDaemonOwner_ShutsDownAndWaits covers supported takeover: the
// launcher asks the incumbent to stop via its recorded health port, waits for
// it to release the machine lock, and then the lock is free for the successor.
// The incumbent is stood in for by a held same-process lock that a fake
// /shutdown endpoint releases (flock treats two opens of one file as
// contending, even in one process).
func TestTakeoverDaemonOwner_ShutsDownAndWaits(t *testing.T) {
	baseDir := t.TempDir()

	held, err := daemon.AcquireOwnership(baseDir, daemon.OwnerInfo{PID: 4242, Version: "0.4.2", StartedAt: time.Now()})
	if err != nil {
		t.Fatalf("seed incumbent lock: %v", err)
	}
	var releaseOnce sync.Once
	defer held.Release()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/shutdown" {
			releaseOnce.Do(func() { _ = held.Release() })
			w.WriteHeader(http.StatusOK)
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
