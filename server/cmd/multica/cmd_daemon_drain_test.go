package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/spf13/cobra"
)

func TestRequestDaemonDrain(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/drain" {
			http.Error(w, "wrong request", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"draining","active_task_count":3,"claims_in_flight":1}`))
	}))
	defer server.Close()

	response, err := requestDaemonDrainURL(server.URL + "/drain")
	if err != nil {
		t.Fatalf("requestDaemonDrainURL: %v", err)
	}
	if response.Status != "draining" || response.ActiveTaskCount != 3 || response.ClaimsInFlight != 1 {
		t.Fatalf("unexpected response: %+v", response)
	}
}

func TestRequestDaemonDrainExplainsOldDaemon(t *testing.T) {
	server := httptest.NewServer(http.NotFoundHandler())
	defer server.Close()

	_, err := requestDaemonDrainURL(server.URL + "/drain")
	if err == nil || !strings.Contains(err.Error(), "does not support maintenance drain") {
		t.Fatalf("error = %v, want old-daemon upgrade guidance", err)
	}
}

func TestPrintDaemonStatusReportsDrainProgress(t *testing.T) {
	health := map[string]any{
		"status":            "draining",
		"pid":               float64(1234),
		"uptime":            "1h2m3s",
		"active_task_count": float64(2),
		"claims_in_flight":  float64(1),
		"workspaces":        []any{},
	}

	var out bytes.Buffer
	printDaemonStatusReport(&out, "Daemon", health)
	got := out.String()
	for _, want := range []string{"draining (pid 1234", "Active tasks:", "2", "Claims in flight:", "1"} {
		if !strings.Contains(got, want) {
			t.Fatalf("daemon status output = %q, want %q", got, want)
		}
	}
}

func TestDaemonDrainCommandIsRegistered(t *testing.T) {
	command, _, err := daemonCmd.Find([]string{"drain"})
	if err != nil {
		t.Fatalf("find drain command: %v", err)
	}
	if command != daemonDrainCmd {
		t.Fatalf("daemon drain resolved to %q", command.Name())
	}
	if got := command.Flags().Lookup("timeout"); got == nil {
		t.Fatal("daemon drain is missing --timeout")
	}
}

func TestRunDaemonDrainWaitsForDaemonExit(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	profile, listener := listenOnTestProfile(t)
	var status atomic.Value
	status.Store("running")

	var server *http.Server
	server = &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/health":
			_ = json.NewEncoder(w).Encode(map[string]any{"status": status.Load().(string)})
		case "/drain":
			status.Store("draining")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"status":            "draining",
				"active_task_count": 1,
				"claims_in_flight":  0,
			})
			go func() {
				time.Sleep(20 * time.Millisecond)
				_ = server.Shutdown(context.Background())
			}()
		default:
			http.NotFound(w, r)
		}
	})}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { _ = server.Close() })

	cmd := newDrainTestCommand(t, profile, time.Second)
	if err := runDaemonDrain(cmd, nil); err != nil {
		t.Fatalf("runDaemonDrain: %v", err)
	}
	if got := status.Load().(string); got != "draining" {
		t.Fatalf("server status = %q, want draining", got)
	}
}

func TestRunDaemonDrainTimeoutLeavesAdmissionClosed(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	profile, listener := listenOnTestProfile(t)
	var status atomic.Value
	status.Store("running")

	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/drain" {
			status.Store("draining")
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "draining"})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"status": status.Load().(string)})
	})}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { _ = server.Close() })

	cmd := newDrainTestCommand(t, profile, 20*time.Millisecond)
	err := runDaemonDrain(cmd, nil)
	if err == nil || !strings.Contains(err.Error(), "claims remain paused") {
		t.Fatalf("error = %v, want persistent-drain timeout guidance", err)
	}
	if got := status.Load().(string); got != "draining" {
		t.Fatalf("server status after timeout = %q, want draining", got)
	}
}

func listenOnTestProfile(t *testing.T) (string, net.Listener) {
	t.Helper()
	for i := 0; i < 1000; i++ {
		profile := fmt.Sprintf("drain-test-%d-%d", time.Now().UnixNano(), i)
		listener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", healthPortForProfile(profile)))
		if err == nil {
			return profile, listener
		}
	}
	t.Fatal("could not reserve a test profile health port")
	return "", nil
}

func newDrainTestCommand(t *testing.T, profile string, timeout time.Duration) *cobra.Command {
	t.Helper()
	cmd := &cobra.Command{}
	cmd.Flags().String("profile", profile, "")
	cmd.Flags().Duration("timeout", timeout, "")
	return cmd
}
