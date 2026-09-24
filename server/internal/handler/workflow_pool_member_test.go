package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
)

func TestPruneStaleRuntimePoolMemberIsScopedAndRejectsLiveRuntimes(t *testing.T) {
	if testHandler == nil {
		t.Skip("database not available")
	}
	ctx := context.Background()
	var poolID string
	if err := testPool.QueryRow(ctx, `
		INSERT INTO runtime_pool (
			workspace_id, name, enabled, max_inflight,
			affinity_grace_seconds, lease_seconds, created_by
		)
		VALUES ($1, $2, true, 1, 60, 90, $3)
		RETURNING id
	`, testWorkspaceID, "dangling-member-"+uuid.NewString(), testUserID).Scan(&poolID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = testPool.Exec(context.Background(), `DELETE FROM runtime_pool_runtime WHERE pool_id = $1`, poolID)
		_, _ = testPool.Exec(context.Background(), `DELETE FROM runtime_pool WHERE id = $1`, poolID)
	})
	staleRuntimeID := uuid.NewString()
	if _, err := testPool.Exec(ctx, `
		INSERT INTO runtime_pool_runtime (pool_id, runtime_id, priority, enabled)
		VALUES ($1, $2, 10, true)
	`, poolID, staleRuntimeID); err != nil {
		t.Fatal(err)
	}

	remove := func(workspaceID, runtimeID string) (int, map[string]any) {
		t.Helper()
		req := newRequest(http.MethodDelete, "/api/workspaces/"+workspaceID+
			"/runtime-pools/"+poolID+"/runtimes/"+runtimeID, nil)
		req = withURLParams(req, "id", workspaceID, "poolId", poolID, "runtimeId", runtimeID)
		w := httptest.NewRecorder()
		testHandler.PruneStaleRuntimePoolMember(w, req)
		var body map[string]any
		if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		return w.Code, body
	}

	if status, _ := remove(uuid.NewString(), staleRuntimeID); status != http.StatusBadRequest {
		t.Fatalf("wrong workspace removal status = %d, want 400", status)
	}
	var count int
	if err := testPool.QueryRow(ctx, `
		SELECT count(*) FROM runtime_pool_runtime WHERE pool_id = $1 AND runtime_id = $2
	`, poolID, staleRuntimeID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("wrong workspace removed membership: count = %d", count)
	}

	if _, err := testPool.Exec(ctx, `
		INSERT INTO runtime_pool_runtime (pool_id, runtime_id, priority, enabled)
		VALUES ($1, $2, 10, true)
	`, poolID, testRuntimeID); err != nil {
		t.Fatal(err)
	}
	if status, _ := remove(testWorkspaceID, testRuntimeID); status != http.StatusConflict {
		t.Fatalf("live runtime removal status = %d, want 409", status)
	}
	if err := testPool.QueryRow(ctx, `
		SELECT count(*) FROM runtime_pool_runtime WHERE pool_id = $1 AND runtime_id = $2
	`, poolID, testRuntimeID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("live runtime membership was removed: count = %d", count)
	}

	if status, body := remove(testWorkspaceID, staleRuntimeID); status != http.StatusOK || body["removed"] != true {
		t.Fatalf("remove stale member = %d %#v, want 200 removed", status, body)
	}
	if status, body := remove(testWorkspaceID, staleRuntimeID); status != http.StatusOK || body["removed"] != false {
		t.Fatalf("repeat remove = %d %#v, want 200 no-op", status, body)
	}
}
