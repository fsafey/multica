package handler

import (
	"context"
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/multica-ai/multica/server/internal/service"
	"github.com/multica-ai/multica/server/internal/testutil"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

func TestPruneStaleRuntimePoolMemberIsScopedAndRejectsLiveRuntimes(t *testing.T) {
	if testHandler == nil {
		t.Skip("database not available")
	}
	poolID := dbfx.Insert(t, "runtime_pool", testutil.Cols{
		"workspace_id":           testWorkspaceID,
		"name":                   "dangling-member-" + uuid.NewString(),
		"enabled":                true,
		"max_inflight":           1,
		"affinity_grace_seconds": 60,
		"lease_seconds":          90,
		"created_by":             testUserID,
	})
	staleRuntimeID := uuid.NewString()
	dbfx.InsertNoID(t, "runtime_pool_runtime", testutil.Cols{
		"pool_id": poolID, "runtime_id": staleRuntimeID, "priority": 10, "enabled": true,
	}, "pool_id = $1 AND runtime_id = $2", poolID, staleRuntimeID)

	remove := func(workspaceID, runtimeID string, status int) map[string]any {
		t.Helper()
		req := newRequest(http.MethodDelete, "/api/workspaces/"+workspaceID+
			"/runtime-pools/"+poolID+"/runtimes/"+runtimeID, nil)
		req = withURLParams(req, "id", workspaceID, "poolId", poolID, "runtimeId", runtimeID)
		var body map[string]any
		testutil.Call(t, testHandler.PruneStaleRuntimePoolMember, req).Want(status).JSON(&body)
		return body
	}

	remove(uuid.NewString(), staleRuntimeID, http.StatusBadRequest)
	count := dbfx.Count(t, `SELECT count(*) FROM runtime_pool_runtime WHERE pool_id = $1 AND runtime_id = $2`, poolID, staleRuntimeID)
	if count != 1 {
		t.Fatalf("wrong workspace removed membership: count = %d", count)
	}

	dbfx.InsertNoID(t, "runtime_pool_runtime", testutil.Cols{
		"pool_id": poolID, "runtime_id": testRuntimeID, "priority": 10, "enabled": true,
	}, "pool_id = $1 AND runtime_id = $2", poolID, testRuntimeID)
	remove(testWorkspaceID, testRuntimeID, http.StatusConflict)
	count = dbfx.Count(t, `SELECT count(*) FROM runtime_pool_runtime WHERE pool_id = $1 AND runtime_id = $2`, poolID, testRuntimeID)
	if count != 1 {
		t.Fatalf("live runtime membership was removed: count = %d", count)
	}

	if body := remove(testWorkspaceID, staleRuntimeID, http.StatusOK); body["removed"] != true {
		t.Fatalf("remove stale member = %#v, want removed", body)
	}
	if body := remove(testWorkspaceID, staleRuntimeID, http.StatusOK); body["removed"] != false {
		t.Fatalf("repeat remove = %#v, want no-op", body)
	}
}

// Retention GC never collects a pool member, so pruning is the only way a
// retired worker's row becomes reclaimable. A runtime offline past the
// retention TTL counts as stale; one that only just went offline does not.
func TestPruneStaleRuntimePoolMemberReleasesRuntimePastRetentionToGC(t *testing.T) {
	if testHandler == nil {
		t.Skip("database not available")
	}
	poolID := dbfx.Insert(t, "runtime_pool", testutil.Cols{
		"workspace_id": testWorkspaceID, "name": "retired-member-" + uuid.NewString(),
		"enabled": true, "max_inflight": 1, "affinity_grace_seconds": 60,
		"lease_seconds": 90, "created_by": testUserID,
	})
	retiredID := dbfx.Runtime(t, "retired pool runtime", testutil.Cols{
		"status": "offline", "last_seen_at": testutil.Raw("now() - interval '8 days'"),
	})
	recentID := dbfx.Runtime(t, "recently offline pool runtime", testutil.Cols{
		"status": "offline", "last_seen_at": testutil.Raw("now() - interval '1 hour'"),
	})
	for _, runtimeID := range []string{retiredID, recentID} {
		dbfx.InsertNoID(t, "runtime_pool_runtime", testutil.Cols{
			"pool_id": poolID, "runtime_id": runtimeID, "priority": 10, "enabled": true,
		}, "pool_id = $1 AND runtime_id = $2", poolID, runtimeID)
	}
	remove := func(runtimeID string, status int) map[string]any {
		t.Helper()
		req := newRequest(http.MethodDelete, "/api/workspaces/"+testWorkspaceID+
			"/runtime-pools/"+poolID+"/runtimes/"+runtimeID, nil)
		req = withURLParams(req, "id", testWorkspaceID, "poolId", poolID, "runtimeId", runtimeID)
		var body map[string]any
		testutil.Call(t, testHandler.PruneStaleRuntimePoolMember, req).Want(status).JSON(&body)
		return body
	}
	gcCandidate := func(runtimeID string) bool {
		t.Helper()
		rows, err := testHandler.Queries.ListStaleOfflineRuntimeGCCandidates(context.Background(), db.ListStaleOfflineRuntimeGCCandidatesParams{
			StaleSeconds: service.OfflineRuntimeTTLSeconds, MaxPerTick: 10000,
		})
		if err != nil {
			t.Fatal(err)
		}
		for _, id := range rows {
			if uuidToString(id) == runtimeID {
				return true
			}
		}
		return false
	}

	remove(recentID, http.StatusConflict)
	if count := dbfx.Count(t, `SELECT count(*) FROM runtime_pool_runtime WHERE runtime_id = $1`, recentID); count != 1 {
		t.Fatalf("recently offline membership was removed: count = %d", count)
	}

	if gcCandidate(retiredID) {
		t.Fatal("GC candidate set includes a pool member")
	}
	if body := remove(retiredID, http.StatusOK); body["removed"] != true {
		t.Fatalf("prune retired member = %#v, want removed", body)
	}
	if !gcCandidate(retiredID) {
		t.Fatal("pruned runtime past retention did not become a GC candidate")
	}
}

func TestRuntimeDeletionAndLegacyMergeKeepPoolMembershipsConsistent(t *testing.T) {
	if testHandler == nil {
		t.Skip("database not available")
	}
	poolID := dbfx.Insert(t, "runtime_pool", testutil.Cols{
		"workspace_id": testWorkspaceID, "name": "runtime-lifecycle-" + uuid.NewString(),
		"enabled": true, "max_inflight": 1, "affinity_grace_seconds": 60,
		"lease_seconds": 90, "created_by": testUserID,
	})
	oldRuntimeID := dbfx.Runtime(t, "old pool runtime")
	newRuntimeID := dbfx.Runtime(t, "new pool runtime")
	if _, err := testHandler.WorkflowService.AddRuntimeToPool(context.Background(),
		parseUUID(testWorkspaceID), parseUUID(poolID), parseUUID(oldRuntimeID), 7); err != nil {
		t.Fatalf("add live runtime to pool: %v", err)
	}
	dbfx.Cleanup(t, `DELETE FROM runtime_pool_runtime WHERE pool_id = $1`, poolID)

	if err := testHandler.mergeLegacyRuntime(context.Background(), parseUUID(newRuntimeID), parseUUID(oldRuntimeID), "legacy-pool-test", "handler_test_runtime", parseUUID(testUserID)); err != nil {
		t.Fatalf("merge legacy runtime: %v", err)
	}
	if count := dbfx.Count(t, `SELECT count(*) FROM runtime_pool_runtime WHERE pool_id = $1 AND runtime_id = $2`, poolID, oldRuntimeID); count != 0 {
		t.Fatalf("merged runtime left %d old memberships", count)
	}
	if count := dbfx.Count(t, `SELECT count(*) FROM runtime_pool_runtime WHERE pool_id = $1 AND runtime_id = $2 AND priority = 7`, poolID, newRuntimeID); count != 1 {
		t.Fatalf("merged runtime has %d transferred memberships, want 1", count)
	}

	if err := testHandler.Queries.DeleteAgentRuntime(context.Background(), parseUUID(newRuntimeID)); err != nil {
		t.Fatalf("delete merged runtime: %v", err)
	}
	if count := dbfx.Count(t, `SELECT count(*) FROM runtime_pool_runtime WHERE pool_id = $1 AND runtime_id = $2`, poolID, newRuntimeID); count != 0 {
		t.Fatalf("deleted runtime left %d pool memberships", count)
	}
}
