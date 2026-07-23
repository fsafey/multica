package service

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/events"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

func TestWorkflowClaimFiftyWorkersCreateOneAttempt(t *testing.T) {
	ctx := context.Background()
	pool := newTaskClaimRacePool(t)
	var workflowSchemaPresent bool
	if err := pool.QueryRow(ctx,
		`SELECT to_regclass('public.workflow_node') IS NOT NULL`,
	).Scan(&workflowSchemaPresent); err != nil || !workflowSchemaPresent {
		t.Skip("workflow schema migrations are not present in the configured test database")
	}
	suffix := time.Now().UnixNano()

	var userID, workspaceID, projectID, issueID, runtimeID, agentID string
	if err := pool.QueryRow(ctx,
		`INSERT INTO "user" (name, email) VALUES ('Workflow Claim Test', $1) RETURNING id`,
		fmt.Sprintf("workflow-claim-%d@example.invalid", suffix),
	).Scan(&userID); err != nil {
		t.Fatalf("create user: %v", err)
	}
	if err := pool.QueryRow(ctx, `
		INSERT INTO workspace (name, slug, description, issue_prefix)
		VALUES ('Workflow Claim Test', $1, '', 'WFC')
		RETURNING id
	`, fmt.Sprintf("workflow-claim-%d", suffix)).Scan(&workspaceID); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if _, err := pool.Exec(ctx,
		`INSERT INTO member (workspace_id, user_id, role) VALUES ($1, $2, 'owner')`,
		workspaceID,
		userID,
	); err != nil {
		t.Fatalf("create member: %v", err)
	}
	if err := pool.QueryRow(ctx, `
		INSERT INTO project (workspace_id, title, description, status)
		VALUES ($1, 'Workflow Project', '', 'in_progress')
		RETURNING id
	`, workspaceID).Scan(&projectID); err != nil {
		t.Fatalf("create project: %v", err)
	}
	if err := pool.QueryRow(ctx, `
		INSERT INTO issue (
			workspace_id, project_id, title, status, priority,
			creator_id, creator_type, number, position
		)
		VALUES ($1, $2, 'Workflow Issue', 'in_progress', 'none',
		        $3, 'member', 990001, 1)
		RETURNING id
	`, workspaceID, projectID, userID).Scan(&issueID); err != nil {
		t.Fatalf("create issue: %v", err)
	}
	if err := pool.QueryRow(ctx, `
		INSERT INTO agent_runtime (
			workspace_id, daemon_id, name, runtime_mode, provider, status,
			device_info, metadata, last_seen_at, visibility, owner_id
		)
		VALUES ($1, 'daemon-workflow', 'Workflow Runtime', 'local', 'codex',
		        'online', '', '{}'::jsonb, now(), 'private', $2)
		RETURNING id
	`, workspaceID, userID).Scan(&runtimeID); err != nil {
		t.Fatalf("create runtime: %v", err)
	}
	if err := pool.QueryRow(ctx, `
		INSERT INTO agent (
			workspace_id, name, description, runtime_mode, runtime_config,
			runtime_id, visibility, max_concurrent_tasks, owner_id
		)
		VALUES ($1, 'Workflow Agent', '', 'local', '{}'::jsonb, $2, 'private', 8, $3)
		RETURNING id
	`, workspaceID, runtimeID, userID).Scan(&agentID); err != nil {
		t.Fatalf("create agent: %v", err)
	}

	var runtimePoolID, runID, nodeID string
	if err := pool.QueryRow(ctx, `
		INSERT INTO runtime_pool (
			workspace_id, name, enabled, max_inflight,
			affinity_grace_seconds, lease_seconds, created_by
		)
		VALUES ($1, $2, true, 4, 60, 90, $3)
		RETURNING id
	`, workspaceID, fmt.Sprintf("workflow-pool-%d", suffix), userID).Scan(&runtimePoolID); err != nil {
		t.Fatalf("create runtime pool: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO runtime_pool_runtime (pool_id, runtime_id, priority, enabled)
		VALUES ($1, $2, 0, true)
	`, runtimePoolID, runtimeID); err != nil {
		t.Fatalf("create runtime pool member: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO agent_runtime_pool (agent_id, pool_id, enabled)
		VALUES ($1, $2, true)
	`, agentID, runtimePoolID); err != nil {
		t.Fatalf("create agent pool binding: %v", err)
	}
	if err := pool.QueryRow(ctx, `
		INSERT INTO workflow_run (
			workspace_id, project_id, anchor_issue_id, graph_key, graph_version,
			status, integration_pool_id, wip_limit, human_gate_limit, created_by
		)
		VALUES ($1, $2, $3, 'test', '1', 'running', $4, 4, 5, $5)
		RETURNING id
	`, workspaceID, projectID, issueID, runtimePoolID, userID).Scan(&runID); err != nil {
		t.Fatalf("create workflow run: %v", err)
	}
	if err := pool.QueryRow(ctx, `
		INSERT INTO workflow_node (
			run_id, issue_id, passage_key, node_key, executor_kind,
			agent_id, runtime_pool_id, state, ready_at, stealable_at,
			output_contract
		)
		VALUES (
			$1, $2, 'passage', 'node', 'agent',
			$3, $4, 'ready', now(), now(),
			'{"allowed_paths":["pipeline-output/passage/node.md"]}'::jsonb
		)
		RETURNING id
	`, runID, issueID, agentID, runtimePoolID).Scan(&nodeID); err != nil {
		t.Fatalf("create workflow node: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO workflow_node_resource (node_id, resource_key, mode)
		VALUES ($1, 'book/test/passage/passage', 'exclusive')
	`, nodeID); err != nil {
		t.Fatalf("create workflow resource: %v", err)
	}

	t.Cleanup(func() {
		cleanup := context.Background()
		for _, statement := range []string{
			`DELETE FROM workflow_outbox WHERE run_id = $1`,
			`DELETE FROM workflow_resource_claim WHERE node_id IN (SELECT id FROM workflow_node WHERE run_id = $1)`,
			`DELETE FROM workflow_node_result WHERE node_id IN (SELECT id FROM workflow_node WHERE run_id = $1)`,
			`DELETE FROM workflow_node_attempt WHERE node_id IN (SELECT id FROM workflow_node WHERE run_id = $1)`,
			`DELETE FROM workflow_node_resource WHERE node_id IN (SELECT id FROM workflow_node WHERE run_id = $1)`,
			`DELETE FROM workflow_node_dependency WHERE node_id IN (SELECT id FROM workflow_node WHERE run_id = $1) OR depends_on_node_id IN (SELECT id FROM workflow_node WHERE run_id = $1)`,
			`DELETE FROM agent_task_queue WHERE workflow_node_id IN (SELECT id FROM workflow_node WHERE run_id = $1)`,
			`DELETE FROM workflow_node WHERE run_id = $1`,
		} {
			_, _ = pool.Exec(cleanup, statement, runID)
		}
		_, _ = pool.Exec(cleanup, `DELETE FROM workflow_run WHERE id = $1`, runID)
		_, _ = pool.Exec(cleanup, `DELETE FROM agent_runtime_pool WHERE agent_id = $1`, agentID)
		_, _ = pool.Exec(cleanup, `DELETE FROM runtime_pool_runtime WHERE pool_id = $1`, runtimePoolID)
		_, _ = pool.Exec(cleanup, `DELETE FROM runtime_pool WHERE id = $1`, runtimePoolID)
		_, _ = pool.Exec(cleanup, `DELETE FROM agent WHERE id = $1`, agentID)
		_, _ = pool.Exec(cleanup, `DELETE FROM agent_runtime WHERE id = $1`, runtimeID)
		_, _ = pool.Exec(cleanup, `DELETE FROM issue WHERE id = $1`, issueID)
		_, _ = pool.Exec(cleanup, `DELETE FROM project WHERE id = $1`, projectID)
		_, _ = pool.Exec(cleanup, `DELETE FROM member WHERE workspace_id = $1`, workspaceID)
		_, _ = pool.Exec(cleanup, `DELETE FROM workspace WHERE id = $1`, workspaceID)
		_, _ = pool.Exec(cleanup, `DELETE FROM "user" WHERE id = $1`, userID)
	})

	service := NewWorkflowService(db.New(pool), pool)
	runtimes := []pgtype.UUID{util.MustParseUUID(runtimeID)}
	var claimed atomic.Int64
	var failures atomic.Int64
	var wait sync.WaitGroup
	wait.Add(50)
	for range 50 {
		go func() {
			defer wait.Done()
			tasks, err := service.MaterializeReadyAgentTasks(
				context.Background(),
				"daemon-workflow",
				runtimes,
				1,
			)
			if err != nil {
				failures.Add(1)
				return
			}
			claimed.Add(int64(len(tasks)))
		}()
	}
	wait.Wait()
	if failures.Load() != 0 {
		t.Fatalf("%d concurrent claim calls failed", failures.Load())
	}
	if claimed.Load() != 1 {
		t.Fatalf("concurrent claims returned %d tasks, want 1", claimed.Load())
	}
	var attempts, claims, tasks int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM workflow_node_attempt WHERE node_id = $1`,
		nodeID,
	).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM workflow_resource_claim WHERE node_id = $1`,
		nodeID,
	).Scan(&claims); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM agent_task_queue WHERE workflow_node_id = $1`,
		nodeID,
	).Scan(&tasks); err != nil {
		t.Fatal(err)
	}
	if attempts != 1 || claims != 1 || tasks != 1 {
		t.Fatalf("attempts=%d claims=%d tasks=%d, want 1 each", attempts, claims, tasks)
	}
	var workflowTaskID string
	if err := pool.QueryRow(ctx,
		`SELECT id FROM agent_task_queue WHERE workflow_node_id = $1`,
		nodeID,
	).Scan(&workflowTaskID); err != nil {
		t.Fatalf("load workflow task: %v", err)
	}
	taskService := NewTaskService(db.New(pool), pool, nil, events.New())
	startedTask, err := taskService.StartTask(ctx, util.MustParseUUID(workflowTaskID))
	if err != nil {
		t.Fatalf("start materialized workflow task: %v", err)
	}
	if startedTask.Status != "running" {
		t.Fatalf("started task status = %q, want running", startedTask.Status)
	}
	var attemptStatus, nodeState string
	var attemptStarted bool
	if err := pool.QueryRow(ctx, `
		SELECT a.status, a.started_at IS NOT NULL, n.state
		FROM workflow_node_attempt a
		JOIN workflow_node n ON n.id = a.node_id
		WHERE a.node_id = $1
	`, nodeID).Scan(&attemptStatus, &attemptStarted, &nodeState); err != nil {
		t.Fatalf("load started workflow state: %v", err)
	}
	if attemptStatus != "running" || !attemptStarted || nodeState != "running" {
		t.Fatalf(
			"attempt status=%q started=%t node state=%q, want running, true, running",
			attemptStatus,
			attemptStarted,
			nodeState,
		)
	}
	var preferredAtClaim *string
	var affinityStolen bool
	if err := pool.QueryRow(ctx, `
		SELECT preferred_daemon_at_claim, affinity_stolen
		FROM workflow_node_attempt
		WHERE node_id = $1
	`, nodeID).Scan(&preferredAtClaim, &affinityStolen); err != nil {
		t.Fatalf("load affinity evidence: %v", err)
	}
	if preferredAtClaim == nil || *preferredAtClaim != "daemon-workflow" {
		t.Fatalf("preferred daemon at claim = %v, want daemon-workflow", preferredAtClaim)
	}
	if affinityStolen {
		t.Fatal("first claim was incorrectly recorded as stolen")
	}

	if _, err := pool.Exec(ctx, `
		UPDATE runtime_pool SET max_inflight = 2 WHERE id = $1
	`, runtimePoolID); err != nil {
		t.Fatalf("raise integration pool capacity: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE workflow_node SET state = 'submitted' WHERE id = $1
	`, nodeID); err != nil {
		t.Fatalf("submit first node: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE workflow_node_attempt SET status = 'submitted' WHERE node_id = $1
	`, nodeID); err != nil {
		t.Fatalf("submit first attempt: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO workflow_outbox (run_id, node_id, event_type, payload)
		VALUES ($1, $2, 'workflow.artifact_submitted', '{}'::jsonb)
	`, runID, nodeID); err != nil {
		t.Fatalf("enqueue first integration: %v", err)
	}

	var secondNodeID, secondAttemptID string
	if err := pool.QueryRow(ctx, `
		INSERT INTO workflow_node (
			run_id, issue_id, passage_key, node_key, executor_kind,
			agent_id, runtime_pool_id, state, claim_epoch, ready_at,
			output_contract
		)
		VALUES (
			$1, $2, 'passage-2', 'node-2', 'agent',
			$3, $4, 'submitted', 1, now(),
			'{"allowed_paths":["pipeline-output/passage-2/node.md"]}'::jsonb
		)
		RETURNING id
	`, runID, issueID, agentID, runtimePoolID).Scan(&secondNodeID); err != nil {
		t.Fatalf("create second submitted node: %v", err)
	}
	if err := pool.QueryRow(ctx, `
		INSERT INTO workflow_node_attempt (
			node_id, claim_epoch, runtime_id, daemon_id, status,
			lease_expires_at, artifact_digest, manifest, submitted_at
		)
		VALUES (
			$1, 1, $2, 'daemon-workflow', 'submitted',
			now() + interval '90 seconds', 'sha256:test', '{}'::jsonb, now()
		)
		RETURNING id
	`, secondNodeID, runtimeID).Scan(&secondAttemptID); err != nil {
		t.Fatalf("create second submitted attempt: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE workflow_node SET current_attempt_id = $2 WHERE id = $1
	`, secondNodeID, secondAttemptID); err != nil {
		t.Fatalf("attach second submitted attempt: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO workflow_outbox (run_id, node_id, event_type, payload)
		VALUES ($1, $2, 'workflow.artifact_submitted', '{}'::jsonb)
	`, runID, secondNodeID); err != nil {
		t.Fatalf("enqueue second integration: %v", err)
	}

	integrationJobs, err := service.ClaimIntegrationJobs(
		ctx,
		"daemon-workflow",
		runtimes,
		2,
	)
	if err != nil {
		t.Fatalf("claim integration jobs: %v", err)
	}
	if len(integrationJobs) != 1 {
		t.Fatalf("claimed %d integration jobs for one book, want 1", len(integrationJobs))
	}
	var pendingJobs, processingJobs int
	if err := pool.QueryRow(ctx, `
		SELECT
			count(*) FILTER (WHERE status = 'pending'),
			count(*) FILTER (WHERE status = 'processing')
		FROM workflow_outbox
		WHERE run_id = $1
		  AND event_type = 'workflow.artifact_submitted'
	`, runID).Scan(&pendingJobs, &processingJobs); err != nil {
		t.Fatalf("count integration jobs: %v", err)
	}
	if pendingJobs != 1 || processingJobs != 1 {
		t.Fatalf("integration jobs pending=%d processing=%d, want 1 each", pendingJobs, processingJobs)
	}
	metrics, err := db.New(pool).GetWorkflowRunMetrics(ctx, util.MustParseUUID(runID))
	if err != nil {
		t.Fatalf("load workflow run metrics: %v", err)
	}
	if metrics.TotalNodes != 2 || metrics.AttemptCount != 2 {
		t.Fatalf(
			"workflow metrics nodes=%d attempts=%d, want 2 each",
			metrics.TotalNodes,
			metrics.AttemptCount,
		)
	}
	if metrics.AffinityRetainedAttempts != 1 || metrics.StolenAttempts != 0 {
		t.Fatalf(
			"workflow affinity metrics retained=%d stolen=%d, want 1 and 0",
			metrics.AffinityRetainedAttempts,
			metrics.StolenAttempts,
		)
	}
}
