package service

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/multica-ai/multica/server/internal/events"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

type workflowTerminalFixture struct {
	pool        *pgxpool.Pool
	userID      string
	workspaceID string
	runID       string
	nodeID      string
	attemptID   string
	taskID      string
}

func seedWorkflowTerminalFixture(t *testing.T) workflowTerminalFixture {
	t.Helper()
	ctx := context.Background()
	pool := sharedTestPool(t)
	suffix := time.Now().UnixNano()
	f := workflowTerminalFixture{pool: pool}
	var projectID, issueID, runtimeID, agentID string
	insert := func(label, sql string, args []any, target *string) {
		t.Helper()
		if err := pool.QueryRow(ctx, sql, args...).Scan(target); err != nil {
			t.Fatalf("create %s: %v", label, err)
		}
	}
	insert("user", `INSERT INTO "user" (name, email) VALUES ('Workflow terminal', $1) RETURNING id`,
		[]any{fmt.Sprintf("workflow-terminal-%d@example.invalid", suffix)}, &f.userID)
	insert("workspace", `INSERT INTO workspace (name, slug, description, issue_prefix) VALUES ('Workflow terminal', $1, '', 'WFT') RETURNING id`,
		[]any{fmt.Sprintf("workflow-terminal-%d", suffix)}, &f.workspaceID)
	if _, err := pool.Exec(ctx, `INSERT INTO member (workspace_id, user_id, role) VALUES ($1, $2, 'owner')`, f.workspaceID, f.userID); err != nil {
		t.Fatalf("create member: %v", err)
	}
	insert("project", `INSERT INTO project (workspace_id, title, description, status) VALUES ($1, 'Workflow terminal', '', 'in_progress') RETURNING id`,
		[]any{f.workspaceID}, &projectID)
	insert("issue", `INSERT INTO issue (workspace_id, project_id, title, status, priority, creator_id, creator_type, number, position) VALUES ($1, $2, 'Workflow terminal', 'in_progress', 'none', $3, 'member', 990003, 1) RETURNING id`,
		[]any{f.workspaceID, projectID, f.userID}, &issueID)
	insert("runtime", `INSERT INTO agent_runtime (workspace_id, daemon_id, name, runtime_mode, provider, status, device_info, metadata, last_seen_at, visibility, owner_id) VALUES ($1, 'workflow-terminal-daemon', 'Workflow terminal', 'local', 'codex', 'online', '', '{}'::jsonb, now(), 'private', $2) RETURNING id`,
		[]any{f.workspaceID, f.userID}, &runtimeID)
	insert("agent", `INSERT INTO agent (workspace_id, name, description, runtime_mode, runtime_config, runtime_id, visibility, max_concurrent_tasks, owner_id) VALUES ($1, 'Workflow terminal', '', 'local', '{}'::jsonb, $2, 'private', 1, $3) RETURNING id`,
		[]any{f.workspaceID, runtimeID, f.userID}, &agentID)
	insert("run", `INSERT INTO workflow_run (workspace_id, project_id, anchor_issue_id, graph_key, graph_version, status, created_by) VALUES ($1, $2, $3, 'terminal-test', '1', 'running', $4) RETURNING id`,
		[]any{f.workspaceID, projectID, issueID, f.userID}, &f.runID)
	insert("node", `INSERT INTO workflow_node (run_id, issue_id, passage_key, node_key, executor_kind, agent_id, state, claim_epoch, output_contract) VALUES ($1, $2, 'passage', 'production', 'agent', $3, 'running', 1, '{}'::jsonb) RETURNING id`,
		[]any{f.runID, issueID, agentID}, &f.nodeID)
	insert("task", `INSERT INTO agent_task_queue (agent_id, issue_id, status, runtime_id, workflow_node_id, workflow_claim_epoch) VALUES ($1, $2, 'running', $3, $4, 1) RETURNING id`,
		[]any{agentID, issueID, runtimeID, f.nodeID}, &f.taskID)
	insert("attempt", `INSERT INTO workflow_node_attempt (node_id, claim_epoch, task_id, runtime_id, daemon_id, status, lease_expires_at, started_at) VALUES ($1, 1, $2, $3, 'workflow-terminal-daemon', 'running', now() + interval '5 minutes', now()) RETURNING id`,
		[]any{f.nodeID, f.taskID, runtimeID}, &f.attemptID)
	for _, statement := range []struct {
		label string
		sql   string
		args  []any
	}{
		{"attach attempt to node", `UPDATE workflow_node SET current_attempt_id = $2 WHERE id = $1`, []any{f.nodeID, f.attemptID}},
		{"attach attempt to task", `UPDATE agent_task_queue SET workflow_attempt_id = $2 WHERE id = $1`, []any{f.taskID, f.attemptID}},
		{"claim resource", `INSERT INTO workflow_resource_claim (resource_key, node_id, attempt_id) VALUES ('book/test/passage', $1, $2)`, []any{f.nodeID, f.attemptID}},
		{"create pending supplement", `INSERT INTO task_supplement (task_id, workspace_id, issue_id, comment_id, author_id, client_request_id, status) VALUES ($1, $2, $3, gen_random_uuid(), $4, gen_random_uuid(), 'pending')`, []any{f.taskID, f.workspaceID, issueID, f.userID}},
	} {
		if _, err := pool.Exec(ctx, statement.sql, statement.args...); err != nil {
			t.Fatalf("%s: %v", statement.label, err)
		}
	}
	t.Cleanup(func() {
		cleanup := context.Background()
		for _, step := range []struct {
			sql  string
			args []any
		}{
			{`DELETE FROM task_supplement WHERE task_id = $1`, []any{f.taskID}},
			{`DELETE FROM workflow_resource_claim WHERE node_id = $1`, []any{f.nodeID}},
			{`DELETE FROM workflow_outbox WHERE run_id = $1`, []any{f.runID}},
			{`UPDATE workflow_node SET current_attempt_id = NULL WHERE id = $1`, []any{f.nodeID}},
			{`UPDATE agent_task_queue SET workflow_attempt_id = NULL, workflow_node_id = NULL WHERE id = $1`, []any{f.taskID}},
			{`DELETE FROM workflow_node_attempt WHERE id = $1`, []any{f.attemptID}},
			{`DELETE FROM agent_task_queue WHERE id = $1`, []any{f.taskID}},
			{`DELETE FROM workflow_node WHERE id = $1`, []any{f.nodeID}},
			{`DELETE FROM workflow_run WHERE id = $1`, []any{f.runID}},
			{`DELETE FROM agent WHERE id = $1`, []any{agentID}},
			{`DELETE FROM agent_runtime WHERE id = $1`, []any{runtimeID}},
			{`DELETE FROM issue WHERE id = $1`, []any{issueID}},
			{`DELETE FROM project WHERE id = $1`, []any{projectID}},
			{`DELETE FROM member WHERE workspace_id = $1`, []any{f.workspaceID}},
			{`DELETE FROM workspace WHERE id = $1`, []any{f.workspaceID}},
			{`DELETE FROM "user" WHERE id = $1`, []any{f.userID}},
		} {
			if _, err := pool.Exec(cleanup, step.sql, step.args...); err != nil {
				t.Errorf("workflow fixture cleanup failed: %v", err)
			}
		}
	})
	return f
}

func (f workflowTerminalFixture) assertTerminal(t *testing.T, taskStatus, attemptStatus, nodeState string) {
	t.Helper()
	var gotTask, gotAttempt, gotNode, supplementStatus, supplementReason string
	var claims int
	if err := f.pool.QueryRow(context.Background(), `
		SELECT task.status, attempt.status, node.state, supplement.status, supplement.failure_reason
		FROM agent_task_queue task
		JOIN workflow_node_attempt attempt ON attempt.id = task.workflow_attempt_id
		JOIN workflow_node node ON node.id = task.workflow_node_id
		JOIN task_supplement supplement ON supplement.task_id = task.id
		WHERE task.id = $1
	`, f.taskID).Scan(&gotTask, &gotAttempt, &gotNode, &supplementStatus, &supplementReason); err != nil {
		t.Fatal(err)
	}
	if err := f.pool.QueryRow(context.Background(), `SELECT count(*) FROM workflow_resource_claim WHERE node_id = $1`, f.nodeID).Scan(&claims); err != nil {
		t.Fatal(err)
	}
	if gotTask != taskStatus || gotAttempt != attemptStatus || gotNode != nodeState || supplementStatus != "failed" || supplementReason != "turn_ended" || claims != 0 {
		t.Fatalf("terminal state task=%s attempt=%s node=%s supplement=%s/%s claims=%d", gotTask, gotAttempt, gotNode, supplementStatus, supplementReason, claims)
	}
}

func TestWorkflowTaskCompletionRequiresSubmittedResultAndFailureReleasesClaim(t *testing.T) {
	f := seedWorkflowTerminalFixture(t)
	service := NewTaskService(db.New(f.pool), f.pool, nil, events.New())
	ctx := context.Background()
	if _, _, err := service.CompleteTaskWithTransition(ctx, util.MustParseUUID(f.taskID), nil, "", "", "", false, "", ""); err == nil {
		t.Fatal("workflow task completed without a submitted result")
	}
	failed, transitioned, err := service.FailTaskWithTransition(ctx, util.MustParseUUID(f.taskID), "agent failed", "", "", "", "", false, "", "")
	if err != nil || !transitioned || failed.Status != "failed" {
		t.Fatalf("fail workflow task = %+v transitioned=%t err=%v", failed, transitioned, err)
	}
	f.assertTerminal(t, "failed", "failed", "ready")
}

func TestWorkflowControllerTerminalPathsSettleSupplements(t *testing.T) {
	for _, action := range []string{"expire", "cancel-node", "cancel-run"} {
		t.Run(action, func(t *testing.T) {
			f := seedWorkflowTerminalFixture(t)
			service := NewWorkflowService(db.New(f.pool), f.pool)
			ctx := context.Background()
			switch action {
			case "expire":
				if _, err := f.pool.Exec(ctx, `UPDATE workflow_node_attempt SET lease_expires_at = now() - interval '1 minute' WHERE id = $1`, f.attemptID); err != nil {
					t.Fatal(err)
				}
				if _, err := service.ExpireLeases(ctx, 50); err != nil {
					t.Fatal(err)
				}
				f.assertTerminal(t, "failed", "expired", "ready")
			case "cancel-node":
				if _, err := service.CancelNode(ctx, util.MustParseUUID(f.runID), util.MustParseUUID(f.nodeID), f.userID); err != nil {
					t.Fatal(err)
				}
				f.assertTerminal(t, "cancelled", "cancelled", "cancelled")
			case "cancel-run":
				if _, err := service.CancelRun(ctx, util.MustParseUUID(f.workspaceID), util.MustParseUUID(f.runID), f.userID); err != nil {
					t.Fatal(err)
				}
				f.assertTerminal(t, "cancelled", "cancelled", "cancelled")
			}
		})
	}
}
