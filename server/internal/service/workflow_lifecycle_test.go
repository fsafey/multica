package service

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

func TestWorkflowGateEvidenceRunCompletionAndRunControl(t *testing.T) {
	ctx := context.Background()
	pool := newTaskClaimRacePool(t)
	var workflowSchemaPresent bool
	if err := pool.QueryRow(ctx,
		`SELECT to_regclass('public.workflow_node') IS NOT NULL`,
	).Scan(&workflowSchemaPresent); err != nil || !workflowSchemaPresent {
		t.Skip("workflow schema migrations are not present in the configured test database")
	}
	suffix := time.Now().UnixNano()

	var userID, workspaceID, projectID, issueID string
	if err := pool.QueryRow(ctx,
		`INSERT INTO "user" (name, email) VALUES ('Workflow Lifecycle Test', $1) RETURNING id`,
		fmt.Sprintf("workflow-lifecycle-%d@example.invalid", suffix),
	).Scan(&userID); err != nil {
		t.Fatalf("create user: %v", err)
	}
	if err := pool.QueryRow(ctx, `
		INSERT INTO workspace (name, slug, description, issue_prefix)
		VALUES ('Workflow Lifecycle Test', $1, '', 'WFL')
		RETURNING id
	`, fmt.Sprintf("workflow-lifecycle-%d", suffix)).Scan(&workspaceID); err != nil {
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
		VALUES ($1, 'Workflow Lifecycle Project', '', 'in_progress')
		RETURNING id
	`, workspaceID).Scan(&projectID); err != nil {
		t.Fatalf("create project: %v", err)
	}
	if err := pool.QueryRow(ctx, `
		INSERT INTO issue (
			workspace_id, project_id, title, status, priority,
			creator_id, creator_type, number, position
		)
		VALUES ($1, $2, 'Workflow Lifecycle Issue', 'in_progress', 'none',
		        $3,  'member', 990002, 1)
		RETURNING id
	`, workspaceID, projectID, userID).Scan(&issueID); err != nil {
		t.Fatalf("create issue: %v", err)
	}

	var runID, predecessorID, predecessorAttemptID, gateID, successorID, commentID string
	if err := pool.QueryRow(ctx, `
		INSERT INTO workflow_run (
			workspace_id, project_id, anchor_issue_id, graph_key, graph_version,
			status, wip_limit, human_gate_limit, created_by
		)
		VALUES ($1, $2, $3, 'lifecycle-test', '1', 'running', 4, 5, $4)
		RETURNING id
	`, workspaceID, projectID, issueID, userID).Scan(&runID); err != nil {
		t.Fatalf("create workflow run: %v", err)
	}
	if err := pool.QueryRow(ctx, `
		INSERT INTO workflow_node (
			run_id, issue_id, passage_key, node_key, executor_kind,
			state, claim_epoch, output_contract, completed_at
		)
		VALUES (
			$1, $2, 'passage', 'revision-05', 'deterministic',
			'completed', 1, '{}'::jsonb, now()
		)
		RETURNING id
	`, runID, issueID).Scan(&predecessorID); err != nil {
		t.Fatalf("create predecessor: %v", err)
	}
	if err := pool.QueryRow(ctx, `
		INSERT INTO workflow_node_attempt (
			node_id, claim_epoch, daemon_id, status, lease_expires_at,
			base_commit, result_commit, artifact_digest, manifest, completed_at
		)
		VALUES (
			$1, 1, 'fixture', 'integrated', now(),
			'commit-1', 'commit-1', 'sha256:predecessor', '{}'::jsonb, now()
		)
		RETURNING id
	`, predecessorID).Scan(&predecessorAttemptID); err != nil {
		t.Fatalf("create predecessor attempt: %v", err)
	}
	if _, err := pool.Exec(ctx,
		`UPDATE workflow_node SET current_attempt_id = $2 WHERE id = $1`,
		predecessorID,
		predecessorAttemptID,
	); err != nil {
		t.Fatalf("attach predecessor attempt: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO workflow_node_result (
			node_id, attempt_id, generation, claim_epoch, canonical_commit,
			artifact_digest, manifest
		)
		VALUES ($1, $2, 1, 1, 'commit-1', 'sha256:predecessor', '{}'::jsonb)
	`, predecessorID, predecessorAttemptID); err != nil {
		t.Fatalf("create predecessor result: %v", err)
	}
	gateContract := `{
		"operation":"comment_gate_v1",
		"gate":"gate-05",
		"controls":{
			"approved":"[GATE APPROVED] pass 05",
			"rejected":"[GATE REJECTED] pass 05"
		},
		"accepted_verdicts":["approved"]
	}`
	if err := pool.QueryRow(ctx, `
		INSERT INTO workflow_node (
			run_id, issue_id, passage_key, node_key, executor_kind,
			state, output_contract
		)
		VALUES ($1, $2, 'passage', 'gate-05', 'human_gate', 'waiting_human', $3)
		RETURNING id
	`, runID, issueID, gateContract).Scan(&gateID); err != nil {
		t.Fatalf("create gate: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO workflow_node_dependency (node_id, depends_on_node_id)
		VALUES ($1, $2)
	`, gateID, predecessorID); err != nil {
		t.Fatalf("create gate dependency: %v", err)
	}
	if err := pool.QueryRow(ctx, `
		INSERT INTO workflow_node (
			run_id, issue_id, passage_key, node_key, executor_kind,
			state, output_contract
		)
		VALUES ($1, $2, 'passage', 'resolution-06', 'deterministic', 'pending', '{}')
		RETURNING id
	`, runID, issueID).Scan(&successorID); err != nil {
		t.Fatalf("create successor: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO workflow_node_dependency (node_id, depends_on_node_id)
		VALUES ($1, $2)
	`, successorID, gateID); err != nil {
		t.Fatalf("create successor dependency: %v", err)
	}
	if err := pool.QueryRow(ctx, `
		INSERT INTO comment (
			workspace_id, issue_id, author_type, author_id, content
		)
		VALUES ($1, $2, 'member', $3, '[GATE APPROVED] pass 05')
		RETURNING id
	`, workspaceID, issueID, userID).Scan(&commentID); err != nil {
		t.Fatalf("create gate comment: %v", err)
	}

	t.Cleanup(func() {
		cleanup := context.Background()
		for _, statement := range []string{
			`DELETE FROM workflow_outbox WHERE run_id IN (SELECT id FROM workflow_run WHERE workspace_id = $1)`,
			`DELETE FROM workflow_resource_claim WHERE node_id IN (SELECT id FROM workflow_node WHERE run_id IN (SELECT id FROM workflow_run WHERE workspace_id = $1))`,
			`DELETE FROM workflow_node_result WHERE node_id IN (SELECT id FROM workflow_node WHERE run_id IN (SELECT id FROM workflow_run WHERE workspace_id = $1))`,
			`DELETE FROM workflow_node_attempt WHERE node_id IN (SELECT id FROM workflow_node WHERE run_id IN (SELECT id FROM workflow_run WHERE workspace_id = $1))`,
			`DELETE FROM workflow_node_resource WHERE node_id IN (SELECT id FROM workflow_node WHERE run_id IN (SELECT id FROM workflow_run WHERE workspace_id = $1))`,
			`DELETE FROM workflow_node_dependency WHERE node_id IN (SELECT id FROM workflow_node WHERE run_id IN (SELECT id FROM workflow_run WHERE workspace_id = $1)) OR depends_on_node_id IN (SELECT id FROM workflow_node WHERE run_id IN (SELECT id FROM workflow_run WHERE workspace_id = $1))`,
			`DELETE FROM agent_task_queue WHERE workflow_node_id IN (SELECT id FROM workflow_node WHERE run_id IN (SELECT id FROM workflow_run WHERE workspace_id = $1))`,
			`DELETE FROM workflow_node WHERE run_id IN (SELECT id FROM workflow_run WHERE workspace_id = $1)`,
			`DELETE FROM workflow_run WHERE workspace_id = $1`,
			`DELETE FROM comment WHERE workspace_id = $1`,
			`DELETE FROM issue WHERE workspace_id = $1`,
			`DELETE FROM project WHERE workspace_id = $1`,
			`DELETE FROM member WHERE workspace_id = $1`,
			`DELETE FROM workspace WHERE id = $1`,
		} {
			_, _ = pool.Exec(cleanup, statement, workspaceID)
		}
		_, _ = pool.Exec(cleanup, `DELETE FROM "user" WHERE id = $1`, userID)
	})

	service := NewWorkflowService(db.New(pool), pool)
	var rejectionID string
	if err := pool.QueryRow(ctx, `
		INSERT INTO comment (
			workspace_id, issue_id, author_type, author_id, content
		)
		VALUES ($1, $2, 'member', $3, '[GATE REJECTED] pass 05')
		RETURNING id
	`, workspaceID, issueID, userID).Scan(&rejectionID); err != nil {
		t.Fatalf("create newer gate rejection: %v", err)
	}
	if _, err := service.CompleteHumanGate(
		ctx,
		util.MustParseUUID(workspaceID),
		util.MustParseUUID(runID),
		util.MustParseUUID(gateID),
		util.MustParseUUID(commentID),
		"approved",
		userID,
	); err == nil {
		t.Fatal("stale gate approval was accepted after a newer rejection")
	}
	if _, err := pool.Exec(ctx, `DELETE FROM comment WHERE id = $1`, rejectionID); err != nil {
		t.Fatalf("remove gate rejection fixture: %v", err)
	}
	gate, err := service.CompleteHumanGate(
		ctx,
		util.MustParseUUID(workspaceID),
		util.MustParseUUID(runID),
		util.MustParseUUID(gateID),
		util.MustParseUUID(commentID),
		"approved",
		userID,
	)
	if err != nil {
		t.Fatalf("complete human gate: %v", err)
	}
	if gate.State != "completed" || gate.ClaimEpoch != 1 {
		t.Fatalf("gate state=%s epoch=%d, want completed epoch 1", gate.State, gate.ClaimEpoch)
	}
	result, err := db.New(pool).GetWorkflowNodeResult(ctx, util.MustParseUUID(gateID))
	if err != nil {
		t.Fatalf("load gate result: %v", err)
	}
	var manifest map[string]any
	if err := json.Unmarshal(result.Manifest, &manifest); err != nil {
		t.Fatalf("decode gate result manifest: %v", err)
	}
	if manifest["comment_id"] != commentID || manifest["verdict"] != "approved" {
		t.Fatalf("gate manifest = %#v", manifest)
	}
	if processed, err := service.ProcessOutbox(ctx, 10); err != nil || processed != 1 {
		t.Fatalf("process gate outbox: processed=%d err=%v", processed, err)
	}
	var successorState string
	var successorInput *string
	if err := pool.QueryRow(ctx,
		`SELECT state, input_digest FROM workflow_node WHERE id = $1`,
		successorID,
	).Scan(&successorState, &successorInput); err != nil {
		t.Fatalf("load successor: %v", err)
	}
	if successorState != "ready" || successorInput == nil || *successorInput == "" {
		t.Fatalf("successor state=%s input=%v", successorState, successorInput)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE workflow_node SET state = 'completed', completed_at = now()
		WHERE id = $1
	`, successorID); err != nil {
		t.Fatalf("complete successor fixture: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO workflow_outbox (run_id, node_id, event_type, payload)
		VALUES ($1, $2, 'workflow.node_accepted', '{}')
	`, runID, successorID); err != nil {
		t.Fatalf("enqueue terminal event: %v", err)
	}
	if processed, err := service.ProcessOutbox(ctx, 10); err != nil || processed != 1 {
		t.Fatalf("process terminal outbox: processed=%d err=%v", processed, err)
	}
	var runStatus string
	if err := pool.QueryRow(ctx,
		`SELECT status FROM workflow_run WHERE id = $1`,
		runID,
	).Scan(&runStatus); err != nil {
		t.Fatalf("load completed run: %v", err)
	}
	if runStatus != "completed" {
		t.Fatalf("run status=%s, want completed", runStatus)
	}

	var controlRunID, controlNodeID, controlAttemptID string
	if err := pool.QueryRow(ctx, `
		INSERT INTO workflow_run (
			workspace_id, project_id, anchor_issue_id, graph_key, graph_version,
			status, wip_limit, human_gate_limit, created_by
		)
		VALUES ($1, $2, $3, 'control-test', '1', 'running', 4, 5, $4)
		RETURNING id
	`, workspaceID, projectID, issueID, userID).Scan(&controlRunID); err != nil {
		t.Fatalf("create controlled run: %v", err)
	}
	if err := pool.QueryRow(ctx, `
		INSERT INTO workflow_node (
			run_id, issue_id, passage_key, node_key, executor_kind,
			state, claim_epoch, output_contract
		)
		VALUES ($1, $2, 'passage', 'active', 'deterministic', 'running', 1, '{}')
		RETURNING id
	`, controlRunID, issueID).Scan(&controlNodeID); err != nil {
		t.Fatalf("create controlled node: %v", err)
	}
	if err := pool.QueryRow(ctx, `
		INSERT INTO workflow_node_attempt (
			node_id, claim_epoch, daemon_id, status, lease_expires_at, manifest
		)
		VALUES ($1, 1, 'fixture', 'running', now() + interval '90 seconds', '{}')
		RETURNING id
	`, controlNodeID).Scan(&controlAttemptID); err != nil {
		t.Fatalf("create controlled attempt: %v", err)
	}
	if _, err := pool.Exec(ctx,
		`UPDATE workflow_node SET current_attempt_id = $2 WHERE id = $1`,
		controlNodeID,
		controlAttemptID,
	); err != nil {
		t.Fatalf("attach controlled attempt: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO workflow_resource_claim (resource_key, node_id, attempt_id)
		VALUES ('book/test/passage/control', $1, $2)
	`, controlNodeID, controlAttemptID); err != nil {
		t.Fatalf("create controlled resource: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO workflow_outbox (run_id, node_id, event_type, payload)
		VALUES ($1, $2, 'workflow.deterministic_ready', '{}')
	`, controlRunID, controlNodeID); err != nil {
		t.Fatalf("create controlled outbox: %v", err)
	}
	if paused, err := service.PauseRun(
		ctx,
		util.MustParseUUID(workspaceID),
		util.MustParseUUID(controlRunID),
		userID,
	); err != nil || paused.Status != "paused" {
		t.Fatalf("pause run: status=%s err=%v", paused.Status, err)
	}
	if resumed, err := service.ResumeRun(
		ctx,
		util.MustParseUUID(workspaceID),
		util.MustParseUUID(controlRunID),
		userID,
	); err != nil || resumed.Status != "running" {
		t.Fatalf("resume run: status=%s err=%v", resumed.Status, err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE workflow_outbox
		SET status = 'processing'
		WHERE run_id = $1 AND event_type = 'workflow.deterministic_ready'
	`, controlRunID); err != nil {
		t.Fatalf("mark integration processing: %v", err)
	}
	if _, err := service.CancelRun(
		ctx,
		util.MustParseUUID(workspaceID),
		util.MustParseUUID(controlRunID),
		userID,
	); err == nil {
		t.Fatal("cancel run accepted a processing integration job")
	}
	if _, err := pool.Exec(ctx, `
		UPDATE workflow_outbox
		SET status = 'pending'
		WHERE run_id = $1 AND event_type = 'workflow.deterministic_ready'
	`, controlRunID); err != nil {
		t.Fatalf("drain integration fixture: %v", err)
	}
	if cancelled, err := service.CancelRun(
		ctx,
		util.MustParseUUID(workspaceID),
		util.MustParseUUID(controlRunID),
		userID,
	); err != nil || cancelled.Status != "cancelled" {
		t.Fatalf("cancel run: status=%s err=%v", cancelled.Status, err)
	}
	var attemptStatus, nodeState string
	var resourceClaims, activeOutbox int
	if err := pool.QueryRow(ctx,
		`SELECT status FROM workflow_node_attempt WHERE id = $1`,
		controlAttemptID,
	).Scan(&attemptStatus); err != nil {
		t.Fatalf("load cancelled attempt: %v", err)
	}
	if err := pool.QueryRow(ctx,
		`SELECT state FROM workflow_node WHERE id = $1`,
		controlNodeID,
	).Scan(&nodeState); err != nil {
		t.Fatalf("load cancelled node: %v", err)
	}
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM workflow_resource_claim WHERE node_id = $1`,
		controlNodeID,
	).Scan(&resourceClaims); err != nil {
		t.Fatalf("count cancelled resources: %v", err)
	}
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM workflow_outbox
		WHERE run_id = $1 AND status IN ('pending', 'processing')
	`, controlRunID).Scan(&activeOutbox); err != nil {
		t.Fatalf("count active outbox: %v", err)
	}
	if attemptStatus != "cancelled" || nodeState != "cancelled" ||
		resourceClaims != 0 || activeOutbox != 0 {
		t.Fatalf(
			"cancelled attempt=%s node=%s resources=%d active_outbox=%d",
			attemptStatus,
			nodeState,
			resourceClaims,
			activeOutbox,
		)
	}
}
