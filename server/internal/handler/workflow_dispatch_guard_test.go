package handler

import (
	"context"
	"testing"

	"github.com/multica-ai/multica/server/internal/testutil"
	"github.com/multica-ai/multica/server/internal/util"
)

func TestCommentCannotDispatchWorkflowManagedProductionAgent(t *testing.T) {
	ctx := context.Background()
	agentID := dbfx.Agent(t, "workflow-managed-comment-agent", testRuntimeID)
	projectID := dbfx.Project(t, "workflow-managed-comment-project")
	issueID := dbfx.Issue(t, "workflow-managed-comment-issue", testutil.Cols{
		"project_id":    projectID,
		"status":        "in_progress",
		"assignee_type": "agent",
		"assignee_id":   agentID,
	})
	var runID, nodeID string
	dbfx.QueryRow(t, `
		INSERT INTO workflow_run (
			workspace_id, project_id, anchor_issue_id, graph_key, graph_version,
			status, created_by
		)
		VALUES ($1, $2, $3, 'comment-guard', '1', 'running', $4)
		RETURNING id
	`, testWorkspaceID, projectID, issueID, testUserID).Scan(&runID)
	t.Cleanup(func() { _, _ = testPool.Exec(context.Background(), `DELETE FROM workflow_run WHERE id = $1`, runID) })
	dbfx.QueryRow(t, `
		INSERT INTO workflow_node (
			run_id, issue_id, passage_key, node_key, executor_kind,
			agent_id, state, output_contract
		)
		VALUES ($1, $2, 'passage', 'production', 'agent', $3, 'ready', '{}'::jsonb)
		RETURNING id
	`, runID, issueID, agentID).Scan(&nodeID)
	t.Cleanup(func() { _, _ = testPool.Exec(context.Background(), `DELETE FROM workflow_node WHERE id = $1`, nodeID) })

	issue, err := testHandler.Queries.GetIssue(ctx, util.MustParseUUID(issueID))
	if err != nil {
		t.Fatal(err)
	}
	agent, err := testHandler.Queries.GetAgent(ctx, util.MustParseUUID(agentID))
	if err != nil {
		t.Fatal(err)
	}
	commentID := dbfx.Comment(t, issueID, "@workflow-managed-comment-agent please run")
	results := testHandler.enqueueCommentAgentTriggers(ctx, issue, util.MustParseUUID(commentID), []commentAgentTrigger{{Agent: agent}})
	if result := results[agentID]; result.status != DispatchBlocked || result.reason != ReasonWorkflowManaged {
		t.Fatalf("comment dispatch = %+v, want blocked/workflow_managed", result)
	}
	var tasks int
	if err := testPool.QueryRow(ctx, `SELECT count(*) FROM agent_task_queue WHERE issue_id = $1 AND agent_id = $2`, issueID, agentID).Scan(&tasks); err != nil {
		t.Fatal(err)
	}
	if tasks != 0 {
		t.Fatalf("ordinary tasks created for workflow-managed agent = %d, want 0", tasks)
	}
}
