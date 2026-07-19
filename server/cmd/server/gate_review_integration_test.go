package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/multica-ai/multica/server/internal/auth"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

func gateReviewBody(revision int) map[string]any {
	return map[string]any{
		"gate":           "P0",
		"revision":       revision,
		"subject_digest": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"review": map[string]any{
			"selected_source":  "Attachment att-123",
			"scope":            "Translate one supplied book",
			"defaults":         []string{"Standard publication profile"},
			"rights":           "Scholar supplied source",
			"uncertainties":    []string{"Page extent remains unknown"},
			"cost":             "No paid extraction authorized",
			"canonical_detail": map[string]any{"source": map[string]any{"attachment_id": "att-123"}},
		},
	}
}

func TestGateReviewDecisionCASAndExactlyOnceWake(t *testing.T) {
	agentID := getAgentID(t)
	issueID := createIssueAssignedToAgent(t, "Gate review decision integration", agentID)
	t.Cleanup(func() {
		clearTasks(t, issueID)
		_, _ = testPool.Exec(context.Background(), `DELETE FROM gate_decision_wake WHERE issue_id = $1`, issueID)
		_, _ = testPool.Exec(context.Background(), `DELETE FROM gate_review_decision WHERE issue_id = $1`, issueID)
		_, _ = testPool.Exec(context.Background(), `DELETE FROM gate_review_request WHERE issue_id = $1`, issueID)
		resp := authRequest(t, http.MethodDelete, "/api/issues/"+issueID, nil)
		resp.Body.Close()
	})
	clearTasks(t, issueID)

	create := func(revision int) string {
		resp := authRequest(t, http.MethodPost, "/api/issues/"+issueID+"/gate-reviews/", gateReviewBody(revision))
		if resp.StatusCode != http.StatusCreated {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			t.Fatalf("create gate review: got %d: %s", resp.StatusCode, body)
		}
		var body struct {
			ID string `json:"id"`
		}
		readJSON(t, resp, &body)
		return body.ID
	}
	request1 := create(1)
	requestRetry := authRequest(t, http.MethodPost, "/api/issues/"+issueID+"/gate-reviews/", gateReviewBody(1))
	if requestRetry.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(requestRetry.Body)
		requestRetry.Body.Close()
		t.Fatalf("identical request retry: got %d: %s", requestRetry.StatusCode, body)
	}
	var requestRetryBody struct {
		ID string `json:"id"`
	}
	readJSON(t, requestRetry, &requestRetryBody)
	if requestRetryBody.ID != request1 {
		t.Fatalf("identical request retry id=%s, want %s", requestRetryBody.ID, request1)
	}
	request2 := create(2)

	stale := authRequest(t, http.MethodPost, "/api/issues/"+issueID+"/gate-reviews/"+request1+"/decision", map[string]any{"outcome": "approved"})
	if stale.StatusCode != http.StatusConflict {
		body, _ := io.ReadAll(stale.Body)
		stale.Body.Close()
		t.Fatalf("stale decision: got %d: %s", stale.StatusCode, body)
	}
	stale.Body.Close()

	decidePath := "/api/issues/" + issueID + "/gate-reviews/" + request2 + "/decision"
	first := authRequest(t, http.MethodPost, decidePath, map[string]any{"outcome": "approved"})
	if first.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(first.Body)
		first.Body.Close()
		t.Fatalf("first decision: got %d: %s", first.StatusCode, body)
	}
	var firstBody struct {
		Decision struct {
			ID string `json:"id"`
		} `json:"decision"`
	}
	readJSON(t, first, &firstBody)

	retry := authRequest(t, http.MethodPost, decidePath, map[string]any{"outcome": "approved"})
	if retry.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(retry.Body)
		retry.Body.Close()
		t.Fatalf("identical retry: got %d: %s", retry.StatusCode, body)
	}
	retry.Body.Close()

	competing := authRequest(t, http.MethodPost, decidePath, map[string]any{"outcome": "changes_requested", "reason": "Different"})
	if competing.StatusCode != http.StatusConflict {
		body, _ := io.ReadAll(competing.Body)
		competing.Body.Close()
		t.Fatalf("competing decision: got %d: %s", competing.StatusCode, body)
	}
	competing.Body.Close()

	var decisions, tasks int
	if err := testPool.QueryRow(context.Background(), `SELECT count(*) FROM gate_review_decision WHERE request_id = $1`, request2).Scan(&decisions); err != nil {
		t.Fatal(err)
	}
	if err := testPool.QueryRow(context.Background(), `SELECT count(*) FROM agent_task_queue WHERE gate_decision_id = $1`, firstBody.Decision.ID).Scan(&tasks); err != nil {
		t.Fatal(err)
	}
	if decisions != 1 || tasks != 1 {
		t.Fatalf("decisions=%d tasks=%d, want exactly one each", decisions, tasks)
	}

	clearTasks(t, issueID)
	request3 := create(3)
	concurrentPath := "/api/issues/" + issueID + "/gate-reviews/" + request3 + "/decision"
	statuses := make(chan int, 2)
	postDecisionStatus := func() int {
		body, _ := json.Marshal(map[string]any{"outcome": "approved"})
		req, err := http.NewRequest(http.MethodPost, testServer.URL+concurrentPath, bytes.NewReader(body))
		if err != nil {
			return 0
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+testToken)
		req.Header.Set("X-Workspace-ID", testWorkspaceID)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return 0
		}
		defer resp.Body.Close()
		return resp.StatusCode
	}
	for range 2 {
		go func() {
			statuses <- postDecisionStatus()
		}()
	}
	for range 2 {
		status := <-statuses
		if status != http.StatusCreated && status != http.StatusOK {
			t.Fatalf("concurrent identical decision status=%d", status)
		}
	}
	if err := testPool.QueryRow(context.Background(), `SELECT count(*) FROM gate_review_decision WHERE request_id = $1`, request3).Scan(&decisions); err != nil {
		t.Fatal(err)
	}
	if err := testPool.QueryRow(context.Background(), `
		SELECT count(*) FROM agent_task_queue t
		JOIN gate_review_decision d ON d.id = t.gate_decision_id
		WHERE d.request_id = $1
	`, request3).Scan(&tasks); err != nil {
		t.Fatal(err)
	}
	if decisions != 1 || tasks != 1 {
		t.Fatalf("concurrent decisions=%d tasks=%d, want exactly one each", decisions, tasks)
	}
}

func TestGateDecisionWakeWorkerRecoversCommittedPendingWake(t *testing.T) {
	agentID := getAgentID(t)
	issueID := createIssueAssignedToAgent(t, "Gate wake crash recovery", agentID)
	t.Cleanup(func() {
		clearTasks(t, issueID)
		_, _ = testPool.Exec(context.Background(), `DELETE FROM gate_decision_wake WHERE issue_id = $1`, issueID)
		_, _ = testPool.Exec(context.Background(), `DELETE FROM gate_review_decision WHERE issue_id = $1`, issueID)
		_, _ = testPool.Exec(context.Background(), `DELETE FROM gate_review_request WHERE issue_id = $1`, issueID)
		resp := authRequest(t, http.MethodDelete, "/api/issues/"+issueID, nil)
		resp.Body.Close()
	})
	clearTasks(t, issueID)

	var requestID, decisionID string
	err := testPool.QueryRow(context.Background(), `
		WITH request AS (
			INSERT INTO gate_review_request (
				workspace_id, issue_id, gate, revision, subject_digest, review_data, actor_type, actor_id
			) VALUES ($1, $2, 'P0', 1, 'sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
				'{"selected_source":"att","scope":"book","defaults":[],"rights":"supplied","uncertainties":[],"cost":"none","canonical_detail":{"source":{"attachment_id":"att-123"}}}'::jsonb,
				'agent', $3)
			RETURNING id
		), decision AS (
			INSERT INTO gate_review_decision (workspace_id, issue_id, request_id, outcome, reason, actor_id)
			SELECT $1, $2, id, 'approved', '', $4 FROM request
			RETURNING id, request_id
		), wake AS (
			INSERT INTO gate_decision_wake (decision_id, workspace_id, issue_id)
			SELECT id, $1, $2 FROM decision
		)
		SELECT request_id::text, id::text FROM decision
	`, testWorkspaceID, issueID, agentID, testUserID).Scan(&requestID, &decisionID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := testPool.Exec(context.Background(), `UPDATE agent SET archived_at = now() WHERE id = $1`, agentID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = testPool.Exec(context.Background(), `UPDATE agent SET archived_at = NULL WHERE id = $1`, agentID)
	})
	worked, firstErr := testHandler.GateDecisionWakeWorker.ProcessNext(context.Background())
	if !worked || firstErr == nil {
		t.Fatalf("offline enqueue worked=%v err=%v, want durable pending failure", worked, firstErr)
	}
	var pendingState string
	var attempts int
	if err := testPool.QueryRow(context.Background(), `SELECT state, attempt_count FROM gate_decision_wake WHERE decision_id = $1`, decisionID).Scan(&pendingState, &attempts); err != nil {
		t.Fatal(err)
	}
	if pendingState != "pending" || attempts != 1 {
		t.Fatalf("failed enqueue state=%s attempts=%d, want pending/1", pendingState, attempts)
	}
	if _, err := testPool.Exec(context.Background(), `UPDATE agent SET archived_at = NULL WHERE id = $1`, agentID); err != nil {
		t.Fatal(err)
	}
	if _, err := testPool.Exec(context.Background(), `UPDATE gate_decision_wake SET next_attempt_at = now() WHERE decision_id = $1`, decisionID); err != nil {
		t.Fatal(err)
	}

	type workerResult struct {
		worked bool
		err    error
	}
	results := make(chan workerResult, 2)
	for range 2 {
		go func() {
			worked, processErr := testHandler.GateDecisionWakeWorker.ProcessNext(context.Background())
			results <- workerResult{worked: worked, err: processErr}
		}()
	}
	for range 2 {
		result := <-results
		if result.err != nil {
			t.Fatalf("concurrent ProcessNext worked=%v err=%v", result.worked, result.err)
		}
	}

	var state string
	var taskCount int
	if err := testPool.QueryRow(context.Background(), `SELECT state FROM gate_decision_wake WHERE decision_id = $1`, decisionID).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if err := testPool.QueryRow(context.Background(), `SELECT count(*) FROM agent_task_queue WHERE gate_decision_id = $1`, decisionID).Scan(&taskCount); err != nil {
		t.Fatal(err)
	}
	if state != "delivered" || taskCount != 1 {
		t.Fatalf("state=%s tasks=%d, want delivered and one task (request %s)", state, taskCount, requestID)
	}
}

func TestGateDecisionWakeWorkerDoesNotStarveBehindOrphan(t *testing.T) {
	agentID := getAgentID(t)
	issueID := createIssueAssignedToAgent(t, "Gate wake poison isolation", agentID)
	const orphanIssueID = "00000000-0000-0000-0000-00000000f434"
	clearTasks(t, issueID)

	insertWake := func(targetIssueID string, createdAt time.Time) (string, string) {
		t.Helper()
		var requestID, decisionID string
		err := testPool.QueryRow(context.Background(), `
			WITH request AS (
				INSERT INTO gate_review_request (
					workspace_id, issue_id, gate, revision, subject_digest, review_data,
					actor_type, actor_id, created_at
				) VALUES ($1, $2, 'P0', 1,
					'sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
					'{"selected_source":"att","scope":"book","defaults":[],"rights":"supplied","uncertainties":[],"cost":"none","canonical_detail":{}}'::jsonb,
					'agent', $3, $5)
				RETURNING id
			), decision AS (
				INSERT INTO gate_review_decision (
					workspace_id, issue_id, request_id, outcome, reason, actor_id, created_at
				) SELECT $1, $2, id, 'approved', '', $4, $5 FROM request
				RETURNING id, request_id
			), wake AS (
				INSERT INTO gate_decision_wake (decision_id, workspace_id, issue_id, created_at)
				SELECT id, $1, $2, $5 FROM decision
			)
			SELECT request_id::text, id::text FROM decision
		`, testWorkspaceID, targetIssueID, agentID, testUserID, createdAt).Scan(&requestID, &decisionID)
		if err != nil {
			t.Fatal(err)
		}
		return requestID, decisionID
	}
	orphanRequestID, orphanDecisionID := insertWake(orphanIssueID, time.Now().Add(-time.Minute))
	validRequestID, validDecisionID := insertWake(issueID, time.Now())
	t.Cleanup(func() {
		clearTasks(t, issueID)
		_, _ = testPool.Exec(context.Background(), `DELETE FROM gate_decision_wake WHERE decision_id IN ($1, $2)`, orphanDecisionID, validDecisionID)
		_, _ = testPool.Exec(context.Background(), `DELETE FROM gate_review_decision WHERE id IN ($1, $2)`, orphanDecisionID, validDecisionID)
		_, _ = testPool.Exec(context.Background(), `DELETE FROM gate_review_request WHERE id IN ($1, $2)`, orphanRequestID, validRequestID)
		resp := authRequest(t, http.MethodDelete, "/api/issues/"+issueID, nil)
		resp.Body.Close()
	})

	worked, err := testHandler.GateDecisionWakeWorker.ProcessNext(context.Background())
	if !worked || err == nil {
		t.Fatalf("orphan wake worked=%v err=%v, want recorded failure", worked, err)
	}
	var orphanAttempts int
	if err := testPool.QueryRow(context.Background(), `SELECT attempt_count FROM gate_decision_wake WHERE decision_id = $1`, orphanDecisionID).Scan(&orphanAttempts); err != nil {
		t.Fatal(err)
	}
	if orphanAttempts != 1 {
		t.Fatalf("orphan attempts=%d, want 1", orphanAttempts)
	}
	pending, err := testHandler.Queries.ListPendingGateDecisionWakesForIssue(
		context.Background(),
		db.ListPendingGateDecisionWakesForIssueParams{
			WorkspaceID: util.MustParseUUID(testWorkspaceID),
			IssueID:     util.MustParseUUID(orphanIssueID),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 0 {
		t.Fatalf("issue-scoped reconcile bypassed wake backoff: got %d pending", len(pending))
	}

	worked, err = testHandler.GateDecisionWakeWorker.ProcessNext(context.Background())
	if !worked || err != nil {
		t.Fatalf("valid wake behind orphan worked=%v err=%v", worked, err)
	}
	var state string
	if err := testPool.QueryRow(context.Background(), `SELECT state FROM gate_decision_wake WHERE decision_id = $1`, validDecisionID).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "delivered" {
		t.Fatalf("valid wake state=%s, want delivered", state)
	}
}

func TestDeleteIssueCleansGateReviewRows(t *testing.T) {
	agentID := getAgentID(t)
	issueID := createIssueAssignedToAgent(t, "Gate review delete cleanup", agentID)
	clearTasks(t, issueID)
	t.Cleanup(func() {
		clearTasks(t, issueID)
		resp := authRequest(t, http.MethodDelete, "/api/issues/"+issueID, nil)
		resp.Body.Close()
	})

	created := authRequest(t, http.MethodPost, "/api/issues/"+issueID+"/gate-reviews/", gateReviewBody(1))
	if created.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(created.Body)
		created.Body.Close()
		t.Fatalf("create gate review: got %d: %s", created.StatusCode, body)
	}
	var createdBody struct {
		ID string `json:"id"`
	}
	readJSON(t, created, &createdBody)
	decided := authRequest(t, http.MethodPost, "/api/issues/"+issueID+"/gate-reviews/"+createdBody.ID+"/decision", map[string]any{"outcome": "approved"})
	if decided.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(decided.Body)
		decided.Body.Close()
		t.Fatalf("decide gate review: got %d: %s", decided.StatusCode, body)
	}
	decided.Body.Close()

	deleted := authRequest(t, http.MethodDelete, "/api/issues/"+issueID, nil)
	if deleted.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(deleted.Body)
		deleted.Body.Close()
		t.Fatalf("delete issue: got %d: %s", deleted.StatusCode, body)
	}
	deleted.Body.Close()
	for table, query := range map[string]string{
		"requests":  `SELECT count(*) FROM gate_review_request WHERE issue_id = $1`,
		"decisions": `SELECT count(*) FROM gate_review_decision WHERE issue_id = $1`,
		"wakes":     `SELECT count(*) FROM gate_decision_wake WHERE issue_id = $1`,
	} {
		var count int
		if err := testPool.QueryRow(context.Background(), query, issueID).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("delete left %d %s", count, table)
		}
	}
}

func TestTaskTokenCanRequestReviewButCannotPostHumanGateControls(t *testing.T) {
	agentID := getAgentID(t)
	issueID := createIssueAssignedToAgent(t, "Raw task token gate control", agentID)
	t.Cleanup(func() {
		clearTasks(t, issueID)
		resp := authRequest(t, http.MethodDelete, "/api/issues/"+issueID, nil)
		resp.Body.Close()
	})
	clearTasks(t, issueID)
	taskID := ensureAgentTask(t, agentID)
	rawToken, err := auth.GenerateAgentTaskToken()
	if err != nil {
		t.Fatal(err)
	}
	_, err = testPool.Exec(context.Background(), `
		INSERT INTO task_token (token_hash, task_id, agent_id, workspace_id, user_id, expires_at)
		VALUES ($1, $2, $3, $4, $5, $6)
	`, auth.HashToken(rawToken), taskID, agentID, testWorkspaceID, testUserID, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = testPool.Exec(context.Background(), `DELETE FROM task_token WHERE token_hash = $1`, auth.HashToken(rawToken))
	})

	body, _ := json.Marshal(map[string]any{"content": `[GATE APPROVED] pass P0`, "type": "comment"})
	req, _ := http.NewRequest(http.MethodPost, testServer.URL+"/api/issues/"+issueID+"/comments", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+rawToken)
	req.Header.Set("X-Workspace-ID", testWorkspaceID)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		got, _ := io.ReadAll(resp.Body)
		t.Fatalf("task-token reserved control got %d: %s", resp.StatusCode, got)
	}
	var comments int
	if err := testPool.QueryRow(context.Background(), `SELECT count(*) FROM comment WHERE issue_id = $1 AND content LIKE '[GATE APPROVED]%'`, issueID).Scan(&comments); err != nil {
		t.Fatal(err)
	}
	if comments != 0 {
		t.Fatalf("reserved control persisted %d comments", comments)
	}

	gateJSON, _ := json.Marshal(gateReviewBody(1))
	gateReq, _ := http.NewRequest(http.MethodPost, testServer.URL+"/api/issues/"+issueID+"/gate-reviews/", bytes.NewReader(gateJSON))
	gateReq.Header.Set("Authorization", "Bearer "+rawToken)
	gateReq.Header.Set("X-Workspace-ID", testWorkspaceID)
	gateReq.Header.Set("Content-Type", "application/json")
	gate, err := http.DefaultClient.Do(gateReq)
	if err != nil {
		t.Fatal(err)
	}
	if gate.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(gate.Body)
		gate.Body.Close()
		t.Fatalf("task-token create gate review: got %d: %s", gate.StatusCode, body)
	}
	var gateBody struct {
		ID        string `json:"id"`
		ActorType string `json:"actor_type"`
		ActorID   string `json:"actor_id"`
	}
	readJSON(t, gate, &gateBody)
	if gateBody.ActorType != "agent" || gateBody.ActorID != agentID {
		t.Fatalf("task-token review actor=(%s,%s), want (agent,%s)", gateBody.ActorType, gateBody.ActorID, agentID)
	}
	var storedActorType, storedActorID string
	if err := testPool.QueryRow(context.Background(), `SELECT actor_type, actor_id::text FROM gate_review_request WHERE id = $1`, gateBody.ID).Scan(&storedActorType, &storedActorID); err != nil {
		t.Fatal(err)
	}
	if storedActorType != "agent" || storedActorID != agentID {
		t.Fatalf("stored task-token review actor=(%s,%s), want (agent,%s)", storedActorType, storedActorID, agentID)
	}
	decisionBody, _ := json.Marshal(map[string]any{"outcome": "approved"})
	decisionReq, _ := http.NewRequest(http.MethodPost, testServer.URL+"/api/issues/"+issueID+"/gate-reviews/"+gateBody.ID+"/decision", bytes.NewReader(decisionBody))
	decisionReq.Header.Set("Authorization", "Bearer "+rawToken)
	decisionReq.Header.Set("X-Workspace-ID", testWorkspaceID)
	decisionReq.Header.Set("Content-Type", "application/json")
	decisionResp, err := http.DefaultClient.Do(decisionReq)
	if err != nil {
		t.Fatal(err)
	}
	defer decisionResp.Body.Close()
	if decisionResp.StatusCode != http.StatusForbidden {
		got, _ := io.ReadAll(decisionResp.Body)
		t.Fatalf("task-token decision got %d: %s", decisionResp.StatusCode, got)
	}
}
