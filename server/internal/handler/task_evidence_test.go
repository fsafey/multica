package handler

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/multica-ai/multica/server/internal/middleware"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/executionevidence"
)

func createRunningEvidenceTask(t *testing.T) (runtimeID, agentID, issueID, taskID string) {
	t.Helper()
	ctx := context.Background()
	runtimeID = createClaimReclaimRuntime(t, ctx, "execution evidence runtime")
	agentID, issueID = createClaimReclaimAgentAndIssue(t, ctx, runtimeID, "execution evidence")
	if err := testPool.QueryRow(ctx, `
		INSERT INTO agent_task_queue (
			agent_id, runtime_id, issue_id, status, priority, dispatched_at, started_at
		)
		VALUES ($1, $2, $3, 'running', 0, now(), now())
		RETURNING id
	`, agentID, runtimeID, issueID).Scan(&taskID); err != nil {
		t.Fatalf("create running evidence task: %v", err)
	}
	t.Cleanup(func() {
		_, _ = testPool.Exec(context.Background(), `DELETE FROM agent_task_queue WHERE id = $1`, taskID)
	})
	return runtimeID, agentID, issueID, taskID
}

func recordEvidenceRequest(t *testing.T, taskID string, snapshot executionevidence.Snapshot) *httptest.ResponseRecorder {
	t.Helper()
	digest, err := snapshot.Digest()
	if err != nil {
		t.Fatalf("digest snapshot: %v", err)
	}
	w := httptest.NewRecorder()
	req := newDaemonTokenRequest(http.MethodPost, "/api/daemon/tasks/"+taskID+"/evidence", map[string]any{
		"snapshot":     snapshot,
		"payload_hash": digest,
	}, testWorkspaceID, "evidence-test-daemon")
	req = withURLParam(req, "taskId", taskID)
	testHandler.RecordTaskExecutionEvidence(w, req)
	return w
}

func TestTaskExecutionEvidenceFutureSchemaReturnsUpgradeError(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	_, _, _, taskID := createRunningEvidenceTask(t)
	w := httptest.NewRecorder()
	req := newDaemonTokenRequest(http.MethodPost, "/api/daemon/tasks/"+taskID+"/evidence", map[string]any{
		"snapshot": map[string]any{
			"schema_version": executionevidence.CurrentSchemaVersion + 1,
			"future_field":   "future-value",
		},
		"payload_hash": "sha256:" + strings.Repeat("a", 64),
	}, testWorkspaceID, "future-evidence-daemon")
	req = withURLParam(req, "taskId", taskID)
	testHandler.RecordTaskExecutionEvidence(w, req)
	if w.Code != http.StatusBadRequest || !strings.Contains(w.Body.String(), "upgrade the server") {
		t.Fatalf("future evidence schema response = %d: %s", w.Code, w.Body.String())
	}
}

func TestTaskExecutionEvidenceRejectsOversizedBody(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	_, _, _, taskID := createRunningEvidenceTask(t)
	w := httptest.NewRecorder()
	req := newDaemonTokenRequest(
		http.MethodPost,
		"/api/daemon/tasks/"+taskID+"/evidence",
		nil,
		testWorkspaceID,
		"oversized-evidence-daemon",
	)
	req.Body = io.NopCloser(strings.NewReader(strings.Repeat("x", maxTaskExecutionEvidenceBodyBytes+1)))
	req = withURLParam(req, "taskId", taskID)
	testHandler.RecordTaskExecutionEvidence(w, req)
	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized evidence response = %d: %s", w.Code, w.Body.String())
	}
}

func TestTaskExecutionEvidenceEndToEnd(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	runtimeID, agentID, _, taskID := createRunningEvidenceTask(t)
	snapshot := executionevidence.Snapshot{
		SchemaVersion:          executionevidence.CurrentSchemaVersion,
		TaskID:                 taskID,
		Provider:               "handler_test_runtime",
		InvocationModel:        "claude-sonnet-5",
		InvocationModelSource:  "agent",
		ProviderCLIVersion:     "2.1.0",
		MulticaCLIVersion:      "v0.4.3",
		MulticaGitCommit:       "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		AgentID:                agentID,
		RuntimeID:              runtimeID,
		WorkspaceID:            testWorkspaceID,
		Instructions:           "exact instructions",
		WorkspaceContext:       "workspace context",
		MountedSkills:          []executionevidence.MountedSkill{},
		CustomArguments:        []string{},
		CustomEnvironmentNames: []string{"ANTHROPIC_API_KEY"},
		MCPServerNames:         []string{},
	}

	if w := recordEvidenceRequest(t, taskID, snapshot); w.Code != http.StatusCreated {
		t.Fatalf("first evidence write: expected 201, got %d: %s", w.Code, w.Body.String())
	}
	if w := recordEvidenceRequest(t, taskID, snapshot); w.Code != http.StatusOK {
		t.Fatalf("identical evidence replay: expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if _, err := testPool.Exec(context.Background(), `UPDATE task_execution_evidence SET payload_hash = payload_hash WHERE task_id = $1`, taskID); err == nil {
		t.Fatal("database allowed an execution evidence row to be updated")
	}
	conflict := snapshot
	conflict.Instructions = "different instructions"
	if w := recordEvidenceRequest(t, taskID, conflict); w.Code != http.StatusConflict {
		t.Fatalf("conflicting evidence replay: expected 409, got %d: %s", w.Code, w.Body.String())
	}

	reportMessages := func(messages []TaskMessageRequest) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		req := newDaemonTokenRequest(http.MethodPost, "/api/daemon/tasks/"+taskID+"/messages", TaskMessageBatchRequest{Messages: messages}, testWorkspaceID, "evidence-test-daemon")
		req = withURLParam(req, "taskId", taskID)
		testHandler.ReportTaskMessages(w, req)
		return w
	}
	messages := []TaskMessageRequest{
		{Seq: 1, Type: "text", Content: "starting"},
		{Seq: 3, Type: "result", Content: "finished"},
	}
	if w := reportMessages(messages); w.Code != http.StatusOK {
		t.Fatalf("initial message batch: %d: %s", w.Code, w.Body.String())
	}
	if _, err := testPool.Exec(context.Background(), `INSERT INTO task_message (task_id, seq, type) VALUES ($1, 1, 'text')`, taskID); err == nil {
		t.Fatal("database allowed a duplicate (task_id, seq) message")
	}
	if w := reportMessages(messages[:1]); w.Code != http.StatusOK {
		t.Fatalf("identical message replay: %d: %s", w.Code, w.Body.String())
	}
	const concurrentReplays = 8
	start := make(chan struct{})
	responses := make(chan *httptest.ResponseRecorder, concurrentReplays)
	var replayWG sync.WaitGroup
	for range concurrentReplays {
		replayWG.Add(1)
		go func() {
			defer replayWG.Done()
			<-start
			responses <- reportMessages([]TaskMessageRequest{{
				Seq: 4, Type: "tool_use", Tool: "exec_command", Content: "concurrent replay",
				Input: map[string]any{"zeta": "last", "nested": map[string]any{"alpha": "first"}},
			}})
		}()
	}
	close(start)
	replayWG.Wait()
	close(responses)
	for response := range responses {
		if response.Code != http.StatusOK {
			t.Fatalf("concurrent identical message replay: %d: %s", response.Code, response.Body.String())
		}
	}
	if w := reportMessages([]TaskMessageRequest{
		{Seq: 1, Type: "text", Content: "conflict"},
		{Seq: 5, Type: "text", Content: "preserved after conflict"},
	}); w.Code != http.StatusConflict {
		t.Fatalf("conflicting message replay: expected 409, got %d: %s", w.Code, w.Body.String())
	}
	if _, err := testHandler.Queries.GetTaskMessageBySequence(context.Background(), db.GetTaskMessageBySequenceParams{
		TaskID: parseUUID(taskID), Seq: 5,
	}); err != nil {
		t.Fatalf("message after conflicting sequence was not preserved: %v", err)
	}

	ctx := context.Background()
	if err := testHandler.Queries.UpsertTaskUsage(ctx, db.UpsertTaskUsageParams{
		TaskID: parseUUID(taskID), Provider: "claude", Model: "claude-sonnet-5", InputTokens: 10, OutputTokens: 5,
	}); err != nil {
		t.Fatalf("insert task usage: %v", err)
	}
	absoluteWorkDir := "/Users/private/multica_workspaces/" + testWorkspaceID + "/" + shortTaskID(taskID) + "/workdir"
	if _, err := testPool.Exec(ctx, `
		UPDATE agent_task_queue
		SET status = 'completed',
		    completed_at = now(),
		    work_dir = $2,
		    transcript_expected_message_count = 4,
		    transcript_expected_last_seq = 5,
		    transcript_delivery_confirmed = true
		WHERE id = $1
	`, taskID, absoluteWorkDir); err != nil {
		t.Fatalf("complete task: %v", err)
	}

	member, err := testHandler.Queries.GetMemberByUserAndWorkspace(ctx, db.GetMemberByUserAndWorkspaceParams{
		UserID: parseUUID(testUserID), WorkspaceID: parseUUID(testWorkspaceID),
	})
	if err != nil {
		t.Fatalf("load member: %v", err)
	}
	w := httptest.NewRecorder()
	req := newRequest(http.MethodGet, "/api/tasks/"+taskID+"/evidence", nil)
	req = withURLParam(req, "taskId", taskID)
	req = req.WithContext(middleware.SetMemberContext(req.Context(), testWorkspaceID, member))
	testHandler.GetTaskEvidence(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("get task evidence: %d: %s", w.Code, w.Body.String())
	}
	var response TaskEvidenceResponse
	if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
		t.Fatalf("decode task evidence: %v", err)
	}
	if response.Complete || !response.ExecutionSnapshotIntegrity || response.SequenceIntegrity.Valid {
		t.Fatalf("gap-bearing evidence completeness = %#v", response)
	}
	if len(response.Messages) != 4 || len(response.PerModelUsage) != 1 {
		t.Fatalf("messages/usage = %d/%d", len(response.Messages), len(response.PerModelUsage))
	}
	if len(response.SequenceIntegrity.Gaps) != 1 {
		t.Fatalf("sequence gaps = %#v", response.SequenceIntegrity.Gaps)
	}
	if !slices.Contains(response.CompletenessIssues, "message_sequence_gap") {
		t.Fatalf("completeness issues = %#v", response.CompletenessIssues)
	}
	if response.Task.ExpectedMessageCount == nil || *response.Task.ExpectedMessageCount != 4 ||
		response.Task.ExpectedLastSequence == nil || *response.Task.ExpectedLastSequence != 5 ||
		response.Task.TranscriptDeliveryConfirmed == nil || !*response.Task.TranscriptDeliveryConfirmed {
		t.Fatalf("terminal transcript expectation = %#v", response.Task)
	}
	if response.ExecutionSnapshotDigest == "" || response.EvidenceManifestDigest == "" {
		t.Fatalf("evidence digests are missing: %#v", response)
	}
	if response.Task.WorkDir != testWorkspaceID+"/"+shortTaskID(taskID)+"/workdir" {
		t.Fatalf("privacy-safe workdir = %q", response.Task.WorkDir)
	}

	originalManifestDigest := response.EvidenceManifestDigest
	if _, err := testPool.Exec(ctx, `UPDATE agent_task_queue SET transcript_delivery_confirmed = false WHERE id = $1`, taskID); err != nil {
		t.Fatalf("mark transcript delivery unconfirmed: %v", err)
	}
	w = httptest.NewRecorder()
	req = newRequest(http.MethodGet, "/api/tasks/"+taskID+"/evidence", nil)
	req = withURLParam(req, "taskId", taskID)
	req = req.WithContext(middleware.SetMemberContext(req.Context(), testWorkspaceID, member))
	testHandler.GetTaskEvidence(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("get delivery-failed task evidence: %d: %s", w.Code, w.Body.String())
	}
	if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
		t.Fatalf("decode delivery-failed task evidence: %v", err)
	}
	if response.Complete || !slices.Contains(response.CompletenessIssues, "message_delivery") {
		t.Fatalf("unacknowledged transcript completeness = %#v", response)
	}
	if response.EvidenceManifestDigest == originalManifestDigest {
		t.Fatal("evidence manifest digest did not cover transcript delivery state")
	}
}

func TestCompletedTaskCannotBeBackfilledAsNativeEvidence(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	runtimeID, agentID, _, taskID := createRunningEvidenceTask(t)
	if _, err := testPool.Exec(context.Background(), `UPDATE agent_task_queue SET status = 'completed', completed_at = now() WHERE id = $1`, taskID); err != nil {
		t.Fatalf("complete historical task fixture: %v", err)
	}
	snapshot := executionevidence.Snapshot{
		SchemaVersion:          executionevidence.CurrentSchemaVersion,
		TaskID:                 taskID,
		Provider:               "handler_test_runtime",
		InvocationModel:        "claude-sonnet-5",
		InvocationModelSource:  "agent",
		MulticaCLIVersion:      "v0.4.3",
		MulticaGitCommit:       "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		AgentID:                agentID,
		RuntimeID:              runtimeID,
		WorkspaceID:            testWorkspaceID,
		MountedSkills:          []executionevidence.MountedSkill{},
		CustomArguments:        []string{},
		CustomEnvironmentNames: []string{},
		MCPServerNames:         []string{},
	}
	if w := recordEvidenceRequest(t, taskID, snapshot); w.Code != http.StatusConflict {
		t.Fatalf("historical evidence backfill: expected 409, got %d: %s", w.Code, w.Body.String())
	}
	var count int
	if err := testPool.QueryRow(context.Background(), `SELECT COUNT(*) FROM task_execution_evidence WHERE task_id = $1`, taskID).Scan(&count); err != nil {
		t.Fatalf("count evidence rows: %v", err)
	}
	if count != 0 {
		t.Fatalf("historical evidence rows = %d, want 0", count)
	}
}

func TestTaskTranscriptExpectationIsImmutableAndIdempotent(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	_, _, _, taskID := createRunningEvidenceTask(t)
	expected := 3
	delivered := true
	request := httptest.NewRequest(http.MethodPost, "/", nil)
	if ok := testHandler.recordTaskTranscriptExpectation(
		httptest.NewRecorder(), request, taskID, &expected, &expected, &delivered,
	); !ok {
		t.Fatal("initial transcript expectation was rejected")
	}
	if ok := testHandler.recordTaskTranscriptExpectation(
		httptest.NewRecorder(), request, taskID, &expected, &expected, &delivered,
	); !ok {
		t.Fatal("identical transcript expectation replay was rejected")
	}
	conflicting := 4
	recorder := httptest.NewRecorder()
	if ok := testHandler.recordTaskTranscriptExpectation(
		recorder, request, taskID, &conflicting, &conflicting, &delivered,
	); ok || recorder.Code != http.StatusConflict {
		t.Fatalf("conflicting transcript expectation = %t/%d", ok, recorder.Code)
	}
	task, err := testHandler.Queries.GetAgentTask(context.Background(), parseUUID(taskID))
	if err != nil {
		t.Fatalf("read task expectation: %v", err)
	}
	if !task.TranscriptExpectedMessageCount.Valid || task.TranscriptExpectedMessageCount.Int32 != 3 ||
		!task.TranscriptExpectedLastSeq.Valid || task.TranscriptExpectedLastSeq.Int32 != 3 ||
		!task.TranscriptDeliveryConfirmed.Valid || !task.TranscriptDeliveryConfirmed.Bool {
		t.Fatalf("stored transcript expectation = %#v", task)
	}
}

func TestTaskMessageArrivalOrderRemainsVisibleToIntegrityCheck(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	_, _, _, taskID := createRunningEvidenceTask(t)
	if _, err := testPool.Exec(context.Background(), `
		INSERT INTO task_message (task_id, seq, type, created_at)
		VALUES ($1, 2, 'text', now() - interval '1 second'),
		       ($1, 1, 'text', now())
	`, taskID); err != nil {
		t.Fatalf("insert out-of-order transcript: %v", err)
	}
	sequences, err := testHandler.Queries.ListTaskMessageSequencesByArrival(context.Background(), parseUUID(taskID))
	if err != nil {
		t.Fatalf("list transcript arrival order: %v", err)
	}
	if !slices.Equal(sequences, []int32{2, 1}) {
		t.Fatalf("arrival sequences = %#v, want [2 1]", sequences)
	}
	if integrity := executionevidence.CheckSequenceIntegrity(sequences); integrity.Valid {
		t.Fatalf("out-of-order transcript considered valid: %#v", integrity)
	}
}

func TestIssueProjectMoveDoesNotReplaceClaimTimeEvidence(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	runtimeID, agentID, issueID, taskID := createRunningEvidenceTask(t)
	ctx := context.Background()
	var claimProjectID, currentProjectID string
	if err := testPool.QueryRow(ctx, `
		INSERT INTO project (workspace_id, title) VALUES ($1, 'claim-time evidence project') RETURNING id
	`, testWorkspaceID).Scan(&claimProjectID); err != nil {
		t.Fatalf("create claim-time project: %v", err)
	}
	if err := testPool.QueryRow(ctx, `
		INSERT INTO project (workspace_id, title) VALUES ($1, 'current evidence project') RETURNING id
	`, testWorkspaceID).Scan(&currentProjectID); err != nil {
		t.Fatalf("create current project: %v", err)
	}
	if _, err := testPool.Exec(ctx, `UPDATE issue SET project_id = $2 WHERE id = $1`, issueID, claimProjectID); err != nil {
		t.Fatalf("set claim-time project: %v", err)
	}
	if _, err := testPool.Exec(ctx, `UPDATE issue SET project_id = $2 WHERE id = $1`, issueID, currentProjectID); err != nil {
		t.Fatalf("move issue after claim: %v", err)
	}

	snapshot := executionevidence.Snapshot{
		SchemaVersion:          executionevidence.CurrentSchemaVersion,
		TaskID:                 taskID,
		Provider:               "handler_test_runtime",
		InvocationModel:        "claude-sonnet-5",
		InvocationModelSource:  "agent",
		ProviderCLIVersion:     "2.1.0",
		MulticaCLIVersion:      "v0.4.3",
		MulticaGitCommit:       "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		AgentID:                agentID,
		RuntimeID:              runtimeID,
		WorkspaceID:            testWorkspaceID,
		ProjectID:              claimProjectID,
		MountedSkills:          []executionevidence.MountedSkill{},
		CustomArguments:        []string{},
		CustomEnvironmentNames: []string{},
		MCPServerNames:         []string{},
	}
	snapshot.ProjectID = "11111111-1111-4111-8111-111111111111"
	if w := recordEvidenceRequest(t, taskID, snapshot); w.Code != http.StatusBadRequest || !strings.Contains(w.Body.String(), "project does not match") {
		t.Fatalf("projectless run-only evidence with a project = %d: %s", w.Code, w.Body.String())
	}
	snapshot.ProjectID = ""
	if w := recordEvidenceRequest(t, taskID, snapshot); w.Code != http.StatusCreated {
		t.Fatalf("claim-time project evidence: expected 201, got %d: %s", w.Code, w.Body.String())
	}
	row, err := testHandler.Queries.GetTaskExecutionEvidence(ctx, parseUUID(taskID))
	if err != nil {
		t.Fatalf("read claim-time project evidence: %v", err)
	}
	var stored executionevidence.Snapshot
	if err := json.Unmarshal(row.Payload, &stored); err != nil {
		t.Fatalf("decode claim-time project evidence: %v", err)
	}
	if stored.ProjectID != claimProjectID {
		t.Fatalf("stored project = %q, want claim-time %q", stored.ProjectID, claimProjectID)
	}
}

func TestUnreadableTaskEvidenceIsDistinctFromMissingEvidence(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	_, _, _, taskID := createRunningEvidenceTask(t)
	if _, err := testPool.Exec(context.Background(), `
		INSERT INTO task_execution_evidence (task_id, schema_version, payload, payload_hash)
		VALUES ($1, 1, convert_to('{"schema_version":"invalid"}', 'UTF8'), 'sha256:' || repeat('a', 64))
	`, taskID); err != nil {
		t.Fatalf("insert unreadable evidence fixture: %v", err)
	}

	ctx := context.Background()
	member, err := testHandler.Queries.GetMemberByUserAndWorkspace(ctx, db.GetMemberByUserAndWorkspaceParams{
		UserID: parseUUID(testUserID), WorkspaceID: parseUUID(testWorkspaceID),
	})
	if err != nil {
		t.Fatalf("load member: %v", err)
	}
	w := httptest.NewRecorder()
	req := newRequest(http.MethodGet, "/api/tasks/"+taskID+"/evidence", nil)
	req = withURLParam(req, "taskId", taskID)
	req = req.WithContext(middleware.SetMemberContext(req.Context(), testWorkspaceID, member))
	testHandler.GetTaskEvidence(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("get unreadable task evidence: %d: %s", w.Code, w.Body.String())
	}
	var response TaskEvidenceResponse
	if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
		t.Fatalf("decode unreadable task evidence: %v", err)
	}
	if response.ExecutionSnapshot != nil || response.ExecutionSnapshotIntegrity || response.Complete {
		t.Fatalf("unreadable evidence was treated as usable: %#v", response)
	}
	foundUnreadable := false
	for _, issue := range response.CompletenessIssues {
		if issue == "execution_snapshot" {
			t.Fatalf("unreadable evidence collapsed into missing evidence: %#v", response.CompletenessIssues)
		}
		if issue == "execution_snapshot_unreadable" {
			foundUnreadable = true
		}
	}
	if !foundUnreadable {
		t.Fatalf("missing unreadable evidence issue: %#v", response.CompletenessIssues)
	}
}

func TestIssueDeletionRemovesTaskExecutionEvidence(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	runtimeID, agentID, issueID, taskID := createRunningEvidenceTask(t)
	snapshot := executionevidence.Snapshot{
		SchemaVersion:          executionevidence.CurrentSchemaVersion,
		TaskID:                 taskID,
		Provider:               "handler_test_runtime",
		InvocationModel:        "claude-sonnet-5",
		InvocationModelSource:  "agent",
		ProviderCLIVersion:     "2.1.0",
		MulticaCLIVersion:      "v0.4.3",
		MulticaGitCommit:       "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		AgentID:                agentID,
		RuntimeID:              runtimeID,
		WorkspaceID:            testWorkspaceID,
		MountedSkills:          []executionevidence.MountedSkill{},
		CustomArguments:        []string{},
		CustomEnvironmentNames: []string{},
		MCPServerNames:         []string{},
	}
	if w := recordEvidenceRequest(t, taskID, snapshot); w.Code != http.StatusCreated {
		t.Fatalf("evidence write: expected 201, got %d: %s", w.Code, w.Body.String())
	}
	if err := testHandler.Queries.DeleteIssue(context.Background(), db.DeleteIssueParams{
		ID: parseUUID(issueID), WorkspaceID: parseUUID(testWorkspaceID),
	}); err != nil {
		t.Fatalf("delete issue: %v", err)
	}
	var count int
	if err := testPool.QueryRow(context.Background(), `SELECT COUNT(*) FROM task_execution_evidence WHERE task_id = $1`, taskID).Scan(&count); err != nil {
		t.Fatalf("count evidence after issue deletion: %v", err)
	}
	if count != 0 {
		t.Fatalf("execution evidence rows after issue deletion = %d, want 0", count)
	}
}

func TestQuickCreateEvidenceUsesContextProject(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	runtimeID, agentID, _, taskID := createRunningEvidenceTask(t)
	projectID := "22222222-2222-4222-8222-222222222222"
	contextPayload, err := json.Marshal(map[string]any{
		"type":         "quick_create",
		"workspace_id": testWorkspaceID,
		"project_id":   projectID,
		"prompt":       "quick create evidence test",
	})
	if err != nil {
		t.Fatalf("marshal quick-create context: %v", err)
	}
	if _, err := testPool.Exec(context.Background(), `
		UPDATE agent_task_queue SET issue_id = NULL, context = $2 WHERE id = $1
	`, taskID, contextPayload); err != nil {
		t.Fatalf("convert task to quick-create fixture: %v", err)
	}
	snapshot := executionevidence.Snapshot{
		SchemaVersion:          executionevidence.CurrentSchemaVersion,
		TaskID:                 taskID,
		Provider:               "handler_test_runtime",
		InvocationModel:        "claude-sonnet-5",
		InvocationModelSource:  "agent",
		ProviderCLIVersion:     "2.1.0",
		MulticaCLIVersion:      "v0.4.3",
		MulticaGitCommit:       "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		AgentID:                agentID,
		RuntimeID:              runtimeID,
		WorkspaceID:            testWorkspaceID,
		ProjectID:              projectID,
		MountedSkills:          []executionevidence.MountedSkill{},
		CustomArguments:        []string{},
		CustomEnvironmentNames: []string{},
		MCPServerNames:         []string{},
	}
	if w := recordEvidenceRequest(t, taskID, snapshot); w.Code != http.StatusCreated {
		t.Fatalf("quick-create evidence write: expected 201, got %d: %s", w.Code, w.Body.String())
	}
}

func TestAutopilotRunOnlyEvidenceUsesAutopilotProject(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	ctx := context.Background()
	runtimeID, agentID, _, taskID := createRunningEvidenceTask(t)

	var projectID string
	if err := testPool.QueryRow(ctx, `
		INSERT INTO project (workspace_id, title) VALUES ($1, 'execution evidence autopilot project') RETURNING id
	`, testWorkspaceID).Scan(&projectID); err != nil {
		t.Fatalf("create project: %v", err)
	}
	var autopilotID string
	if err := testPool.QueryRow(ctx, `
		INSERT INTO autopilot (
			workspace_id, project_id, title, description, assignee_id, execution_mode,
			created_by_type, created_by_id
		)
		VALUES ($1, $2, 'execution evidence run only', 'project-bound run only', $3, 'run_only', 'member', $4)
		RETURNING id
	`, testWorkspaceID, projectID, agentID, testUserID).Scan(&autopilotID); err != nil {
		t.Fatalf("create autopilot: %v", err)
	}
	var runID string
	if err := testPool.QueryRow(ctx, `
		INSERT INTO autopilot_run (autopilot_id, source, status)
		VALUES ($1, 'manual', 'running')
		RETURNING id
	`, autopilotID).Scan(&runID); err != nil {
		t.Fatalf("create autopilot run: %v", err)
	}
	if _, err := testPool.Exec(ctx, `
		UPDATE agent_task_queue SET issue_id = NULL, autopilot_run_id = $2 WHERE id = $1
	`, taskID, runID); err != nil {
		t.Fatalf("link running task to autopilot run: %v", err)
	}
	t.Cleanup(func() {
		_, _ = testPool.Exec(context.Background(), `UPDATE agent_task_queue SET autopilot_run_id = NULL WHERE id = $1`, taskID)
		_, _ = testPool.Exec(context.Background(), `DELETE FROM autopilot_run WHERE id = $1`, runID)
		_, _ = testPool.Exec(context.Background(), `DELETE FROM autopilot WHERE id = $1`, autopilotID)
		_, _ = testPool.Exec(context.Background(), `DELETE FROM project WHERE id = $1`, projectID)
	})

	snapshot := executionevidence.Snapshot{
		SchemaVersion:          executionevidence.CurrentSchemaVersion,
		TaskID:                 taskID,
		Provider:               "handler_test_runtime",
		InvocationModel:        "claude-sonnet-5",
		InvocationModelSource:  "agent",
		ProviderCLIVersion:     "2.1.0",
		MulticaCLIVersion:      "v0.4.3",
		MulticaGitCommit:       "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		AgentID:                agentID,
		RuntimeID:              runtimeID,
		WorkspaceID:            testWorkspaceID,
		ProjectID:              "11111111-1111-4111-8111-111111111111",
		MountedSkills:          []executionevidence.MountedSkill{},
		CustomArguments:        []string{},
		CustomEnvironmentNames: []string{},
		MCPServerNames:         []string{},
	}
	if w := recordEvidenceRequest(t, taskID, snapshot); w.Code != http.StatusBadRequest || !strings.Contains(w.Body.String(), "project does not match") {
		t.Fatalf("mismatched autopilot project evidence = %d: %s", w.Code, w.Body.String())
	}

	snapshot.ProjectID = projectID
	if w := recordEvidenceRequest(t, taskID, snapshot); w.Code != http.StatusCreated {
		t.Fatalf("matching autopilot project evidence = %d: %s", w.Code, w.Body.String())
	}
}

func TestProjectlessRunOnlyEvidenceRequiresEmptyProject(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	runtimeID, agentID, _, taskID := createRunningEvidenceTask(t)
	if _, err := testPool.Exec(context.Background(), `UPDATE agent_task_queue SET issue_id = NULL WHERE id = $1`, taskID); err != nil {
		t.Fatalf("convert task to projectless run-only fixture: %v", err)
	}
	snapshot := executionevidence.Snapshot{
		SchemaVersion:          executionevidence.CurrentSchemaVersion,
		TaskID:                 taskID,
		Provider:               "handler_test_runtime",
		InvocationModel:        "claude-sonnet-5",
		InvocationModelSource:  "agent",
		ProviderCLIVersion:     "2.1.0",
		MulticaCLIVersion:      "v0.4.3",
		MulticaGitCommit:       "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		AgentID:                agentID,
		RuntimeID:              runtimeID,
		WorkspaceID:            testWorkspaceID,
		MountedSkills:          []executionevidence.MountedSkill{},
		CustomArguments:        []string{},
		CustomEnvironmentNames: []string{},
		MCPServerNames:         []string{},
	}
	if w := recordEvidenceRequest(t, taskID, snapshot); w.Code != http.StatusCreated {
		t.Fatalf("projectless run-only evidence = %d: %s", w.Code, w.Body.String())
	}
}

func TestGetTaskEvidenceRejectsCrossWorkspaceTask(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	_, foreignTaskID := setupForeignWorkspaceFixture(t)
	ctx := context.Background()
	member, err := testHandler.Queries.GetMemberByUserAndWorkspace(ctx, db.GetMemberByUserAndWorkspaceParams{
		UserID: parseUUID(testUserID), WorkspaceID: parseUUID(testWorkspaceID),
	})
	if err != nil {
		t.Fatalf("load member: %v", err)
	}
	w := httptest.NewRecorder()
	req := newRequest(http.MethodGet, "/api/tasks/"+foreignTaskID+"/evidence", nil)
	req = withURLParam(req, "taskId", foreignTaskID)
	req = req.WithContext(middleware.SetMemberContext(req.Context(), testWorkspaceID, member))
	testHandler.GetTaskEvidence(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("cross-workspace evidence read: expected 404, got %d: %s", w.Code, w.Body.String())
	}
}
