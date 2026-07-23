package handler

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/middleware"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

type workflowCommitOnUploadStorage struct {
	mockStorage
	onUpload func(key string, data []byte) error
}

func (s *workflowCommitOnUploadStorage) Upload(
	ctx context.Context,
	key string,
	data []byte,
	contentType string,
	filename string,
) (string, error) {
	url, err := s.mockStorage.Upload(ctx, key, data, contentType, filename)
	if err != nil {
		return "", err
	}
	if s.onUpload != nil {
		if err := s.onUpload(key, data); err != nil {
			return "", err
		}
	}
	return url, nil
}

func TestWorkflowBundleArtifactKeyIsStablePerImmutableSubmission(t *testing.T) {
	manifest := workflowBundleManifest{
		RunID:     "run-1",
		NodeID:    "node-1",
		AttemptID: "attempt-1",
	}

	first := workflowBundleArtifactKey("workspace-1", manifest, "digest-1")
	second := workflowBundleArtifactKey("workspace-1", manifest, "digest-1")

	if first != second {
		t.Fatalf("artifact keys must be retry-stable: %q != %q", first, second)
	}
	want := "workflows/workspace-1/run-1/node-1/attempt-1-digest-1.bundle"
	if first != want {
		t.Fatalf("artifact key = %q, want %q", first, want)
	}
}

func TestWorkflowAttemptMatchesSubmission(t *testing.T) {
	attemptID := pgtype.UUID{
		Bytes: [16]byte{1, 2, 3},
		Valid: true,
	}
	current := db.WorkflowNodeAttempt{
		ID:             attemptID,
		ClaimEpoch:     7,
		Status:         "submitted",
		BaseCommit:     pgtype.Text{String: "base", Valid: true},
		ResultCommit:   pgtype.Text{String: "result", Valid: true},
		ArtifactKey:    pgtype.Text{String: "artifact-key", Valid: true},
		ArtifactDigest: pgtype.Text{String: "digest", Valid: true},
		ArtifactSize:   pgtype.Int8{Int64: 42, Valid: true},
	}

	if !workflowAttemptMatchesSubmission(
		current,
		attemptID,
		7,
		"base",
		"result",
		"digest",
		42,
	) {
		t.Fatal("matching submitted attempt did not converge")
	}

	current.ArtifactDigest.String = "other"
	if workflowAttemptMatchesSubmission(
		current,
		attemptID,
		7,
		"base",
		"result",
		"digest",
		42,
	) {
		t.Fatal("different artifact digest converged")
	}
}

func TestSubmitWorkflowBundlePreservesArtifactWhenCommitAckIsLost(t *testing.T) {
	testSubmitWorkflowBundleConvergence(t)
}

func testSubmitWorkflowBundleConvergence(t *testing.T) {
	ctx := context.Background()
	daemonID := fmt.Sprintf("workflow-bundle-test-%d", time.Now().UnixNano())

	var agentID string
	if err := testPool.QueryRow(
		ctx,
		`SELECT id FROM agent WHERE workspace_id = $1 ORDER BY created_at LIMIT 1`,
		testWorkspaceID,
	).Scan(&agentID); err != nil {
		t.Fatalf("load test agent: %v", err)
	}

	var projectID string
	if err := testPool.QueryRow(ctx, `
		INSERT INTO project (workspace_id, title, description, status)
		VALUES ($1, 'Workflow bundle retry', '', 'in_progress')
		RETURNING id
	`, testWorkspaceID).Scan(&projectID); err != nil {
		t.Fatalf("create project: %v", err)
	}
	var issueID string
	if err := testPool.QueryRow(ctx, `
		INSERT INTO issue (
			workspace_id, project_id, title, status, priority,
			creator_id, creator_type, number, position
		)
		VALUES (
			$1, $2, 'Workflow bundle retry', 'in_progress', 'none',
			$3, 'member',
			(SELECT COALESCE(MAX(number), 0) + 1 FROM issue WHERE workspace_id = $1),
			1
		)
		RETURNING id
	`, testWorkspaceID, projectID, testUserID).Scan(&issueID); err != nil {
		t.Fatalf("create issue: %v", err)
	}
	var runID string
	if err := testPool.QueryRow(ctx, `
		INSERT INTO workflow_run (
			workspace_id, project_id, anchor_issue_id, graph_key, graph_version,
			status, created_by
		)
		VALUES ($1, $2, $3, 'bundle-retry-test', '1', 'running', $4)
		RETURNING id
	`, testWorkspaceID, projectID, issueID, testUserID).Scan(&runID); err != nil {
		t.Fatalf("create workflow run: %v", err)
	}
	var nodeID string
	if err := testPool.QueryRow(ctx, `
		INSERT INTO workflow_node (
			run_id, issue_id, passage_key, node_key, generation, executor_kind,
			agent_id, state, claim_epoch, input_digest, law_digest, output_contract
		)
		VALUES (
			$1, $2, 'passage-1', 'lead-preparation', 1, 'agent',
			$3, 'running', 1, 'input-1', 'law-1', '{}'::jsonb
		)
		RETURNING id
	`, runID, issueID, agentID).Scan(&nodeID); err != nil {
		t.Fatalf("create workflow node: %v", err)
	}
	var taskID string
	if err := testPool.QueryRow(ctx, `
		INSERT INTO agent_task_queue (
			agent_id, issue_id, status, runtime_id, workflow_node_id,
			workflow_claim_epoch, workflow_input_digest, workflow_law_digest
		)
		VALUES ($1, $2, 'running', $3, $4, 1, 'input-1', 'law-1')
		RETURNING id
	`, agentID, issueID, testRuntimeID, nodeID).Scan(&taskID); err != nil {
		t.Fatalf("create workflow task: %v", err)
	}
	var attemptID string
	if err := testPool.QueryRow(ctx, `
		INSERT INTO workflow_node_attempt (
			node_id, claim_epoch, task_id, runtime_id, daemon_id, status,
			lease_expires_at, started_at
		)
		VALUES ($1, 1, $2, $3, $4, 'running', now() + interval '5 minutes', now())
		RETURNING id
	`, nodeID, taskID, testRuntimeID, daemonID).Scan(&attemptID); err != nil {
		t.Fatalf("create workflow attempt: %v", err)
	}
	if _, err := testPool.Exec(ctx,
		`UPDATE workflow_node SET current_attempt_id = $2 WHERE id = $1`,
		nodeID,
		attemptID,
	); err != nil {
		t.Fatalf("attach workflow attempt to node: %v", err)
	}
	if _, err := testPool.Exec(ctx,
		`UPDATE agent_task_queue SET workflow_attempt_id = $2 WHERE id = $1`,
		taskID,
		attemptID,
	); err != nil {
		t.Fatalf("attach workflow attempt to task: %v", err)
	}

	t.Cleanup(func() {
		cleanup := context.Background()
		_, _ = testPool.Exec(cleanup,
			`DELETE FROM workflow_outbox WHERE run_id = $1`,
			runID,
		)
		_, _ = testPool.Exec(cleanup,
			`UPDATE workflow_node SET current_attempt_id = NULL WHERE id = $1`,
			nodeID,
		)
		_, _ = testPool.Exec(cleanup,
			`UPDATE agent_task_queue SET workflow_attempt_id = NULL, workflow_node_id = NULL WHERE id = $1`,
			taskID,
		)
		_, _ = testPool.Exec(cleanup,
			`DELETE FROM workflow_node_attempt WHERE id = $1`,
			attemptID,
		)
		_, _ = testPool.Exec(cleanup, `DELETE FROM agent_task_queue WHERE id = $1`, taskID)
		_, _ = testPool.Exec(cleanup, `DELETE FROM workflow_node WHERE id = $1`, nodeID)
		_, _ = testPool.Exec(cleanup, `DELETE FROM workflow_run WHERE id = $1`, runID)
		_, _ = testPool.Exec(cleanup, `DELETE FROM issue WHERE id = $1`, issueID)
		_, _ = testPool.Exec(cleanup, `DELETE FROM project WHERE id = $1`, projectID)
	})

	bundle := []byte("bundle whose commit acknowledgement was lost")
	digestBytes := sha256.Sum256(bundle)
	digest := hex.EncodeToString(digestBytes[:])
	manifest := workflowBundleManifest{
		RunID:        runID,
		NodeID:       nodeID,
		AttemptID:    attemptID,
		PassageKey:   "passage-1",
		NodeKey:      "lead-preparation",
		Generation:   1,
		ClaimEpoch:   1,
		InputDigest:  "input-1",
		LawDigest:    "law-1",
		BaseCommit:   "base-1",
		ResultCommit: "result-1",
		AgentID:      agentID,
		RuntimeID:    testRuntimeID,
		ChangedPaths: []string{"pipeline-output/passage-1/01-segmented.md"},
		BundleSHA256: digest,
	}
	manifestRaw, err := json.Marshal(manifest)
	if err != nil {
		t.Fatalf("encode workflow manifest: %v", err)
	}

	storage := &workflowCommitOnUploadStorage{}
	var committedArtifactKey string
	var cancelRequest context.CancelFunc
	storage.onUpload = func(key string, data []byte) error {
		committedArtifactKey = key
		_, err := testPool.Exec(context.Background(), `
			UPDATE workflow_node_attempt
			SET status = 'submitted',
			    base_commit = $2,
			    result_commit = $3,
			    artifact_key = $4,
			    artifact_digest = $5,
			    artifact_size = $6,
			    manifest = $7,
			    submitted_at = now(),
			    completed_at = now()
			WHERE id = $1
		`, attemptID, manifest.BaseCommit, manifest.ResultCommit, committedArtifactKey, digest, int64(len(data)), manifestRaw)
		if err == nil {
			cancelRequest()
		}
		return err
	}
	handler := *testHandler
	handler.Storage = storage

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	if err := writer.WriteField("sha256", digest); err != nil {
		t.Fatalf("write digest field: %v", err)
	}
	if err := writer.WriteField("manifest", string(manifestRaw)); err != nil {
		t.Fatalf("write manifest field: %v", err)
	}
	part, err := writer.CreateFormFile("bundle", "result.bundle")
	if err != nil {
		t.Fatalf("create bundle form file: %v", err)
	}
	if _, err := part.Write(bundle); err != nil {
		t.Fatalf("write bundle form file: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close multipart body: %v", err)
	}
	requestPayload := append([]byte(nil), body.Bytes()...)

	request := httptest.NewRequest(
		http.MethodPost,
		"/api/daemon/tasks/"+taskID+"/workflow-bundle",
		bytes.NewReader(requestPayload),
	)
	request.Header.Set("Content-Type", writer.FormDataContentType())
	routeContext := chi.NewRouteContext()
	routeContext.URLParams.Add("taskId", taskID)
	requestContext, requestCancel := context.WithCancel(request.Context())
	cancelRequest = requestCancel
	defer requestCancel()
	requestContext = context.WithValue(requestContext, chi.RouteCtxKey, routeContext)
	requestContext = middleware.WithDaemonContext(
		requestContext,
		testWorkspaceID,
		daemonID,
	)
	request = request.WithContext(requestContext)
	response := httptest.NewRecorder()

	handler.SubmitWorkflowBundle(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", response.Code, response.Body.String())
	}
	var storedKey string
	if err := testPool.QueryRow(ctx,
		`SELECT artifact_key FROM workflow_node_attempt WHERE id = $1`,
		attemptID,
	).Scan(&storedKey); err != nil {
		t.Fatalf("load committed artifact key: %v", err)
	}
	storage.mu.Lock()
	_, preserved := storage.files[storedKey]
	storage.mu.Unlock()
	if !preserved {
		t.Fatalf("committed artifact %q was deleted after convergence", storedKey)
	}

	replayBody := bytes.NewReader(requestPayload)
	replayRequest := httptest.NewRequest(
		http.MethodPost,
		"/api/daemon/tasks/"+taskID+"/workflow-bundle",
		replayBody,
	)
	replayRequest.Header.Set("Content-Type", writer.FormDataContentType())
	replayRouteContext := chi.NewRouteContext()
	replayRouteContext.URLParams.Add("taskId", taskID)
	replayContext := context.WithValue(
		replayRequest.Context(),
		chi.RouteCtxKey,
		replayRouteContext,
	)
	replayContext = middleware.WithDaemonContext(
		replayContext,
		testWorkspaceID,
		daemonID,
	)
	replayRequest = replayRequest.WithContext(replayContext)
	replayResponse := httptest.NewRecorder()

	handler.SubmitWorkflowBundle(replayResponse, replayRequest)

	if replayResponse.Code != http.StatusOK {
		t.Fatalf(
			"replay status = %d, want 200; body=%s",
			replayResponse.Code,
			replayResponse.Body.String(),
		)
	}
	if replayBody.Len() != 0 {
		t.Fatalf("replay returned before draining %d request bytes", replayBody.Len())
	}

	expectBody := bytes.NewReader(requestPayload)
	expectRequest := httptest.NewRequest(
		http.MethodPost,
		"/api/daemon/tasks/"+taskID+"/workflow-bundle",
		expectBody,
	)
	expectRequest.Header.Set("Content-Type", writer.FormDataContentType())
	expectRequest.Header.Set("Expect", "100-continue")
	expectRouteContext := chi.NewRouteContext()
	expectRouteContext.URLParams.Add("taskId", taskID)
	expectContext := context.WithValue(
		expectRequest.Context(),
		chi.RouteCtxKey,
		expectRouteContext,
	)
	expectContext = middleware.WithDaemonContext(
		expectContext,
		testWorkspaceID,
		daemonID,
	)
	expectRequest = expectRequest.WithContext(expectContext)
	expectResponse := httptest.NewRecorder()

	handler.SubmitWorkflowBundle(expectResponse, expectRequest)

	if expectResponse.Code != http.StatusOK {
		t.Fatalf(
			"expect replay status = %d, want 200; body=%s",
			expectResponse.Code,
			expectResponse.Body.String(),
		)
	}
	if expectBody.Len() != len(requestPayload) {
		t.Fatalf(
			"expect replay consumed %d bytes before early response",
			len(requestPayload)-expectBody.Len(),
		)
	}
}
