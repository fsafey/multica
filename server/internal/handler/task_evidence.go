package handler

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"sort"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/middleware"
	"github.com/multica-ai/multica/server/internal/service"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/executionevidence"
	"github.com/multica-ai/multica/server/pkg/protocol"
)

const maxTaskExecutionEvidenceBodyBytes = 16 << 20

type RecordTaskExecutionEvidenceRequest struct {
	Snapshot    executionevidence.Snapshot `json:"snapshot"`
	PayloadHash string                     `json:"payload_hash"`
}

// RecordTaskExecutionEvidence persists the daemon's immutable pre-launch
// snapshot. Only running tasks may create a row, which prevents historical
// backfills from being presented as native execution evidence.
func (h *Handler) RecordTaskExecutionEvidence(w http.ResponseWriter, r *http.Request) {
	taskID := chi.URLParam(r, "taskId")
	task, workspaceID, ok := h.requireDaemonTaskAccessWithWorkspace(w, r, taskID)
	if !ok {
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxTaskExecutionEvidenceBodyBytes)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			writeError(w, http.StatusRequestEntityTooLarge, "execution evidence request body is too large")
			return
		}
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	var versionProbe struct {
		Snapshot struct {
			SchemaVersion int `json:"schema_version"`
		} `json:"snapshot"`
	}
	if json.Unmarshal(body, &versionProbe) != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if versionProbe.Snapshot.SchemaVersion > executionevidence.CurrentSchemaVersion {
		writeError(w, http.StatusBadRequest, "execution evidence schema version is not supported; upgrade the server")
		return
	}

	var req RecordTaskExecutionEvidenceRequest
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if task.Status != "running" {
		writeError(w, http.StatusConflict, "execution evidence can only be recorded before provider launch")
		return
	}
	if req.Snapshot.TaskID != taskID ||
		req.Snapshot.AgentID != uuidToString(task.AgentID) ||
		req.Snapshot.RuntimeID != uuidToString(task.RuntimeID) ||
		req.Snapshot.WorkspaceID != workspaceID {
		writeError(w, http.StatusBadRequest, "execution evidence identifiers do not match task")
		return
	}
	runtime, runtimeErr := h.Queries.GetAgentRuntime(r.Context(), task.RuntimeID)
	if runtimeErr != nil {
		writeError(w, http.StatusInternalServerError, "failed to verify execution evidence runtime")
		return
	}
	if req.Snapshot.Provider != runtime.Provider {
		writeError(w, http.StatusBadRequest, "execution evidence provider does not match runtime")
		return
	}
	// Issue project membership is mutable after claim. The daemon snapshot is
	// the claim-time record, so comparing it to the live issue would replace
	// provenance with current state or fail a valid launch. Quick-create tasks
	// carry their immutable project in task context and can still be checked.
	if !task.IssueID.Valid {
		expectedProjectID := ""
		if len(task.Context) > 0 {
			var quickCreate service.QuickCreateContext
			if json.Unmarshal(task.Context, &quickCreate) == nil && quickCreate.Type == service.QuickCreateContextType {
				expectedProjectID = quickCreate.ProjectID
			}
		}
		if req.Snapshot.ProjectID != expectedProjectID {
			writeError(w, http.StatusBadRequest, "execution evidence project does not match task")
			return
		}
	}
	if req.Snapshot.SchemaVersion <= 0 || req.Snapshot.SchemaVersion > executionevidence.CurrentSchemaVersion {
		writeError(w, http.StatusBadRequest, "execution evidence schema version is not supported")
		return
	}

	payload, err := req.Snapshot.CanonicalPayload()
	if err != nil {
		slog.Warn("execution evidence payload is not canonicalizable", "task_id", taskID, "error", err)
		writeError(w, http.StatusBadRequest, "execution evidence payload is not canonicalizable")
		return
	}
	digest, err := req.Snapshot.Digest()
	if err != nil || req.PayloadHash != digest {
		writeError(w, http.StatusBadRequest, "execution evidence payload hash mismatch")
		return
	}

	created, err := h.Queries.CreateTaskExecutionEvidence(r.Context(), db.CreateTaskExecutionEvidenceParams{
		TaskID:        task.ID,
		SchemaVersion: int32(req.Snapshot.SchemaVersion),
		Payload:       payload,
		PayloadHash:   digest,
	})
	if err == nil {
		writeJSON(w, http.StatusCreated, map[string]any{
			"schema_version": created.SchemaVersion,
			"digest":         created.PayloadHash,
			"created_at":     timestampToString(created.CreatedAt),
		})
		return
	}
	if !isNotFound(err) {
		slog.Error("failed to create task execution evidence", "task_id", taskID, "error", err)
		writeError(w, http.StatusInternalServerError, "failed to persist task execution evidence")
		return
	}

	existing, getErr := h.Queries.GetTaskExecutionEvidence(r.Context(), task.ID)
	if getErr != nil {
		slog.Error("failed to read conflicting task execution evidence", "task_id", taskID, "error", getErr)
		writeError(w, http.StatusInternalServerError, "failed to verify task execution evidence")
		return
	}
	existingPayload, canonicalErr := executionevidence.CanonicalizePayload(existing.Payload)
	if canonicalErr != nil {
		writeError(w, http.StatusConflict, "existing execution evidence is not readable")
		return
	}
	if existing.SchemaVersion != int32(req.Snapshot.SchemaVersion) ||
		existing.PayloadHash != digest || !bytes.Equal(existingPayload, payload) {
		writeError(w, http.StatusConflict, "conflicting execution evidence already exists")
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"schema_version": existing.SchemaVersion,
		"digest":         existing.PayloadHash,
		"created_at":     timestampToString(existing.CreatedAt),
	})
}

type TaskLifecycleEvidence struct {
	ID                          string  `json:"id"`
	AgentID                     string  `json:"agent_id"`
	RuntimeID                   string  `json:"runtime_id"`
	IssueID                     string  `json:"issue_id,omitempty"`
	ChatSessionID               string  `json:"chat_session_id,omitempty"`
	AutopilotRunID              string  `json:"autopilot_run_id,omitempty"`
	Status                      string  `json:"status"`
	Attempt                     int32   `json:"attempt"`
	MaxAttempts                 int32   `json:"max_attempts"`
	CreatedAt                   string  `json:"created_at"`
	DispatchedAt                *string `json:"dispatched_at"`
	StartedAt                   *string `json:"started_at"`
	CompletedAt                 *string `json:"completed_at"`
	SessionID                   string  `json:"session_id,omitempty"`
	WorkDir                     string  `json:"work_dir,omitempty"`
	Result                      any     `json:"result,omitempty"`
	Error                       *string `json:"error,omitempty"`
	FailureReason               string  `json:"failure_reason,omitempty"`
	ExpectedMessageCount        *int32  `json:"expected_message_count,omitempty"`
	ExpectedLastSequence        *int32  `json:"expected_last_sequence,omitempty"`
	TranscriptDeliveryConfirmed *bool   `json:"transcript_delivery_confirmed,omitempty"`
}

type TaskUsageEvidence struct {
	Provider         string `json:"provider"`
	Model            string `json:"model"`
	InputTokens      int64  `json:"input_tokens"`
	OutputTokens     int64  `json:"output_tokens"`
	CacheReadTokens  int64  `json:"cache_read_tokens"`
	CacheWriteTokens int64  `json:"cache_write_tokens"`
}

type TaskEvidenceResponse struct {
	Task                           TaskLifecycleEvidence               `json:"task"`
	ExecutionSnapshot              *executionevidence.Snapshot         `json:"execution_snapshot"`
	Messages                       []protocol.TaskMessagePayload       `json:"messages"`
	PerModelUsage                  []TaskUsageEvidence                 `json:"per_model_usage"`
	SequenceIntegrity              executionevidence.SequenceIntegrity `json:"sequence_integrity"`
	ExecutionSnapshotSchemaVersion int32                               `json:"execution_snapshot_schema_version"`
	ExecutionSnapshotDigest        string                              `json:"execution_snapshot_digest"`
	ExecutionSnapshotIntegrity     bool                                `json:"execution_snapshot_integrity"`
	EvidenceManifestSchemaVersion  int32                               `json:"evidence_manifest_schema_version"`
	EvidenceManifestDigest         string                              `json:"evidence_manifest_digest"`
	CompletenessIssues             []string                            `json:"completeness_issues"`
	Complete                       bool                                `json:"complete"`
}

const evidenceManifestSchemaVersion int32 = 1

type taskEvidenceManifest struct {
	SchemaVersion           int32                               `json:"schema_version"`
	Task                    TaskLifecycleEvidence               `json:"task"`
	ExecutionSnapshot       *executionevidence.Snapshot         `json:"execution_snapshot"`
	ExecutionSnapshotDigest string                              `json:"execution_snapshot_digest"`
	Messages                []protocol.TaskMessagePayload       `json:"messages"`
	PerModelUsage           []TaskUsageEvidence                 `json:"per_model_usage"`
	SequenceIntegrity       executionevidence.SequenceIntegrity `json:"sequence_integrity"`
	CompletenessIssues      []string                            `json:"completeness_issues"`
	Complete                bool                                `json:"complete"`
}

// GetTaskEvidence returns a provider-neutral task trace under the same
// workspace authorization used by the user-facing task-message endpoint.
func (h *Handler) GetTaskEvidence(w http.ResponseWriter, r *http.Request) {
	taskID := chi.URLParam(r, "taskId")
	task, workspaceID, ok := h.requireUserTaskAccess(w, r, taskID)
	if !ok {
		return
	}

	messages, err := h.Queries.ListTaskMessages(r.Context(), task.ID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list task messages")
		return
	}
	arrivalSequences, err := h.Queries.ListTaskMessageSequencesByArrival(r.Context(), task.ID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to verify task message ordering")
		return
	}
	usage, err := h.Queries.GetTaskUsage(r.Context(), task.ID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list task usage")
		return
	}

	messagePayloads := make([]protocol.TaskMessagePayload, len(messages))
	for i, message := range messages {
		messagePayloads[i] = taskMessageToPayload(message, taskID, uuidToString(task.IssueID))
	}
	sequenceIntegrity := executionevidence.CheckSequenceIntegrity(arrivalSequences)

	usagePayloads := make([]TaskUsageEvidence, len(usage))
	for i, entry := range usage {
		usagePayloads[i] = TaskUsageEvidence{
			Provider:         entry.Provider,
			Model:            entry.Model,
			InputTokens:      entry.InputTokens,
			OutputTokens:     entry.OutputTokens,
			CacheReadTokens:  entry.CacheReadTokens,
			CacheWriteTokens: entry.CacheWriteTokens,
		}
	}
	sort.Slice(usagePayloads, func(i, j int) bool {
		if usagePayloads[i].Provider == usagePayloads[j].Provider {
			return usagePayloads[i].Model < usagePayloads[j].Model
		}
		return usagePayloads[i].Provider < usagePayloads[j].Provider
	})

	var snapshot *executionevidence.Snapshot
	var schemaVersion int32
	var digest string
	integrity := false
	completenessIssues := []string{"execution_snapshot"}
	row, evidenceErr := h.Queries.GetTaskExecutionEvidence(r.Context(), task.ID)
	if evidenceErr == nil {
		var decoded executionevidence.Snapshot
		if json.Unmarshal(row.Payload, &decoded) == nil {
			canonical, canonicalErr := executionevidence.CanonicalizePayload(row.Payload)
			integrity = canonicalErr == nil && executionevidence.HashBytes(canonical) == row.PayloadHash &&
				int32(decoded.SchemaVersion) == row.SchemaVersion
			snapshot = &decoded
			completenessIssues = decoded.CompletenessIssues()
			if !integrity {
				completenessIssues = append(completenessIssues, "execution_snapshot_integrity")
			}
		} else {
			completenessIssues = []string{"execution_snapshot_unreadable"}
		}
		schemaVersion = row.SchemaVersion
		digest = row.PayloadHash
	} else if !isNotFound(evidenceErr) {
		writeError(w, http.StatusInternalServerError, "failed to load task execution evidence")
		return
	}
	if !sequenceIntegrity.Valid {
		completenessIssues = append(completenessIssues, "message_sequence")
	}
	if len(sequenceIntegrity.Gaps) > 0 {
		completenessIssues = append(completenessIssues, "message_sequence_gap")
	}
	if !task.TranscriptExpectedMessageCount.Valid || !task.TranscriptExpectedLastSeq.Valid || !task.TranscriptDeliveryConfirmed.Valid {
		completenessIssues = append(completenessIssues, "message_expectation")
	} else {
		expectedCount := task.TranscriptExpectedMessageCount.Int32
		expectedLast := task.TranscriptExpectedLastSeq.Int32
		actualLast := int32(0)
		if len(messages) > 0 {
			actualLast = messages[len(messages)-1].Seq
		}
		if expectedCount == 0 {
			completenessIssues = append(completenessIssues, "message_transcript_empty")
		}
		if int32(len(messages)) != expectedCount {
			completenessIssues = append(completenessIssues, "message_count")
		}
		if actualLast != expectedLast {
			completenessIssues = append(completenessIssues, "message_last_sequence")
		}
		if !task.TranscriptDeliveryConfirmed.Bool {
			completenessIssues = append(completenessIssues, "message_delivery")
		}
	}
	if !isTerminalTaskStatus(task.Status) {
		completenessIssues = append(completenessIssues, "task_not_terminal")
	}
	sort.Strings(completenessIssues)
	taskEvidence := taskLifecycleEvidence(task, workspaceID)
	complete := len(completenessIssues) == 0
	manifest := taskEvidenceManifest{
		SchemaVersion:           evidenceManifestSchemaVersion,
		Task:                    taskEvidence,
		ExecutionSnapshot:       snapshot,
		ExecutionSnapshotDigest: digest,
		Messages:                messagePayloads,
		PerModelUsage:           usagePayloads,
		SequenceIntegrity:       sequenceIntegrity,
		CompletenessIssues:      completenessIssues,
		Complete:                complete,
	}
	manifestJSON, err := json.Marshal(manifest)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to encode task evidence manifest")
		return
	}
	canonicalManifest, err := executionevidence.CanonicalizePayload(manifestJSON)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to canonicalize task evidence manifest")
		return
	}

	writeJSON(w, http.StatusOK, TaskEvidenceResponse{
		Task:                           taskEvidence,
		ExecutionSnapshot:              snapshot,
		Messages:                       messagePayloads,
		PerModelUsage:                  usagePayloads,
		SequenceIntegrity:              sequenceIntegrity,
		ExecutionSnapshotSchemaVersion: schemaVersion,
		ExecutionSnapshotDigest:        digest,
		ExecutionSnapshotIntegrity:     integrity,
		EvidenceManifestSchemaVersion:  evidenceManifestSchemaVersion,
		EvidenceManifestDigest:         executionevidence.HashBytes(canonicalManifest),
		CompletenessIssues:             completenessIssues,
		Complete:                       complete,
	})
}

func (h *Handler) requireUserTaskAccess(w http.ResponseWriter, r *http.Request, taskID string) (db.AgentTaskQueue, string, bool) {
	taskUUID, ok := parseUUIDOrBadRequest(w, taskID, "task_id")
	if !ok {
		return db.AgentTaskQueue{}, "", false
	}
	task, err := h.Queries.GetAgentTask(r.Context(), taskUUID)
	if err != nil {
		writeError(w, http.StatusNotFound, "task not found")
		return db.AgentTaskQueue{}, "", false
	}
	workspaceID := h.TaskService.ResolveTaskWorkspaceID(r.Context(), task)
	if workspaceID == "" || workspaceID != middleware.WorkspaceIDFromContext(r.Context()) {
		writeError(w, http.StatusNotFound, "task not found")
		return db.AgentTaskQueue{}, "", false
	}
	return task, workspaceID, true
}

func taskLifecycleEvidence(task db.AgentTaskQueue, workspaceID string) TaskLifecycleEvidence {
	var result any
	if len(task.Result) > 0 {
		_ = json.Unmarshal(task.Result, &result)
	}
	sessionID := ""
	if task.SessionID.Valid {
		sessionID = task.SessionID.String
	}
	workDir := ""
	if task.WorkDir.Valid {
		workDir = relativeWorkDir(task.WorkDir.String, workspaceID, uuidToString(task.ID))
	}
	failureReason := ""
	if task.FailureReason.Valid {
		failureReason = task.FailureReason.String
	}
	return TaskLifecycleEvidence{
		ID:                          uuidToString(task.ID),
		AgentID:                     uuidToString(task.AgentID),
		RuntimeID:                   uuidToString(task.RuntimeID),
		IssueID:                     uuidToString(task.IssueID),
		ChatSessionID:               uuidToString(task.ChatSessionID),
		AutopilotRunID:              uuidToString(task.AutopilotRunID),
		Status:                      task.Status,
		Attempt:                     task.Attempt,
		MaxAttempts:                 task.MaxAttempts,
		CreatedAt:                   timestampToString(task.CreatedAt),
		DispatchedAt:                timestampToPtr(task.DispatchedAt),
		StartedAt:                   timestampToPtr(task.StartedAt),
		CompletedAt:                 timestampToPtr(task.CompletedAt),
		SessionID:                   sessionID,
		WorkDir:                     workDir,
		Result:                      result,
		Error:                       textToPtr(task.Error),
		FailureReason:               failureReason,
		ExpectedMessageCount:        int32ToPtr(task.TranscriptExpectedMessageCount),
		ExpectedLastSequence:        int32ToPtr(task.TranscriptExpectedLastSeq),
		TranscriptDeliveryConfirmed: boolToPtr(task.TranscriptDeliveryConfirmed),
	}
}

func int32ToPtr(value pgtype.Int4) *int32 {
	if !value.Valid {
		return nil
	}
	result := value.Int32
	return &result
}

func boolToPtr(value pgtype.Bool) *bool {
	if !value.Valid {
		return nil
	}
	result := value.Bool
	return &result
}

func isTerminalTaskStatus(status string) bool {
	switch status {
	case "completed", "failed", "cancelled":
		return true
	default:
		return false
	}
}
