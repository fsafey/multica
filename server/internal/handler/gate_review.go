package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"regexp"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/protocol"
)

const maxGateReviewDataBytes = 32 * 1024

var (
	gateNamePattern      = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9._-]{0,63}$`)
	subjectDigestPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
)

// GateReviewData is the deterministic review sheet shared by web and desktop.
// Every field is deliberately product-shaped instead of accepting arbitrary
// prose so a member can see exactly what one immutable decision covers.
type GateReviewData struct {
	SelectedSource  string          `json:"selected_source"`
	Scope           string          `json:"scope"`
	Defaults        []string        `json:"defaults"`
	Rights          string          `json:"rights"`
	Uncertainties   []string        `json:"uncertainties"`
	Cost            string          `json:"cost"`
	Changes         []string        `json:"changes,omitempty"`
	CanonicalDetail json.RawMessage `json:"canonical_detail"`
}

type CreateGateReviewRequestBody struct {
	Gate          string         `json:"gate"`
	Revision      int32          `json:"revision"`
	SubjectDigest string         `json:"subject_digest"`
	Review        GateReviewData `json:"review"`
}

type CreateGateReviewDecisionBody struct {
	Outcome string `json:"outcome"`
	Reason  string `json:"reason,omitempty"`
}

type GateReviewDecisionResponse struct {
	ID        string `json:"id"`
	Outcome   string `json:"outcome"`
	Reason    string `json:"reason"`
	ActorID   string `json:"actor_id"`
	ActorName string `json:"actor_name,omitempty"`
	CreatedAt string `json:"created_at"`
}

type GateDecisionWakeResponse struct {
	State  string  `json:"state"`
	TaskID *string `json:"task_id,omitempty"`
}

type GateReviewRequestResponse struct {
	ID            string                      `json:"id"`
	IssueID       string                      `json:"issue_id"`
	Gate          string                      `json:"gate"`
	Revision      int32                       `json:"revision"`
	SubjectDigest string                      `json:"subject_digest"`
	Review        GateReviewData              `json:"review"`
	ActorType     string                      `json:"actor_type"`
	ActorID       string                      `json:"actor_id"`
	ActorName     string                      `json:"actor_name,omitempty"`
	CreatedAt     string                      `json:"created_at"`
	Decision      *GateReviewDecisionResponse `json:"decision,omitempty"`
	Wake          *GateDecisionWakeResponse   `json:"wake,omitempty"`
}

type GateReviewDecisionEnvelope struct {
	Request  GateReviewRequestResponse  `json:"request"`
	Decision GateReviewDecisionResponse `json:"decision"`
	Wake     GateDecisionWakeResponse   `json:"wake"`
}

func validateGateReviewData(data GateReviewData) error {
	if strings.TrimSpace(data.SelectedSource) == "" {
		return errors.New("review.selected_source is required")
	}
	if strings.TrimSpace(data.Scope) == "" {
		return errors.New("review.scope is required")
	}
	if strings.TrimSpace(data.Rights) == "" {
		return errors.New("review.rights is required")
	}
	if strings.TrimSpace(data.Cost) == "" {
		return errors.New("review.cost is required")
	}
	if data.Defaults == nil {
		return errors.New("review.defaults is required")
	}
	if data.Uncertainties == nil {
		return errors.New("review.uncertainties is required")
	}
	encoded, err := json.Marshal(data)
	if err != nil {
		return errors.New("review is invalid")
	}
	if len(encoded) > maxGateReviewDataBytes {
		return fmt.Errorf("review exceeds %d bytes", maxGateReviewDataBytes)
	}
	var canonical map[string]any
	if len(data.CanonicalDetail) == 0 || json.Unmarshal(data.CanonicalDetail, &canonical) != nil || canonical == nil {
		return errors.New("review.canonical_detail must be a JSON object")
	}
	return nil
}

func gateReviewJSONEqual(left, right []byte) bool {
	var leftValue, rightValue any
	if json.Unmarshal(left, &leftValue) != nil || json.Unmarshal(right, &rightValue) != nil {
		return false
	}
	return reflect.DeepEqual(leftValue, rightValue)
}

func validateCreateGateReviewRequest(req CreateGateReviewRequestBody) error {
	if !gateNamePattern.MatchString(req.Gate) {
		return errors.New("gate must be 1-64 letters, numbers, dots, underscores, or hyphens")
	}
	if req.Revision <= 0 {
		return errors.New("revision must be positive")
	}
	if !subjectDigestPattern.MatchString(req.SubjectDigest) {
		return errors.New("subject_digest must be sha256 followed by 64 lowercase hex characters")
	}
	return validateGateReviewData(req.Review)
}

func gateReviewStreamKey(issueID pgtype.UUID, gate string) string {
	return uuidToString(issueID) + ":gate:" + gate
}

// gateReviewTimestamp preserves PostgreSQL's microsecond precision. Gate
// requests and their decisions are ordered events and commonly land within
// one second, so the shared second-precision formatter is not safe here.
func gateReviewTimestamp(ts pgtype.Timestamptz) string {
	if !ts.Valid {
		return ""
	}
	return ts.Time.UTC().Format("2006-01-02T15:04:05.000000Z")
}

func requestResponse(row db.GateReviewRequest) GateReviewRequestResponse {
	var review GateReviewData
	_ = json.Unmarshal(row.ReviewData, &review)
	return GateReviewRequestResponse{
		ID:            uuidToString(row.ID),
		IssueID:       uuidToString(row.IssueID),
		Gate:          row.Gate,
		Revision:      row.Revision,
		SubjectDigest: row.SubjectDigest,
		Review:        review,
		ActorType:     row.ActorType,
		ActorID:       uuidToString(row.ActorID),
		CreatedAt:     gateReviewTimestamp(row.CreatedAt),
	}
}

func decisionResponse(row db.GateReviewDecision, actorName string) GateReviewDecisionResponse {
	return GateReviewDecisionResponse{
		ID:        uuidToString(row.ID),
		Outcome:   row.Outcome,
		Reason:    row.Reason,
		ActorID:   uuidToString(row.ActorID),
		ActorName: actorName,
		CreatedAt: gateReviewTimestamp(row.CreatedAt),
	}
}

func wakeResponse(row db.GateDecisionWake) GateDecisionWakeResponse {
	return GateDecisionWakeResponse{State: row.State, TaskID: uuidToPtr(row.TaskID)}
}

func gateReviewProjectionContent(gate string, revision int32, outcome string) string {
	if outcome == "" {
		return fmt.Sprintf("Gate %s revision %d is ready for member review.", gate, revision)
	}
	if outcome == "approved" {
		return fmt.Sprintf("Gate %s revision %d was approved through the member decision control.", gate, revision)
	}
	return fmt.Sprintf("Changes were requested for gate %s revision %d through the member decision control.", gate, revision)
}

func (h *Handler) publishGateReviewProjection(issue db.Issue, comment db.Comment, actorType, actorID string) {
	h.publish(protocol.EventCommentCreated, uuidToString(issue.WorkspaceID), actorType, actorID, map[string]any{
		"comment":             commentToResponse(comment, nil, nil),
		"issue_title":         issue.Title,
		"issue_assignee_type": textToPtr(issue.AssigneeType),
		"issue_assignee_id":   uuidToPtr(issue.AssigneeID),
		"issue_status":        issue.Status,
	})
}

func (h *Handler) CreateGateReviewRequest(w http.ResponseWriter, r *http.Request) {
	issue, ok := h.loadIssueForUser(w, r, chi.URLParam(r, "id"))
	if !ok {
		return
	}
	userID, ok := requireUserID(w, r)
	if !ok {
		return
	}
	var req CreateGateReviewRequestBody
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := validateCreateGateReviewRequest(req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	actorType, actorID := h.resolveActor(r, userID, uuidToString(issue.WorkspaceID))
	reviewJSON, _ := json.Marshal(req.Review)

	tx, err := h.TxStarter.Begin(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to begin gate review request")
		return
	}
	defer tx.Rollback(r.Context())
	qtx := h.Queries.WithTx(tx)
	if _, err := tx.Exec(r.Context(), `SELECT pg_advisory_xact_lock(hashtextextended($1, 0::bigint))`, gateReviewStreamKey(issue.ID, req.Gate)); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to lock gate review stream")
		return
	}
	latest, latestErr := qtx.GetLatestGateReviewRequest(r.Context(), db.GetLatestGateReviewRequestParams{
		WorkspaceID: issue.WorkspaceID, IssueID: issue.ID, Gate: req.Gate,
	})
	if latestErr == nil {
		if latest.Revision == req.Revision && latest.SubjectDigest == req.SubjectDigest && gateReviewJSONEqual(latest.ReviewData, reviewJSON) {
			_ = tx.Rollback(r.Context())
			writeJSON(w, http.StatusOK, requestResponse(latest))
			return
		}
		if req.Revision <= latest.Revision {
			writeError(w, http.StatusConflict, "gate review revision is stale or already bound to different data")
			return
		}
	} else if !errors.Is(latestErr, pgx.ErrNoRows) {
		writeError(w, http.StatusInternalServerError, "failed to read gate review stream")
		return
	}
	created, err := qtx.CreateGateReviewRequest(r.Context(), db.CreateGateReviewRequestParams{
		WorkspaceID:   issue.WorkspaceID,
		IssueID:       issue.ID,
		Gate:          req.Gate,
		Revision:      req.Revision,
		SubjectDigest: req.SubjectDigest,
		ReviewData:    reviewJSON,
		ActorType:     actorType,
		ActorID:       parseUUID(actorID),
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to create gate review request")
		return
	}
	projection, err := qtx.CreateComment(r.Context(), db.CreateCommentParams{
		IssueID:     issue.ID,
		WorkspaceID: issue.WorkspaceID,
		AuthorType:  actorType,
		AuthorID:    parseUUID(actorID),
		Content:     gateReviewProjectionContent(req.Gate, req.Revision, ""),
		Type:        "system",
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to create gate review projection")
		return
	}
	if err := tx.Commit(r.Context()); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to commit gate review request")
		return
	}
	h.publishGateReviewProjection(issue, projection, actorType, actorID)
	writeJSON(w, http.StatusCreated, requestResponse(created))
}

func (h *Handler) ListGateReviews(w http.ResponseWriter, r *http.Request) {
	issue, ok := h.loadIssueForUser(w, r, chi.URLParam(r, "id"))
	if !ok {
		return
	}
	h.reconcileGateDecisionWakes(r.Context(), issue)
	rows, err := h.Queries.ListGateReviewRequestsForIssue(r.Context(), db.ListGateReviewRequestsForIssueParams{
		WorkspaceID: issue.WorkspaceID, IssueID: issue.ID,
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list gate reviews")
		return
	}
	responses := make([]GateReviewRequestResponse, 0, len(rows))
	for _, row := range rows {
		base := requestResponse(db.GateReviewRequest{
			ID: row.ID, WorkspaceID: row.WorkspaceID, IssueID: row.IssueID,
			Gate: row.Gate, Revision: row.Revision, SubjectDigest: row.SubjectDigest,
			ReviewData: row.ReviewData, ActorType: row.ActorType, ActorID: row.ActorID,
			CreatedAt: row.CreatedAt,
		})
		base.ActorName = row.RequestActorName
		if row.DecisionID.Valid {
			base.Decision = &GateReviewDecisionResponse{
				ID: uuidToString(row.DecisionID), Outcome: row.DecisionOutcome.String,
				Reason: row.DecisionReason.String, ActorID: uuidToString(row.DecisionActorID),
				ActorName: row.DecisionActorName.String, CreatedAt: gateReviewTimestamp(row.DecisionCreatedAt),
			}
			base.Wake = &GateDecisionWakeResponse{State: row.WakeState.String, TaskID: uuidToPtr(row.WakeTaskID)}
		}
		responses = append(responses, base)
	}
	writeJSON(w, http.StatusOK, map[string]any{"gate_reviews": responses})
}

func (h *Handler) CreateGateReviewDecision(w http.ResponseWriter, r *http.Request) {
	issue, ok := h.loadIssueForUser(w, r, chi.URLParam(r, "id"))
	if !ok {
		return
	}
	userID, ok := requireUserID(w, r)
	if !ok {
		return
	}
	requestID, ok := parseUUIDOrBadRequest(w, chi.URLParam(r, "requestId"), "request_id")
	if !ok {
		return
	}
	var req CreateGateReviewDecisionBody
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	req.Reason = strings.TrimSpace(req.Reason)
	if req.Outcome != "approved" && req.Outcome != "changes_requested" {
		writeError(w, http.StatusBadRequest, "outcome must be approved or changes_requested")
		return
	}
	if len(req.Reason) > 4000 {
		writeError(w, http.StatusBadRequest, "reason exceeds 4000 characters")
		return
	}
	actorID := parseUUID(userID)

	tx, err := h.TxStarter.Begin(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to begin gate decision")
		return
	}
	defer tx.Rollback(r.Context())
	qtx := h.Queries.WithTx(tx)
	target, err := qtx.GetGateReviewRequestInIssue(r.Context(), db.GetGateReviewRequestInIssueParams{
		ID: requestID, WorkspaceID: issue.WorkspaceID, IssueID: issue.ID,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeError(w, http.StatusNotFound, "gate review request not found")
		} else {
			writeError(w, http.StatusInternalServerError, "failed to load gate review request")
		}
		return
	}
	if _, err := tx.Exec(r.Context(), `SELECT pg_advisory_xact_lock(hashtextextended($1, 0::bigint))`, gateReviewStreamKey(issue.ID, target.Gate)); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to lock gate review stream")
		return
	}
	existing, existingErr := qtx.GetGateReviewDecisionByRequest(r.Context(), db.GetGateReviewDecisionByRequestParams{
		RequestID: requestID, WorkspaceID: issue.WorkspaceID, IssueID: issue.ID,
	})
	if existingErr == nil {
		if existing.ActorID == actorID && existing.Outcome == req.Outcome && existing.Reason == req.Reason {
			_ = tx.Rollback(r.Context())
			wake, _ := h.reconcileOneGateDecisionWake(r.Context(), issue, existing.ID)
			actor, _ := h.Queries.GetUser(r.Context(), actorID)
			writeJSON(w, http.StatusOK, GateReviewDecisionEnvelope{
				Request: requestResponse(target), Decision: decisionResponse(existing, actor.Name), Wake: wakeResponse(wake),
			})
			return
		}
		writeError(w, http.StatusConflict, "gate review request already has a different decision")
		return
	} else if !errors.Is(existingErr, pgx.ErrNoRows) {
		writeError(w, http.StatusInternalServerError, "failed to read gate decision")
		return
	}
	latest, err := qtx.GetLatestGateReviewRequest(r.Context(), db.GetLatestGateReviewRequestParams{
		WorkspaceID: issue.WorkspaceID, IssueID: issue.ID, Gate: target.Gate,
	})
	if err != nil || latest.ID != target.ID {
		writeError(w, http.StatusConflict, "gate review request is stale")
		return
	}
	decision, err := qtx.CreateGateReviewDecision(r.Context(), db.CreateGateReviewDecisionParams{
		WorkspaceID: issue.WorkspaceID, IssueID: issue.ID, RequestID: requestID,
		Outcome: req.Outcome, Reason: req.Reason, ActorID: actorID,
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to create gate decision")
		return
	}
	if _, err := qtx.CreateGateDecisionWake(r.Context(), db.CreateGateDecisionWakeParams{
		DecisionID: decision.ID, WorkspaceID: issue.WorkspaceID, IssueID: issue.ID,
	}); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to create durable gate wake")
		return
	}
	projection, err := qtx.CreateComment(r.Context(), db.CreateCommentParams{
		IssueID: issue.ID, WorkspaceID: issue.WorkspaceID, AuthorType: "member",
		AuthorID: actorID, Content: gateReviewProjectionContent(target.Gate, target.Revision, req.Outcome), Type: "system",
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to create gate decision projection")
		return
	}
	if err := tx.Commit(r.Context()); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to commit gate decision")
		return
	}
	h.publishGateReviewProjection(issue, projection, "member", userID)
	if h.GateDecisionWakeWorker != nil {
		h.GateDecisionWakeWorker.Notify()
	}
	wake, _ := h.reconcileOneGateDecisionWake(r.Context(), issue, decision.ID)
	actor, _ := h.Queries.GetUser(r.Context(), actorID)
	writeJSON(w, http.StatusCreated, GateReviewDecisionEnvelope{
		Request: requestResponse(target), Decision: decisionResponse(decision, actor.Name), Wake: wakeResponse(wake),
	})
}

func (h *Handler) reconcileGateDecisionWakes(ctx context.Context, issue db.Issue) {
	wakes, err := h.Queries.ListPendingGateDecisionWakesForIssue(ctx, db.ListPendingGateDecisionWakesForIssueParams{
		WorkspaceID: issue.WorkspaceID, IssueID: issue.ID,
	})
	if err != nil {
		return
	}
	for _, wake := range wakes {
		_, _ = h.reconcileOneGateDecisionWake(ctx, issue, wake.DecisionID)
	}
}

func (h *Handler) reconcileOneGateDecisionWake(ctx context.Context, issue db.Issue, decisionID pgtype.UUID) (db.GateDecisionWake, error) {
	wake, err := h.Queries.GetGateDecisionWake(ctx, db.GetGateDecisionWakeParams{
		DecisionID: decisionID, WorkspaceID: issue.WorkspaceID, IssueID: issue.ID,
	})
	if err != nil || wake.State == "delivered" {
		return wake, err
	}
	task, err := h.Queries.GetAgentTaskByGateDecisionID(ctx, decisionID)
	if errors.Is(err, pgx.ErrNoRows) {
		// The decision endpoint is human-only, so the immutable decision actor is
		// the accountable user for the wake. Read it directly by id.
		decision, decisionErr := h.Queries.GetGateReviewDecisionInIssue(ctx, db.GetGateReviewDecisionInIssueParams{
			ID: decisionID, WorkspaceID: issue.WorkspaceID, IssueID: issue.ID,
		})
		if decisionErr != nil {
			return wake, decisionErr
		}
		task, err = h.TaskService.EnqueueTaskForGateDecision(ctx, issue, decisionID, decision.ActorID)
	}
	if err != nil {
		if raced, lookupErr := h.Queries.GetAgentTaskByGateDecisionID(ctx, decisionID); lookupErr == nil {
			task, err = raced, nil
		}
	}
	if err != nil {
		_ = h.Queries.RecordGateDecisionWakeFailure(ctx, db.RecordGateDecisionWakeFailureParams{
			DecisionID: decisionID, LastError: err.Error(),
		})
		return wake, err
	}
	delivered, err := h.Queries.MarkGateDecisionWakeDelivered(ctx, db.MarkGateDecisionWakeDeliveredParams{
		DecisionID: decisionID, TaskID: task.ID,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return h.Queries.GetGateDecisionWake(ctx, db.GetGateDecisionWakeParams{
			DecisionID: decisionID, WorkspaceID: issue.WorkspaceID, IssueID: issue.ID,
		})
	}
	return delivered, err
}
