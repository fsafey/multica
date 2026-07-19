package handler

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

const gateDecisionWakePollInterval = time.Second

// GateDecisionWakeWorker reconciles the durable wake outbox independently of
// the HTTP request that created a decision. The unique gate_decision_id task
// provenance makes concurrent workers and crash-after-enqueue retries safe.
type GateDecisionWakeWorker struct {
	h      *Handler
	notify chan struct{}
	done   chan struct{}
}

func NewGateDecisionWakeWorker(h *Handler) *GateDecisionWakeWorker {
	return &GateDecisionWakeWorker{h: h, notify: make(chan struct{}, 1), done: make(chan struct{})}
}

func (w *GateDecisionWakeWorker) Notify() {
	if w == nil {
		return
	}
	select {
	case w.notify <- struct{}{}:
	default:
	}
}

func (w *GateDecisionWakeWorker) Run(ctx context.Context) {
	if w == nil {
		return
	}
	defer close(w.done)
	ticker := time.NewTicker(gateDecisionWakePollInterval)
	defer ticker.Stop()
	for {
		worked, err := w.ProcessNext(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			slog.Warn("gate decision wake worker: reconcile", "error", err)
		}
		if worked && err == nil {
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-w.notify:
		case <-ticker.C:
		}
	}
}

func (w *GateDecisionWakeWorker) ProcessNext(ctx context.Context) (bool, error) {
	if w == nil || w.h == nil || w.h.Queries == nil {
		return false, nil
	}
	wake, err := w.h.Queries.GetNextPendingGateDecisionWake(ctx)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	issue, err := w.h.Queries.GetIssueInWorkspace(ctx, db.GetIssueInWorkspaceParams{
		ID: wake.IssueID, WorkspaceID: wake.WorkspaceID,
	})
	if err != nil {
		_ = w.h.Queries.RecordGateDecisionWakeFailure(ctx, db.RecordGateDecisionWakeFailureParams{
			DecisionID: wake.DecisionID,
			LastError:  err.Error(),
		})
		return true, err
	}
	_, err = w.h.reconcileOneGateDecisionWake(ctx, issue, wake.DecisionID)
	return true, err
}

func (w *GateDecisionWakeWorker) WaitWithTimeout(timeout time.Duration) bool {
	if w == nil {
		return true
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-w.done:
		return true
	case <-timer.C:
		return false
	}
}
