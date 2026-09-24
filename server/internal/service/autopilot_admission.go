package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/dbid"
)

// createAutopilotRunWithAdmission prevents overlapping scheduled run_only
// executions of the same autopilot. Manual and webhook runs remain explicit
// operator or event actions and retain their existing overlap semantics.
//
// The autopilot row lock makes the active-run check and insert one atomic
// admission decision across server replicas. A rejected occurrence still gets
// a terminal skipped row, preserving schedule idempotency and run history
// without creating a task.
func (s *AutopilotService) createAutopilotRunWithAdmission(
	ctx context.Context,
	autopilot db.Autopilot,
	triggerID pgtype.UUID,
	source string,
	payload []byte,
	plannedAt pgtype.Timestamptz,
	webhookDeliveryID pgtype.UUID,
	initialStatus string,
) (db.AutopilotRun, autopilotAdmissionOutcome, error) {
	params := db.CreateAutopilotRunParams{
		ID:                dbid.NewV7(),
		AutopilotID:       autopilot.ID,
		TriggerID:         triggerID,
		Source:            source,
		Status:            initialStatus,
		TriggerPayload:    payload,
		SquadID:           autopilotSquadAttribution(autopilot),
		PlannedAt:         plannedAt,
		WebhookDeliveryID: webhookDeliveryID,
	}
	if source != "schedule" || autopilot.ExecutionMode != "run_only" {
		run, err := s.Queries.CreateAutopilotRun(ctx, params)
		return run, autopilotAdmissionCreated, err
	}

	tx, err := s.TxStarter.Begin(ctx)
	if err != nil {
		return db.AutopilotRun{}, autopilotAdmissionCreated, fmt.Errorf("begin admission tx: %w", err)
	}
	defer tx.Rollback(ctx)
	qtx := s.Queries.WithTx(tx)

	if _, err := qtx.LockAutopilotRunAdmission(ctx, autopilot.ID); err != nil {
		return db.AutopilotRun{}, autopilotAdmissionCreated, fmt.Errorf("lock autopilot admission: %w", err)
	}
	if triggerID.Valid && plannedAt.Valid {
		existing, err := qtx.GetAutopilotRunByTriggerAndPlanned(ctx, db.GetAutopilotRunByTriggerAndPlannedParams{
			TriggerID: triggerID,
			PlannedAt: plannedAt,
		})
		switch {
		case err == nil:
			return existing, autopilotAdmissionReused, nil
		case !errors.Is(err, pgx.ErrNoRows):
			return db.AutopilotRun{}, autopilotAdmissionCreated, fmt.Errorf("check planned autopilot admission: %w", err)
		}
	}
	graceSeconds := autopilotAdmissionPartialGrace.Seconds()
	recovered, err := qtx.RecoverStalePartialAutopilotRunsForAdmission(
		ctx,
		db.RecoverStalePartialAutopilotRunsForAdmissionParams{
			AutopilotID:      autopilot.ID,
			PartialGraceSecs: graceSeconds,
		},
	)
	if err != nil {
		return db.AutopilotRun{}, autopilotAdmissionCreated, fmt.Errorf("recover stale autopilot admission: %w", err)
	}

	active, err := qtx.GetLiveAutopilotRun(ctx, db.GetLiveAutopilotRunParams{
		AutopilotID:      autopilot.ID,
		PartialGraceSecs: graceSeconds,
	})
	switch {
	case err == nil:
		params.Status = "skipped"
		run, createErr := qtx.CreateAutopilotRun(ctx, params)
		if createErr != nil {
			return db.AutopilotRun{}, autopilotAdmissionCreated, fmt.Errorf("create non-overlap receipt: %w", createErr)
		}
		reason := "active autopilot run: " + util.UUIDToString(active.ID)
		run, err = qtx.UpdateAutopilotRunSkipped(ctx, db.UpdateAutopilotRunSkippedParams{
			ID:            run.ID,
			FailureReason: pgtype.Text{String: reason, Valid: true},
			ReasonCode:    pgtype.Text{String: "already_active", Valid: true},
		})
		if err != nil {
			return db.AutopilotRun{}, autopilotAdmissionCreated, fmt.Errorf("finalize non-overlap receipt: %w", err)
		}
		if err := tx.Commit(ctx); err != nil {
			return db.AutopilotRun{}, autopilotAdmissionCreated, fmt.Errorf("commit non-overlap receipt: %w", err)
		}
		s.reportRecoveredAutopilotRuns(autopilot, recovered)
		return run, autopilotAdmissionSkipped, nil
	case !errors.Is(err, pgx.ErrNoRows):
		return db.AutopilotRun{}, autopilotAdmissionCreated, fmt.Errorf("check active autopilot run: %w", err)
	}

	key := "schedule:" + util.UUIDToString(triggerID) + ":" + plannedAt.Time.UTC().Format(time.RFC3339Nano)
	// The autopilot row lock serializes scheduled admission. A separate quota
	// transaction may insert the linked run while this lock is held; NO KEY
	// UPDATE permits its foreign-key KEY SHARE lock on the autopilot row.
	run, reused, err := s.createAutopilotRunWithQuota(ctx, autopilot.WorkspaceID, source, key, params)
	if err != nil {
		return db.AutopilotRun{}, autopilotAdmissionCreated, err
	}
	if err := tx.Commit(ctx); err != nil {
		return db.AutopilotRun{}, autopilotAdmissionCreated, fmt.Errorf("commit admitted run: %w", err)
	}
	s.reportRecoveredAutopilotRuns(autopilot, recovered)
	if reused {
		return run, autopilotAdmissionReused, nil
	}
	return run, autopilotAdmissionCreated, nil
}

func (s *AutopilotService) reportRecoveredAutopilotRuns(autopilot db.Autopilot, recovered []db.AutopilotRun) {
	for _, stale := range recovered {
		reason := stale.FailureReason.String
		slog.Warn("autopilot admission recovered stale partial run",
			"autopilot_id", util.UUIDToString(autopilot.ID),
			"run_id", util.UUIDToString(stale.ID),
		)
		s.captureAutopilotRunFailed(autopilot, stale, stale.Source, reason)
		s.publishRunDone(util.UUIDToString(autopilot.WorkspaceID), stale, "failed")
	}
}
