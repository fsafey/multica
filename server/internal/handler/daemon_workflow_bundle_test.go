package handler

import (
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

func TestWorkflowBundleArtifactKeyIsUniquePerRequest(t *testing.T) {
	manifest := workflowBundleManifest{
		RunID:     "run-1",
		NodeID:    "node-1",
		AttemptID: "attempt-1",
	}

	first := workflowBundleArtifactKey("workspace-1", manifest, "digest-1")
	second := workflowBundleArtifactKey("workspace-1", manifest, "digest-1")

	if first == second {
		t.Fatalf("artifact keys must be request-unique: %q", first)
	}
	prefix := "workflows/workspace-1/run-1/node-1/attempt-1-digest-1-"
	for _, key := range []string{first, second} {
		if !strings.HasPrefix(key, prefix) || !strings.HasSuffix(key, ".bundle") {
			t.Errorf("artifact key %q does not preserve the workflow scope", key)
		}
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
