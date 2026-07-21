-- name: CreateTaskExecutionEvidence :one
INSERT INTO task_execution_evidence (task_id, schema_version, payload, payload_hash)
VALUES ($1, $2, $3, $4)
ON CONFLICT (task_id) DO NOTHING
RETURNING *;

-- name: GetTaskExecutionEvidence :one
SELECT * FROM task_execution_evidence
WHERE task_id = $1;

-- name: SetTaskTranscriptExpectation :one
UPDATE agent_task_queue
SET transcript_expected_message_count = $2,
    transcript_expected_last_seq = $3,
    transcript_delivery_confirmed = $4
WHERE id = $1
  AND (
      (
          transcript_expected_message_count IS NULL
          AND transcript_expected_last_seq IS NULL
          AND transcript_delivery_confirmed IS NULL
      )
      OR (
          transcript_expected_message_count = $2
          AND transcript_expected_last_seq = $3
          AND transcript_delivery_confirmed = $4
      )
  )
RETURNING *;
