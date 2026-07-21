-- name: CreateTaskMessage :one
-- An identical retransmission returns the existing row with inserted=false.
-- A sequence collision with different content returns no row so the handler
-- can fail loudly without changing or rebroadcasting the original message.
WITH inserted AS (
    INSERT INTO task_message (task_id, seq, type, tool, content, input, output)
    VALUES ($1, $2, $3, $4, $5, $6, $7)
    ON CONFLICT (task_id, seq) DO NOTHING
    RETURNING *, TRUE AS inserted
), identical AS (
    SELECT *, FALSE AS inserted
    FROM task_message
    WHERE task_id = $1
      AND seq = $2
      AND type = $3
      AND tool IS NOT DISTINCT FROM $4
      AND content IS NOT DISTINCT FROM $5
      AND input IS NOT DISTINCT FROM $6
      AND output IS NOT DISTINCT FROM $7
)
SELECT * FROM inserted
UNION ALL
SELECT * FROM identical
LIMIT 1;

-- name: GetTaskMessageBySequence :one
SELECT * FROM task_message
WHERE task_id = $1 AND seq = $2;

-- name: ListTaskMessages :many
SELECT * FROM task_message
WHERE task_id = $1
ORDER BY seq ASC;

-- name: ListTaskMessageSequencesByArrival :many
SELECT seq FROM task_message
WHERE task_id = $1
ORDER BY arrival_order ASC;

-- name: ListTaskMessagesSince :many
SELECT * FROM task_message
WHERE task_id = $1 AND seq > $2
ORDER BY seq ASC;

-- name: DeleteTaskMessages :exec
DELETE FROM task_message
WHERE task_id = $1;
