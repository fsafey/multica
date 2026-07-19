-- name: CreateGateReviewRequest :one
INSERT INTO gate_review_request (
    workspace_id, issue_id, gate, revision, subject_digest, review_data,
    actor_type, actor_id
)
VALUES (
    @workspace_id, @issue_id, @gate, @revision, @subject_digest, @review_data,
    @actor_type, @actor_id
)
RETURNING *;

-- name: GetLatestGateReviewRequest :one
SELECT * FROM gate_review_request
WHERE workspace_id = @workspace_id AND issue_id = @issue_id AND gate = @gate
ORDER BY revision DESC, created_at DESC, id DESC
LIMIT 1;

-- name: GetGateReviewRequestInIssue :one
SELECT * FROM gate_review_request
WHERE id = @id AND workspace_id = @workspace_id AND issue_id = @issue_id;

-- name: ListGateReviewRequestsForIssue :many
SELECT
    r.*,
    COALESCE(request_member.name, request_agent.name, r.actor_id::text) AS request_actor_name,
    d.id AS decision_id,
    d.outcome AS decision_outcome,
    d.reason AS decision_reason,
    d.actor_id AS decision_actor_id,
    d.created_at AS decision_created_at,
    u.name AS decision_actor_name,
    w.state AS wake_state,
    w.task_id AS wake_task_id
FROM gate_review_request r
LEFT JOIN "user" request_member
    ON r.actor_type = 'member' AND request_member.id = r.actor_id
LEFT JOIN agent request_agent
    ON r.actor_type = 'agent' AND request_agent.id = r.actor_id
LEFT JOIN gate_review_decision d ON d.request_id = r.id
LEFT JOIN "user" u ON u.id = d.actor_id
LEFT JOIN gate_decision_wake w ON w.decision_id = d.id
WHERE r.workspace_id = @workspace_id AND r.issue_id = @issue_id
ORDER BY r.created_at DESC, r.id DESC;

-- name: GetGateReviewDecisionByRequest :one
SELECT * FROM gate_review_decision
WHERE request_id = @request_id AND workspace_id = @workspace_id AND issue_id = @issue_id;

-- name: GetGateReviewDecisionInIssue :one
SELECT * FROM gate_review_decision
WHERE id = @id AND workspace_id = @workspace_id AND issue_id = @issue_id;

-- name: CreateGateReviewDecision :one
INSERT INTO gate_review_decision (
    workspace_id, issue_id, request_id, outcome, reason, actor_id
)
VALUES (
    @workspace_id, @issue_id, @request_id, @outcome, @reason, @actor_id
)
RETURNING *;

-- name: CreateGateDecisionWake :one
INSERT INTO gate_decision_wake (decision_id, workspace_id, issue_id)
VALUES (@decision_id, @workspace_id, @issue_id)
RETURNING *;

-- name: GetGateDecisionWake :one
SELECT * FROM gate_decision_wake
WHERE decision_id = @decision_id AND workspace_id = @workspace_id AND issue_id = @issue_id;

-- name: RecordGateDecisionWakeFailure :exec
UPDATE gate_decision_wake
SET attempt_count = attempt_count + 1,
    last_error = @last_error,
    next_attempt_at = now() + power(2, LEAST(attempt_count, 8)) * interval '1 second'
WHERE decision_id = @decision_id AND state = 'pending';

-- name: MarkGateDecisionWakeDelivered :one
UPDATE gate_decision_wake
SET state = 'delivered', task_id = @task_id, attempt_count = attempt_count + 1,
    last_error = '', delivered_at = now()
WHERE decision_id = @decision_id AND state = 'pending'
RETURNING *;

-- name: ListPendingGateDecisionWakesForIssue :many
SELECT * FROM gate_decision_wake
WHERE workspace_id = @workspace_id AND issue_id = @issue_id
  AND state = 'pending' AND next_attempt_at <= now()
ORDER BY created_at ASC, decision_id ASC;

-- name: GetNextPendingGateDecisionWake :one
SELECT * FROM gate_decision_wake
WHERE state = 'pending' AND next_attempt_at <= now()
ORDER BY next_attempt_at ASC, created_at ASC, decision_id ASC
LIMIT 1;

-- name: GetAgentTaskByGateDecisionID :one
SELECT * FROM agent_task_queue WHERE gate_decision_id = @gate_decision_id LIMIT 1;
