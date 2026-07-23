-- name: CreateRuntimePool :one
INSERT INTO runtime_pool (
    workspace_id, name, enabled, max_inflight, affinity_grace_seconds,
    lease_seconds, created_by
)
VALUES (
    @workspace_id, @name, @enabled, @max_inflight,
    @affinity_grace_seconds, @lease_seconds, @created_by
)
RETURNING *;

-- name: GetRuntimePool :one
SELECT * FROM runtime_pool
WHERE id = @id;

-- name: GetRuntimePoolInWorkspace :one
SELECT * FROM runtime_pool
WHERE id = @id AND workspace_id = @workspace_id;

-- name: ListRuntimePools :many
SELECT * FROM runtime_pool
WHERE workspace_id = @workspace_id
ORDER BY name ASC;

-- name: UpdateRuntimePool :one
UPDATE runtime_pool
SET name = @name,
    enabled = @enabled,
    max_inflight = @max_inflight,
    affinity_grace_seconds = @affinity_grace_seconds,
    lease_seconds = @lease_seconds,
    updated_at = now()
WHERE id = @id AND workspace_id = @workspace_id
RETURNING *;

-- name: DeleteRuntimePool :execrows
DELETE FROM runtime_pool
WHERE id = @id AND workspace_id = @workspace_id;

-- name: AddRuntimeToPool :one
INSERT INTO runtime_pool_runtime (pool_id, runtime_id, priority, enabled)
VALUES (@pool_id, @runtime_id, @priority, @enabled)
ON CONFLICT (pool_id, runtime_id)
DO UPDATE SET priority = EXCLUDED.priority, enabled = EXCLUDED.enabled
RETURNING *;

-- name: RemoveRuntimeFromPool :execrows
DELETE FROM runtime_pool_runtime
WHERE pool_id = @pool_id AND runtime_id = @runtime_id;

-- name: ListRuntimePoolRuntimes :many
SELECT rpr.*
FROM runtime_pool_runtime rpr
JOIN runtime_pool rp ON rp.id = rpr.pool_id
WHERE rpr.pool_id = @pool_id
  AND rp.workspace_id = @workspace_id
ORDER BY rpr.priority DESC, rpr.created_at ASC;

-- name: BindAgentToRuntimePool :one
INSERT INTO agent_runtime_pool (agent_id, pool_id, enabled)
VALUES (@agent_id, @pool_id, @enabled)
ON CONFLICT (agent_id, pool_id)
DO UPDATE SET enabled = EXCLUDED.enabled
RETURNING *;

-- name: UnbindAgentFromRuntimePool :execrows
DELETE FROM agent_runtime_pool
WHERE agent_id = @agent_id AND pool_id = @pool_id;

-- name: ListAgentRuntimePools :many
SELECT arp.*
FROM agent_runtime_pool arp
JOIN runtime_pool rp ON rp.id = arp.pool_id
WHERE arp.agent_id = @agent_id
  AND rp.workspace_id = @workspace_id
ORDER BY arp.created_at ASC;

-- name: CreateWorkflowRun :one
INSERT INTO workflow_run (
    workspace_id, project_id, anchor_issue_id, graph_key, graph_version,
    status, integration_pool_id, wip_limit, human_gate_limit,
    input_digest, law_digest, metadata, created_by
)
VALUES (
    @workspace_id, @project_id, @anchor_issue_id, @graph_key, @graph_version,
    @status, sqlc.narg(integration_pool_id), @wip_limit, @human_gate_limit,
    sqlc.narg(input_digest), sqlc.narg(law_digest), @metadata, @created_by
)
RETURNING *;

-- name: GetWorkflowRun :one
SELECT * FROM workflow_run
WHERE id = @id;

-- name: GetWorkflowRunInWorkspace :one
SELECT * FROM workflow_run
WHERE id = @id AND workspace_id = @workspace_id;

-- name: ListWorkflowRuns :many
SELECT * FROM workflow_run
WHERE workspace_id = @workspace_id
ORDER BY created_at DESC;

-- name: SetWorkflowRunStatus :one
UPDATE workflow_run
SET status = @status,
    updated_at = now(),
    completed_at = CASE
        WHEN @status::text = 'completed' THEN now()
        ELSE completed_at
    END
WHERE id = @id AND workspace_id = @workspace_id
RETURNING *;

-- name: PauseWorkflowRun :one
UPDATE workflow_run
SET status = 'paused',
    updated_at = now()
WHERE id = @id
  AND workspace_id = @workspace_id
  AND status = 'running'
RETURNING *;

-- name: ResumeWorkflowRun :one
UPDATE workflow_run
SET status = 'running',
    updated_at = now()
WHERE id = @id
  AND workspace_id = @workspace_id
  AND status = 'paused'
RETURNING *;

-- name: CompleteWorkflowRunIfTerminal :one
UPDATE workflow_run run
SET status = 'completed',
    completed_at = now(),
    updated_at = now()
WHERE run.id = @run_id
  AND run.status IN ('running', 'paused')
  AND NOT EXISTS (
      SELECT 1
      FROM workflow_node node
      WHERE node.run_id = run.id
        AND node.state <> 'completed'
  )
RETURNING run.*;

-- name: CancelWorkflowRunAttempts :execrows
UPDATE workflow_node_attempt attempt
SET status = 'cancelled',
    error = @error,
    completed_at = now()
FROM workflow_node node
WHERE attempt.node_id = node.id
  AND node.run_id = @run_id
  AND attempt.status IN ('claimed', 'running', 'submitted');

-- name: CountProcessingWorkflowIntegrationEventsForRun :one
SELECT count(*)
FROM workflow_outbox
WHERE run_id = @run_id
  AND status = 'processing'
  AND event_type IN ('workflow.artifact_submitted', 'workflow.deterministic_ready');

-- name: CancelWorkflowRunTasks :execrows
UPDATE agent_task_queue task
SET status = 'cancelled',
    error = @error,
    completed_at = now()
FROM workflow_node node
WHERE task.workflow_node_id = node.id
  AND node.run_id = @run_id
  AND task.status IN ('queued', 'dispatched', 'waiting_local_directory', 'running');

-- name: ReleaseWorkflowRunResources :execrows
DELETE FROM workflow_resource_claim claim
USING workflow_node node
WHERE claim.node_id = node.id
  AND node.run_id = @run_id;

-- name: CancelWorkflowRunOutbox :execrows
UPDATE workflow_outbox
SET status = 'failed',
    last_error = @error,
    completed_at = now()
WHERE run_id = @run_id
  AND status IN ('pending', 'processing');

-- name: CancelWorkflowRunNodes :execrows
UPDATE workflow_node
SET state = 'cancelled',
    updated_at = now()
WHERE run_id = @run_id
  AND state NOT IN ('completed', 'cancelled');

-- name: CancelWorkflowRun :one
UPDATE workflow_run
SET status = 'cancelled',
    completed_at = now(),
    updated_at = now()
WHERE id = @id
  AND workspace_id = @workspace_id
  AND status IN ('running', 'paused')
RETURNING *;

-- name: CreateWorkflowNode :one
INSERT INTO workflow_node (
    run_id, issue_id, passage_key, node_key, generation, executor_kind,
    agent_id, runtime_pool_id, state, priority, preferred_daemon_id,
    stealable_at, input_digest, law_digest, output_contract, max_attempts,
    ready_at
)
VALUES (
    @run_id, @issue_id, @passage_key, @node_key, @generation,
    @executor_kind, sqlc.narg(agent_id), sqlc.narg(runtime_pool_id),
    @state, @priority, sqlc.narg(preferred_daemon_id),
    sqlc.narg(stealable_at), sqlc.narg(input_digest), sqlc.narg(law_digest),
    @output_contract, @max_attempts, sqlc.narg(ready_at)
)
RETURNING *;

-- name: CreateWorkflowNodeDependency :one
INSERT INTO workflow_node_dependency (node_id, depends_on_node_id)
VALUES (@node_id, @depends_on_node_id)
ON CONFLICT (node_id, depends_on_node_id) DO NOTHING
RETURNING *;

-- name: CreateWorkflowNodeResource :one
INSERT INTO workflow_node_resource (node_id, resource_key, mode)
VALUES (@node_id, @resource_key, @mode)
ON CONFLICT (node_id, resource_key)
DO UPDATE SET mode = EXCLUDED.mode
RETURNING *;

-- name: ListWorkflowNodes :many
SELECT * FROM workflow_node
WHERE run_id = @run_id
ORDER BY passage_key ASC, created_at ASC;

-- name: GetWorkflowNode :one
SELECT * FROM workflow_node
WHERE id = @id;

-- name: ListWorkflowNodeDependencies :many
SELECT * FROM workflow_node_dependency
WHERE node_id = @node_id
ORDER BY created_at ASC;

-- name: ListWorkflowNodeResources :many
SELECT * FROM workflow_node_resource
WHERE node_id = @node_id
ORDER BY resource_key ASC;

-- name: ListWorkflowNodeAttempts :many
SELECT * FROM workflow_node_attempt
WHERE node_id = @node_id
ORDER BY claim_epoch ASC;

-- name: GetWorkflowRunMetrics :one
WITH node_metrics AS (
    SELECT
        count(*)::int AS total_nodes,
        count(*) FILTER (WHERE state = 'completed')::int AS completed_nodes,
        count(*) FILTER (WHERE state = 'waiting_human')::int AS waiting_human_nodes,
        count(*) FILTER (WHERE state IN ('blocked', 'failed'))::int AS blocked_nodes
    FROM workflow_node n
    WHERE n.run_id = sqlc.arg(workflow_run_id)
),
attempt_metrics AS (
    SELECT
        count(*)::int AS attempt_count,
        count(*) FILTER (
            WHERE a.preferred_daemon_at_claim IS NOT NULL
              AND a.daemon_id = a.preferred_daemon_at_claim
        )::int AS affinity_retained_attempts,
        count(*) FILTER (WHERE a.affinity_stolen)::int AS stolen_attempts,
        count(*) FILTER (
            WHERE t.session_id IS NOT NULL
              AND EXISTS (
                  SELECT 1
                  FROM agent_task_queue prior
                  WHERE prior.id <> t.id
                    AND prior.created_at < t.created_at
                    AND prior.agent_id = t.agent_id
                    AND prior.issue_id = t.issue_id
                    AND prior.runtime_id = t.runtime_id
                    AND prior.workflow_input_digest IS NOT DISTINCT FROM t.workflow_input_digest
                    AND prior.workflow_law_digest IS NOT DISTINCT FROM t.workflow_law_digest
                    AND prior.session_id = t.session_id
                    AND prior.status = 'completed'
              )
        )::int AS session_resume_count
    FROM workflow_node_attempt a
    JOIN workflow_node n ON n.id = a.node_id
    LEFT JOIN agent_task_queue t ON t.id = a.task_id
    WHERE n.run_id = sqlc.arg(workflow_run_id)
),
result_metrics AS (
    SELECT
        count(*)::int AS accepted_result_count,
        COALESCE(
            avg(EXTRACT(EPOCH FROM (r.accepted_at - a.claimed_at))),
            0
        )::double precision AS avg_node_latency_seconds
    FROM workflow_node_result r
    JOIN workflow_node n ON n.id = r.node_id
    JOIN workflow_node_attempt a ON a.id = r.attempt_id
    WHERE n.run_id = sqlc.arg(workflow_run_id)
),
usage_metrics AS (
    SELECT
        COALESCE(sum(tu.input_tokens), 0)::bigint AS input_tokens,
        COALESCE(sum(tu.output_tokens), 0)::bigint AS output_tokens,
        COALESCE(sum(tu.cache_read_tokens), 0)::bigint AS cache_read_tokens,
        COALESCE(sum(tu.cache_write_tokens), 0)::bigint AS cache_write_tokens
    FROM task_usage tu
    JOIN agent_task_queue t ON t.id = tu.task_id
    JOIN workflow_node n ON n.id = t.workflow_node_id
    WHERE n.run_id = sqlc.arg(workflow_run_id)
)
SELECT
    node_metrics.*,
    attempt_metrics.*,
    result_metrics.*,
    usage_metrics.*,
    CAST(CASE
        WHEN (
            usage_metrics.input_tokens
            + usage_metrics.cache_read_tokens
            + usage_metrics.cache_write_tokens
        ) = 0 THEN 0::double precision
        ELSE usage_metrics.cache_read_tokens::double precision / (
            usage_metrics.input_tokens
            + usage_metrics.cache_read_tokens
            + usage_metrics.cache_write_tokens
        )::double precision
    END AS double precision) AS cached_input_token_ratio
FROM node_metrics, attempt_metrics, result_metrics, usage_metrics;

-- name: ListWorkflowRunUsageByModel :many
SELECT
    lower(tu.provider) AS provider,
    tu.model,
    sum(tu.input_tokens)::bigint AS input_tokens,
    sum(tu.output_tokens)::bigint AS output_tokens,
    sum(tu.cache_read_tokens)::bigint AS cache_read_tokens,
    sum(tu.cache_write_tokens)::bigint AS cache_write_tokens,
    count(DISTINCT tu.task_id)::int AS task_count
FROM task_usage tu
JOIN agent_task_queue t ON t.id = tu.task_id
JOIN workflow_node n ON n.id = t.workflow_node_id
WHERE n.run_id = sqlc.arg(workflow_run_id)
GROUP BY lower(tu.provider), tu.model
ORDER BY lower(tu.provider), tu.model;

-- name: IsWorkflowManagedIssue :one
SELECT EXISTS (
    SELECT 1
    FROM workflow_node n
    JOIN workflow_run r ON r.id = n.run_id
    WHERE n.issue_id = @issue_id
      AND r.workspace_id = @workspace_id
      AND r.status IN ('running', 'paused')
) AS managed;

-- name: IsWorkflowManagedProductionAgent :one
SELECT EXISTS (
    SELECT 1
    FROM workflow_node n
    JOIN workflow_run r ON r.id = n.run_id
    WHERE n.issue_id = @issue_id
      AND n.agent_id = @agent_id
      AND r.workspace_id = @workspace_id
      AND r.status IN ('running', 'paused')
) AS managed;

-- name: AcquireWorkflowClaimLock :exec
SELECT pg_advisory_xact_lock(hashtextextended('multica-workflow-node-claim-v1', 0));

-- name: SelectWorkflowNodeClaimCandidate :one
SELECT
    n.*,
    selected.runtime_id AS selected_runtime_id,
    rp.lease_seconds,
    rp.affinity_grace_seconds,
    wr.created_by AS run_created_by
FROM workflow_node n
JOIN workflow_run wr ON wr.id = n.run_id
JOIN runtime_pool rp ON rp.id = n.runtime_pool_id
JOIN agent a ON a.id = n.agent_id
JOIN agent_runtime default_runtime ON default_runtime.id = a.runtime_id
JOIN LATERAL (
    SELECT rpr.runtime_id
    FROM runtime_pool_runtime rpr
    JOIN agent_runtime ar ON ar.id = rpr.runtime_id
    WHERE rpr.pool_id = n.runtime_pool_id
      AND rpr.enabled = TRUE
      AND rpr.runtime_id = ANY(@runtime_ids::uuid[])
      AND ar.workspace_id = wr.workspace_id
      AND ar.status = 'online'
      AND ar.provider = default_runtime.provider
    ORDER BY
        CASE WHEN ar.daemon_id = @daemon_id THEN 1 ELSE 0 END DESC,
        rpr.priority DESC,
        rpr.created_at ASC
    LIMIT 1
) selected ON TRUE
WHERE n.state = 'ready'
  AND n.executor_kind = 'agent'
  AND wr.status = 'running'
  AND rp.enabled = TRUE
  AND EXISTS (
      SELECT 1
      FROM agent_runtime_pool arp
      WHERE arp.agent_id = n.agent_id
        AND arp.pool_id = n.runtime_pool_id
        AND arp.enabled = TRUE
  )
  AND (
      n.preferred_daemon_id IS NULL
      OR n.preferred_daemon_id = @daemon_id
      OR n.stealable_at <= now()
      OR NOT EXISTS (
          SELECT 1
          FROM runtime_pool_runtime home_member
          JOIN agent_runtime home_runtime ON home_runtime.id = home_member.runtime_id
          WHERE home_member.pool_id = n.runtime_pool_id
            AND home_member.enabled = TRUE
            AND home_runtime.daemon_id = n.preferred_daemon_id
            AND home_runtime.status = 'online'
            AND home_runtime.last_seen_at >= now() - interval '45 seconds'
      )
  )
  AND (
      SELECT count(*)
      FROM workflow_node active
      WHERE active.runtime_pool_id = n.runtime_pool_id
        AND active.state IN ('claimed', 'running', 'submitted', 'integrating')
  ) < rp.max_inflight
  AND (
      SELECT count(*)
      FROM workflow_node active
      WHERE active.run_id = n.run_id
        AND active.state IN ('claimed', 'running', 'submitted', 'integrating')
  ) < wr.wip_limit
  AND (
      SELECT count(*)
      FROM workflow_node gate
      WHERE gate.run_id = n.run_id
        AND gate.state = 'waiting_human'
  ) < wr.human_gate_limit
  AND (
      SELECT count(*)
      FROM agent_task_queue active
      WHERE active.agent_id = n.agent_id
        AND active.status IN ('dispatched', 'waiting_local_directory', 'running')
  ) < a.max_concurrent_tasks
  AND NOT EXISTS (
      SELECT 1
      FROM workflow_node_resource required
      JOIN workflow_resource_claim held
        ON held.resource_key = required.resource_key
      WHERE required.node_id = n.id
  )
ORDER BY
    CASE WHEN n.preferred_daemon_id = @daemon_id THEN 1 ELSE 0 END DESC,
    (
        SELECT count(*)
        FROM agent_task_queue active
        WHERE active.agent_id = n.agent_id
          AND active.status IN ('dispatched', 'waiting_local_directory', 'running')
    ) ASC,
    n.priority DESC,
    n.ready_at ASC NULLS LAST,
    n.created_at ASC
LIMIT 1
FOR UPDATE OF n SKIP LOCKED;

-- name: ClaimWorkflowNode :one
UPDATE workflow_node
SET state = 'claimed',
    claim_epoch = claim_epoch + 1,
    current_attempt_id = @attempt_id,
    preferred_daemon_id = COALESCE(preferred_daemon_id, @daemon_id),
    started_at = COALESCE(started_at, now()),
    updated_at = now()
WHERE id = @id AND state = 'ready'
RETURNING *;

-- name: CreateWorkflowNodeAttempt :one
INSERT INTO workflow_node_attempt (
    id, node_id, claim_epoch, runtime_id, daemon_id,
    preferred_daemon_at_claim, affinity_stolen, status, lease_expires_at
)
VALUES (
    @id, @node_id, @claim_epoch, @runtime_id, @daemon_id,
    sqlc.narg(preferred_daemon_at_claim),
    COALESCE(sqlc.narg(affinity_stolen)::boolean, FALSE),
    'claimed',
    now() + make_interval(secs => @lease_seconds::double precision)
)
RETURNING *;

-- name: SelectDeterministicWorkflowNodeCandidate :one
SELECT
    n.*,
    selected.runtime_id AS selected_runtime_id,
    rp.lease_seconds
FROM workflow_node n
JOIN workflow_run wr ON wr.id = n.run_id
JOIN runtime_pool rp ON rp.id = wr.integration_pool_id
JOIN LATERAL (
    SELECT rpr.runtime_id
    FROM runtime_pool_runtime rpr
    JOIN agent_runtime ar ON ar.id = rpr.runtime_id
    WHERE rpr.pool_id = wr.integration_pool_id
      AND rpr.enabled = TRUE
      AND rpr.runtime_id = ANY(@runtime_ids::uuid[])
      AND ar.workspace_id = wr.workspace_id
      AND ar.daemon_id = @daemon_id
      AND ar.status = 'online'
      AND ar.last_seen_at >= now() - interval '45 seconds'
    ORDER BY rpr.priority DESC, rpr.created_at ASC
    LIMIT 1
) selected ON TRUE
WHERE n.executor_kind = 'deterministic'
  AND n.state = 'ready'
  AND wr.status = 'running'
  AND rp.enabled = TRUE
  AND NOT EXISTS (
      SELECT 1
      FROM workflow_node_dependency d
      JOIN workflow_node dependency ON dependency.id = d.depends_on_node_id
      WHERE d.node_id = n.id AND dependency.state <> 'completed'
  )
  AND (
      SELECT count(*)
      FROM workflow_node active
      WHERE active.run_id = n.run_id
        AND active.state IN ('claimed', 'running', 'submitted', 'integrating')
  ) < wr.wip_limit
  AND NOT EXISTS (
      SELECT 1
      FROM workflow_node_resource required
      JOIN workflow_resource_claim held
        ON held.resource_key = required.resource_key
      WHERE required.node_id = n.id
  )
ORDER BY n.priority DESC, n.ready_at ASC NULLS LAST, n.created_at ASC
LIMIT 1
FOR UPDATE OF n SKIP LOCKED;

-- name: ClaimDeterministicWorkflowNode :one
UPDATE workflow_node
SET state = 'submitted',
    claim_epoch = claim_epoch + 1,
    current_attempt_id = @attempt_id,
    started_at = COALESCE(started_at, now()),
    updated_at = now()
WHERE id = @id
  AND executor_kind = 'deterministic'
  AND state = 'ready'
RETURNING *;

-- name: CreateDeterministicWorkflowNodeAttempt :one
INSERT INTO workflow_node_attempt (
    id, node_id, claim_epoch, runtime_id, daemon_id, status,
    lease_expires_at, artifact_digest, manifest, submitted_at, completed_at
)
VALUES (
    @id, @node_id, @claim_epoch, @runtime_id, @daemon_id, 'submitted',
    now() + make_interval(secs => @lease_seconds::double precision),
    @artifact_digest, @manifest, now(), now()
)
RETURNING *;

-- name: CreateImportedWorkflowNodeAttempt :one
INSERT INTO workflow_node_attempt (
    id, node_id, claim_epoch, runtime_id, daemon_id, status,
    lease_expires_at, base_commit, result_commit, artifact_digest,
    manifest, claimed_at, started_at, submitted_at, completed_at
)
VALUES (
    @id, @node_id, @claim_epoch, NULL, 'legacy-import', 'integrated',
    now(), @canonical_commit, @canonical_commit, @artifact_digest,
    @manifest, now(), now(), now(), now()
)
RETURNING *;

-- name: CompleteImportedWorkflowNode :one
UPDATE workflow_node
SET state = 'completed',
    claim_epoch = @claim_epoch,
    current_attempt_id = @attempt_id,
    started_at = COALESCE(started_at, now()),
    completed_at = now(),
    updated_at = now()
WHERE id = @node_id
  AND state IN ('pending', 'ready', 'waiting_human')
RETURNING *;

-- name: CreateImportedWorkflowNodeResult :one
INSERT INTO workflow_node_result (
    node_id, attempt_id, generation, claim_epoch, canonical_commit,
    artifact_digest, manifest
)
SELECT
    n.id, @attempt_id, n.generation, @claim_epoch, @canonical_commit,
    @artifact_digest, @manifest
FROM workflow_node n
WHERE n.id = @node_id
ON CONFLICT (node_id) DO NOTHING
RETURNING *;

-- name: ClaimWorkflowNodeResources :execrows
INSERT INTO workflow_resource_claim (resource_key, node_id, attempt_id)
SELECT resource_key, @node_id, @attempt_id
FROM workflow_node_resource
WHERE node_id = @node_id AND mode = 'exclusive'
ON CONFLICT (resource_key) DO NOTHING;

-- name: CountWorkflowNodeExclusiveResources :one
SELECT count(*) FROM workflow_node_resource
WHERE node_id = @node_id AND mode = 'exclusive';

-- name: CountWorkflowAttemptResourceClaims :one
SELECT count(*) FROM workflow_resource_claim
WHERE node_id = @node_id AND attempt_id = @attempt_id;

-- name: CreateWorkflowAgentTask :one
INSERT INTO agent_task_queue (
    agent_id, runtime_id, issue_id, status, dispatched_at,
    prepare_lease_expires_at, priority, context,
    force_fresh_session, max_attempts, originator_user_id,
    accountable_user_id, originator_source, trigger_evidence_kind,
    trigger_evidence_ref_id, workflow_node_id, workflow_attempt_id,
    workflow_claim_epoch, workflow_input_digest, workflow_law_digest
)
SELECT
    n.agent_id,
    @runtime_id,
    n.issue_id,
    'dispatched',
    now(),
    now() + interval '45 seconds',
    n.priority,
    jsonb_build_object(
        'type', 'workflow_node',
        'workflow_run_id', n.run_id,
        'workflow_node_id', n.id,
        'passage_key', n.passage_key,
        'node_key', n.node_key,
        'generation', n.generation,
        'claim_epoch', n.claim_epoch,
        'output_contract', n.output_contract
    ),
    NOT EXISTS (
        SELECT 1
        FROM agent_task_queue prior
        WHERE prior.agent_id = n.agent_id
          AND prior.issue_id = n.issue_id
          AND prior.runtime_id = @runtime_id
          AND prior.workflow_input_digest IS NOT DISTINCT FROM n.input_digest
          AND prior.workflow_law_digest IS NOT DISTINCT FROM n.law_digest
          AND prior.session_id IS NOT NULL
          AND prior.status = 'completed'
    ),
    1,
    wr.created_by,
    wr.created_by,
    'workflow_run',
    'workflow_node',
    n.id,
    n.id,
    @attempt_id,
    n.claim_epoch,
    n.input_digest,
    n.law_digest
FROM workflow_node n
JOIN workflow_run wr ON wr.id = n.run_id
WHERE n.id = @node_id
  AND n.current_attempt_id = @attempt_id
  AND n.state = 'claimed'
RETURNING *;

-- name: AttachTaskToWorkflowAttempt :one
UPDATE workflow_node_attempt
SET task_id = @task_id
WHERE id = @attempt_id
  AND node_id = @node_id
  AND task_id IS NULL
RETURNING *;

-- name: GetWorkflowAttemptByTask :one
SELECT a.*
FROM workflow_node_attempt a
WHERE a.task_id = @task_id;

-- name: GetWorkflowTaskLeaseContext :one
SELECT
    a.*,
    n.run_id,
    n.state AS node_state,
    rp.lease_seconds
FROM workflow_node_attempt a
JOIN workflow_node n ON n.id = a.node_id
JOIN runtime_pool rp ON rp.id = n.runtime_pool_id
WHERE a.task_id = @task_id;

-- name: GetWorkflowNodeForTask :one
SELECT n.*
FROM workflow_node n
JOIN agent_task_queue t ON t.workflow_node_id = n.id
WHERE t.id = @task_id;

-- name: GetLastCompatibleWorkflowTaskSession :one
SELECT session_id, work_dir, runtime_id
FROM agent_task_queue
WHERE agent_id = @agent_id
  AND issue_id = @issue_id
  AND runtime_id = @runtime_id
  AND workflow_node_id IS NOT NULL
  AND workflow_input_digest IS NOT DISTINCT FROM sqlc.narg(input_digest)::text
  AND workflow_law_digest IS NOT DISTINCT FROM sqlc.narg(law_digest)::text
  AND session_id IS NOT NULL
  AND status = 'completed'
ORDER BY completed_at DESC NULLS LAST, created_at DESC
LIMIT 1;

-- name: StartWorkflowAttemptByTask :one
UPDATE workflow_node_attempt a
SET status = 'running',
    started_at = COALESCE(a.started_at, now()),
    lease_expires_at = now() + make_interval(secs => @lease_seconds::double precision)
FROM workflow_node n
WHERE a.task_id = @task_id
  AND a.node_id = n.id
  AND a.id = n.current_attempt_id
  AND a.claim_epoch = n.claim_epoch
  AND a.status = 'claimed'
  AND n.state = 'claimed'
RETURNING a.*;

-- name: MarkWorkflowNodeRunning :one
UPDATE workflow_node n
SET state = 'running', updated_at = now()
FROM workflow_node_attempt a
WHERE a.task_id = @task_id
  AND a.node_id = n.id
  AND a.id = n.current_attempt_id
  AND a.claim_epoch = n.claim_epoch
  AND n.state = 'claimed'
RETURNING n.*;

-- name: RenewWorkflowAttemptLease :one
UPDATE workflow_node_attempt a
SET lease_expires_at = now() + make_interval(secs => @lease_seconds::double precision)
FROM workflow_node n
WHERE a.task_id = @task_id
  AND a.node_id = n.id
  AND a.id = n.current_attempt_id
  AND a.claim_epoch = n.claim_epoch
  AND a.status IN ('claimed', 'running')
  AND n.state IN ('claimed', 'running')
RETURNING a.*;

-- name: SubmitWorkflowAttemptArtifact :one
UPDATE workflow_node_attempt a
SET status = 'submitted',
    base_commit = @base_commit,
    result_commit = @result_commit,
    artifact_key = @artifact_key,
    artifact_digest = @artifact_digest,
    artifact_size = @artifact_size,
    manifest = @manifest,
    submitted_at = now(),
    completed_at = now()
FROM workflow_node n
WHERE a.id = @attempt_id
  AND a.node_id = n.id
  AND a.claim_epoch = @claim_epoch
  AND a.id = n.current_attempt_id
  AND a.claim_epoch = n.claim_epoch
  AND a.status IN ('claimed', 'running')
  AND n.state IN ('claimed', 'running')
RETURNING a.*;

-- name: MarkWorkflowNodeSubmitted :one
UPDATE workflow_node n
SET state = 'submitted', updated_at = now()
FROM workflow_node_attempt a
WHERE a.id = @attempt_id
  AND a.node_id = n.id
  AND a.claim_epoch = @claim_epoch
  AND a.id = n.current_attempt_id
  AND a.claim_epoch = n.claim_epoch
  AND a.status = 'submitted'
  AND n.state IN ('claimed', 'running')
RETURNING n.*;

-- name: FailWorkflowAttempt :one
UPDATE workflow_node_attempt
SET status = @status,
    error = @error,
    completed_at = now()
WHERE id = @attempt_id
  AND claim_epoch = @claim_epoch
  AND status IN ('claimed', 'running')
RETURNING *;

-- name: RequeueWorkflowNodeAfterAttempt :one
UPDATE workflow_node
SET state = CASE
        WHEN claim_epoch < max_attempts THEN 'ready'
        ELSE 'failed'
    END,
    current_attempt_id = NULL,
    ready_at = CASE
        WHEN claim_epoch < max_attempts THEN now()
        ELSE ready_at
    END,
    stealable_at = CASE
        WHEN claim_epoch < max_attempts THEN now()
        ELSE stealable_at
    END,
    updated_at = now()
WHERE id = @node_id
  AND current_attempt_id = @attempt_id
  AND claim_epoch = @claim_epoch
  AND state IN ('claimed', 'running')
RETURNING *;

-- name: ReleaseWorkflowAttemptResources :execrows
DELETE FROM workflow_resource_claim
WHERE attempt_id = @attempt_id;

-- name: ExpireWorkflowTask :execrows
UPDATE agent_task_queue
SET status = 'failed',
    error = 'workflow lease expired',
    failure_reason = 'workflow_lease_expired',
    completed_at = now()
WHERE workflow_attempt_id = @attempt_id
  AND status IN ('queued', 'dispatched', 'waiting_local_directory', 'running');

-- name: ListExpiredWorkflowAttempts :many
SELECT a.*
FROM workflow_node_attempt a
JOIN workflow_node n ON n.current_attempt_id = a.id
JOIN workflow_run r ON r.id = n.run_id
WHERE a.status IN ('claimed', 'running')
  AND a.lease_expires_at < now()
  AND n.state IN ('claimed', 'running')
  AND r.status = 'running'
ORDER BY a.lease_expires_at ASC
LIMIT @max_attempts
FOR UPDATE OF a SKIP LOCKED;

-- name: InsertWorkflowOutbox :one
INSERT INTO workflow_outbox (run_id, node_id, event_type, payload)
VALUES (@run_id, sqlc.narg(node_id), @event_type, @payload)
RETURNING *;

-- name: ClaimWorkflowOutboxEvents :many
UPDATE workflow_outbox
SET status = 'processing',
    claimed_at = now(),
    attempt_count = attempt_count + 1
WHERE id IN (
    SELECT id
    FROM workflow_outbox
    WHERE status = 'pending' AND available_at <= now()
    ORDER BY available_at ASC, created_at ASC
    LIMIT @max_events
    FOR UPDATE SKIP LOCKED
)
RETURNING *;

-- name: ClaimWorkflowReleaseEvents :many
UPDATE workflow_outbox
SET status = 'processing',
    claimed_at = now(),
    attempt_count = attempt_count + 1
WHERE id IN (
    SELECT id
    FROM workflow_outbox
    WHERE status = 'pending'
      AND event_type = 'workflow.node_accepted'
      AND available_at <= now()
    ORDER BY available_at ASC, created_at ASC
    LIMIT @max_events
    FOR UPDATE SKIP LOCKED
)
RETURNING *;

-- name: RequeueExpiredWorkflowIntegrationEvents :execrows
UPDATE workflow_outbox
SET status = 'pending',
    available_at = now(),
    claimed_at = NULL,
    claimed_runtime_id = NULL,
    claimed_daemon_id = NULL,
    lease_expires_at = NULL,
    last_error = 'integration lease expired'
WHERE status = 'processing'
  AND event_type IN ('workflow.artifact_submitted', 'workflow.deterministic_ready')
  AND lease_expires_at < now();

-- name: ClaimWorkflowIntegrationJob :one
WITH candidate AS (
    SELECT
        o.id,
        selected.runtime_id,
        rp.lease_seconds
    FROM workflow_outbox o
    JOIN workflow_node n ON n.id = o.node_id
    JOIN workflow_run wr ON wr.id = n.run_id
    JOIN runtime_pool rp ON rp.id = wr.integration_pool_id
    JOIN LATERAL (
        SELECT rpr.runtime_id
        FROM runtime_pool_runtime rpr
        JOIN agent_runtime ar ON ar.id = rpr.runtime_id
        WHERE rpr.pool_id = wr.integration_pool_id
          AND rpr.enabled = TRUE
          AND rpr.runtime_id = ANY(@runtime_ids::uuid[])
          AND ar.workspace_id = wr.workspace_id
          AND ar.daemon_id = @daemon_id
          AND ar.status = 'online'
        ORDER BY rpr.priority DESC, rpr.created_at ASC
        LIMIT 1
    ) selected ON TRUE
    JOIN workflow_node_attempt a ON a.id = n.current_attempt_id
    WHERE o.status = 'pending'
      AND o.event_type IN ('workflow.artifact_submitted', 'workflow.deterministic_ready')
      AND o.available_at <= now()
      AND wr.status = 'running'
      AND rp.enabled = TRUE
      AND n.state = 'submitted'
      AND a.status = 'submitted'
      AND (
          SELECT count(*)
          FROM workflow_outbox active_outbox
          JOIN workflow_node active_node ON active_node.id = active_outbox.node_id
          JOIN workflow_run active_run ON active_run.id = active_node.run_id
          WHERE active_outbox.status = 'processing'
            AND active_outbox.event_type IN (
                'workflow.artifact_submitted',
                'workflow.deterministic_ready'
            )
            AND active_run.integration_pool_id = wr.integration_pool_id
      ) < rp.max_inflight
      AND NOT EXISTS (
          SELECT 1
          FROM workflow_outbox active_outbox
          JOIN workflow_node active_node ON active_node.id = active_outbox.node_id
          JOIN workflow_run active_run ON active_run.id = active_node.run_id
          WHERE active_outbox.status = 'processing'
            AND active_outbox.event_type IN (
                'workflow.artifact_submitted',
                'workflow.deterministic_ready'
            )
            AND active_run.project_id = wr.project_id
            AND COALESCE(
                active_run.metadata->>'book_slug',
                active_run.id::text
            ) = COALESCE(
                wr.metadata->>'book_slug',
                wr.id::text
            )
      )
    ORDER BY o.available_at ASC, o.created_at ASC
    LIMIT 1
    FOR UPDATE OF o SKIP LOCKED
),
claimed AS (
    UPDATE workflow_outbox o
    SET status = 'processing',
        claimed_at = now(),
        claimed_runtime_id = candidate.runtime_id,
        claimed_daemon_id = @daemon_id,
        lease_expires_at = now() + make_interval(secs => candidate.lease_seconds::double precision),
        attempt_count = attempt_count + 1
    FROM candidate
    WHERE o.id = candidate.id
    RETURNING o.*, candidate.runtime_id
)
SELECT
    claimed.*,
    n.issue_id,
    n.passage_key,
    n.node_key,
    n.generation,
    n.claim_epoch,
    n.output_contract,
    a.id AS attempt_id,
    a.base_commit,
    a.result_commit,
    a.artifact_key,
    a.artifact_digest,
    a.artifact_size,
    a.manifest,
    wr.workspace_id,
    wr.project_id
FROM claimed
JOIN workflow_node n ON n.id = claimed.node_id
JOIN workflow_node_attempt a ON a.id = n.current_attempt_id
JOIN workflow_run wr ON wr.id = n.run_id;

-- name: GetWorkflowIntegrationJob :one
SELECT
    o.*,
    n.issue_id,
    n.passage_key,
    n.node_key,
    n.generation,
    n.claim_epoch,
    n.output_contract,
    a.id AS attempt_id,
    a.base_commit,
    a.result_commit,
    a.artifact_key,
    a.artifact_digest,
    a.artifact_size,
    a.manifest,
    wr.workspace_id,
    wr.project_id
FROM workflow_outbox o
JOIN workflow_node n ON n.id = o.node_id
JOIN workflow_node_attempt a ON a.id = n.current_attempt_id
JOIN workflow_run wr ON wr.id = n.run_id
WHERE o.id = @id
  AND o.event_type IN ('workflow.artifact_submitted', 'workflow.deterministic_ready');

-- name: RenewWorkflowIntegrationLease :one
UPDATE workflow_outbox o
SET lease_expires_at = now() + make_interval(secs => rp.lease_seconds::double precision)
FROM workflow_node n
JOIN workflow_run wr ON wr.id = n.run_id
JOIN runtime_pool rp ON rp.id = wr.integration_pool_id
WHERE o.id = @id
  AND o.node_id = n.id
  AND o.status = 'processing'
  AND o.event_type IN ('workflow.artifact_submitted', 'workflow.deterministic_ready')
  AND o.claimed_runtime_id = @runtime_id
  AND o.claimed_daemon_id = @daemon_id
RETURNING o.*;

-- name: CompleteWorkflowOutboxEvent :one
UPDATE workflow_outbox
SET status = 'completed', completed_at = now(), last_error = NULL
WHERE id = @id AND status = 'processing'
RETURNING *;

-- name: RetryWorkflowOutboxEvent :one
UPDATE workflow_outbox
SET status = CASE WHEN attempt_count >= 10 THEN 'failed' ELSE 'pending' END,
    available_at = now() + make_interval(secs => @retry_after_seconds::double precision),
    last_error = @last_error,
    claimed_at = NULL
WHERE id = @id AND status = 'processing'
RETURNING *;

-- name: FailWorkflowOutboxEvent :one
UPDATE workflow_outbox
SET status = 'failed',
    completed_at = now(),
    last_error = @last_error
WHERE id = @id AND status = 'processing'
RETURNING *;

-- name: BlockWorkflowNodeIntegration :one
UPDATE workflow_node n
SET state = 'blocked', updated_at = now()
FROM workflow_node_attempt a
WHERE n.id = @node_id
  AND a.id = @attempt_id
  AND a.node_id = n.id
  AND a.claim_epoch = @claim_epoch
  AND n.current_attempt_id = a.id
  AND n.claim_epoch = a.claim_epoch
  AND n.state IN ('submitted', 'integrating')
  AND a.status = 'submitted'
RETURNING n.*;

-- name: AcceptWorkflowNodeResult :one
INSERT INTO workflow_node_result (
    node_id, attempt_id, generation, claim_epoch, canonical_commit,
    artifact_digest, manifest
)
SELECT
    n.id, a.id, n.generation, a.claim_epoch, @canonical_commit,
    a.artifact_digest, a.manifest
FROM workflow_node n
JOIN workflow_node_attempt a ON a.id = n.current_attempt_id
WHERE n.id = @node_id
  AND n.state = 'integrating'
  AND a.id = @attempt_id
  AND a.claim_epoch = @claim_epoch
  AND a.status = 'submitted'
ON CONFLICT (node_id) DO NOTHING
RETURNING *;

-- name: MarkWorkflowNodeIntegrating :one
UPDATE workflow_node n
SET state = 'integrating', updated_at = now()
FROM workflow_node_attempt a
WHERE n.id = @node_id
  AND a.id = @attempt_id
  AND a.node_id = n.id
  AND a.claim_epoch = @claim_epoch
  AND n.current_attempt_id = a.id
  AND n.claim_epoch = a.claim_epoch
  AND n.state = 'submitted'
  AND a.status = 'submitted'
RETURNING n.*;

-- name: CompleteWorkflowNode :one
UPDATE workflow_node n
SET state = 'completed',
    completed_at = now(),
    updated_at = now(),
    preferred_daemon_id = CASE
        WHEN preferred_daemon_id IS NULL
          OR NOT EXISTS (
              SELECT 1
              FROM agent_runtime ar
              WHERE ar.daemon_id = preferred_daemon_id
                AND ar.status = 'online'
                AND ar.last_seen_at >= now() - interval '45 seconds'
          )
        THEN a.daemon_id
        ELSE preferred_daemon_id
    END
FROM workflow_node_attempt a
WHERE n.id = @node_id
  AND a.id = @attempt_id
  AND a.node_id = n.id
  AND a.claim_epoch = @claim_epoch
  AND n.current_attempt_id = a.id
  AND n.claim_epoch = a.claim_epoch
  AND n.state = 'integrating'
  AND EXISTS (
      SELECT 1 FROM workflow_node_result r WHERE r.node_id = n.id
  )
RETURNING n.*;

-- name: MarkWorkflowAttemptIntegrated :one
UPDATE workflow_node_attempt
SET status = 'integrated', completed_at = now()
WHERE id = @attempt_id
  AND claim_epoch = @claim_epoch
  AND status = 'submitted'
RETURNING *;

-- name: ReleaseReadyWorkflowSuccessors :many
UPDATE workflow_node successor
SET state = CASE
        WHEN successor.executor_kind = 'human_gate' THEN 'waiting_human'
        ELSE 'ready'
    END,
    ready_at = CASE
        WHEN successor.executor_kind = 'human_gate' THEN successor.ready_at
        ELSE now()
    END,
    stealable_at = CASE
        WHEN successor.executor_kind = 'agent'
        THEN now() + make_interval(
            secs => COALESCE(
                (
                    SELECT pool.affinity_grace_seconds
                    FROM runtime_pool pool
                    WHERE pool.id = successor.runtime_pool_id
                ),
                60
            )::double precision
        )
        ELSE successor.stealable_at
    END,
    preferred_daemon_id = COALESCE(successor.preferred_daemon_id, completed.preferred_daemon_id),
    updated_at = now()
FROM workflow_node completed
WHERE completed.id = @completed_node_id
  AND successor.run_id = completed.run_id
  AND successor.state = 'pending'
  AND EXISTS (
      SELECT 1
      FROM workflow_node_dependency direct_edge
      WHERE direct_edge.node_id = successor.id
        AND direct_edge.depends_on_node_id = completed.id
  )
  AND NOT EXISTS (
      SELECT 1
      FROM workflow_node_dependency dependency
      JOIN workflow_node predecessor ON predecessor.id = dependency.depends_on_node_id
      WHERE dependency.node_id = successor.id
        AND predecessor.state <> 'completed'
  )
RETURNING successor.*;

-- name: GetWorkflowNodeResolvedInputMaterial :one
SELECT COALESCE(
    jsonb_agg(
        jsonb_build_object(
            'node_id', predecessor.id,
            'node_key', predecessor.node_key,
            'generation', predecessor.generation,
            'executor_kind', predecessor.executor_kind,
            'canonical_commit', result.canonical_commit,
            'artifact_digest', result.artifact_digest,
            'input_digest', predecessor.input_digest,
            'law_digest', predecessor.law_digest
        )
        ORDER BY predecessor.node_key, predecessor.id
    ),
    '[]'::jsonb
) AS material
FROM workflow_node_dependency dependency
JOIN workflow_node predecessor ON predecessor.id = dependency.depends_on_node_id
LEFT JOIN workflow_node_result result ON result.node_id = predecessor.id
WHERE dependency.node_id = @node_id;

-- name: SetWorkflowNodeResolvedInputDigest :one
UPDATE workflow_node
SET input_digest = @input_digest,
    updated_at = now()
WHERE id = @node_id
  AND state IN ('ready', 'waiting_human')
RETURNING *;

-- name: CompleteWorkflowHumanGate :one
UPDATE workflow_node
SET state = 'completed',
    claim_epoch = claim_epoch + 1,
    current_attempt_id = @attempt_id,
    completed_at = now(),
    updated_at = now()
WHERE id = @node_id
  AND run_id = @run_id
  AND executor_kind = 'human_gate'
  AND state = 'waiting_human'
RETURNING *;

-- name: GetWorkflowHumanGateDependencyResult :one
SELECT result.*
FROM workflow_node_dependency dependency
JOIN workflow_node_result result
  ON result.node_id = dependency.depends_on_node_id
WHERE dependency.node_id = @node_id;

-- name: CreateWorkflowHumanGateAttempt :one
INSERT INTO workflow_node_attempt (
    id, node_id, claim_epoch, runtime_id, daemon_id, status,
    lease_expires_at, base_commit, result_commit, artifact_digest,
    manifest, claimed_at, started_at, submitted_at, completed_at
)
VALUES (
    @id, @node_id, @claim_epoch, NULL, 'human-gate', 'integrated',
    now(), @canonical_commit, @canonical_commit, @artifact_digest,
    @manifest, now(), now(), now(), now()
)
RETURNING *;

-- name: CreateWorkflowHumanGateResult :one
INSERT INTO workflow_node_result (
    node_id, attempt_id, generation, claim_epoch, canonical_commit,
    artifact_digest, manifest
)
SELECT
    node.id, @attempt_id, node.generation, node.claim_epoch,
    @canonical_commit, @artifact_digest, @manifest
FROM workflow_node node
WHERE node.id = @node_id
  AND node.current_attempt_id = @attempt_id
  AND node.state = 'completed'
RETURNING *;

-- name: RetryWorkflowNode :one
UPDATE workflow_node
SET state = 'ready',
    generation = generation + 1,
    current_attempt_id = NULL,
    ready_at = now(),
    stealable_at = now(),
    input_digest = COALESCE(sqlc.narg(input_digest)::text, input_digest),
    law_digest = COALESCE(sqlc.narg(law_digest)::text, law_digest),
    completed_at = NULL,
    updated_at = now()
WHERE id = @node_id
  AND run_id = @run_id
  AND state IN ('blocked', 'failed', 'cancelled')
  AND executor_kind = 'agent'
RETURNING *;

-- name: CancelWorkflowNode :one
UPDATE workflow_node
SET state = 'cancelled',
    updated_at = now()
WHERE id = @node_id
  AND run_id = @run_id
  AND state NOT IN ('completed', 'cancelled')
RETURNING *;

-- name: CancelWorkflowNodeAttempt :execrows
UPDATE workflow_node_attempt
SET status = 'cancelled',
    error = @error,
    completed_at = now()
WHERE id = @attempt_id
  AND status IN ('claimed', 'running', 'submitted');

-- name: CancelWorkflowTaskForAttempt :execrows
UPDATE agent_task_queue
SET status = 'cancelled',
    error = @error,
    completed_at = now()
WHERE workflow_attempt_id = @attempt_id
  AND status IN ('queued', 'dispatched', 'waiting_local_directory', 'running');

-- name: CancelWorkflowOutboxForNode :execrows
UPDATE workflow_outbox
SET status = 'failed',
    last_error = @error,
    completed_at = now()
WHERE node_id = @node_id
  AND status IN ('pending', 'processing');

-- name: CountProcessingWorkflowIntegrationEventsForNode :one
SELECT count(*)
FROM workflow_outbox
WHERE node_id = @node_id
  AND status = 'processing'
  AND event_type IN ('workflow.artifact_submitted', 'workflow.deterministic_ready');

-- name: InsertWorkflowAuditEvent :one
INSERT INTO workflow_outbox (
    run_id, node_id, event_type, payload, status, completed_at
)
VALUES (
    @run_id, sqlc.narg(node_id), @event_type, @payload, 'completed', now()
)
RETURNING *;

-- name: GetWorkflowNodeResult :one
SELECT * FROM workflow_node_result
WHERE node_id = @node_id;

-- name: ListWorkflowNodeResults :many
SELECT r.*
FROM workflow_node_result r
JOIN workflow_node n ON n.id = r.node_id
WHERE n.run_id = @run_id
ORDER BY r.accepted_at ASC;
