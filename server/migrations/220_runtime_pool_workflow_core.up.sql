CREATE TABLE IF NOT EXISTS runtime_pool (
    id UUID NOT NULL DEFAULT gen_random_uuid(),
    workspace_id UUID NOT NULL,
    name TEXT NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    max_inflight INTEGER NOT NULL DEFAULT 1
        CHECK (max_inflight > 0 AND max_inflight <= 32),
    affinity_grace_seconds INTEGER NOT NULL DEFAULT 60
        CHECK (affinity_grace_seconds >= 0 AND affinity_grace_seconds <= 3600),
    lease_seconds INTEGER NOT NULL DEFAULT 90
        CHECK (lease_seconds >= 30 AND lease_seconds <= 3600),
    created_by UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS runtime_pool_runtime (
    pool_id UUID NOT NULL,
    runtime_id UUID NOT NULL,
    priority INTEGER NOT NULL DEFAULT 0,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS agent_runtime_pool (
    agent_id UUID NOT NULL,
    pool_id UUID NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS workflow_run (
    id UUID NOT NULL DEFAULT gen_random_uuid(),
    workspace_id UUID NOT NULL,
    project_id UUID NOT NULL,
    anchor_issue_id UUID NOT NULL,
    graph_key TEXT NOT NULL,
    graph_version TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'draft'
        CHECK (status IN ('draft', 'running', 'paused', 'completed', 'failed', 'cancelled')),
    integration_pool_id UUID,
    wip_limit INTEGER NOT NULL DEFAULT 4
        CHECK (wip_limit > 0 AND wip_limit <= 32),
    human_gate_limit INTEGER NOT NULL DEFAULT 5
        CHECK (human_gate_limit > 0 AND human_gate_limit <= 100),
    input_digest TEXT,
    law_digest TEXT,
    metadata JSONB NOT NULL DEFAULT '{}',
    created_by UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS workflow_node (
    id UUID NOT NULL DEFAULT gen_random_uuid(),
    run_id UUID NOT NULL,
    issue_id UUID NOT NULL,
    passage_key TEXT NOT NULL,
    node_key TEXT NOT NULL,
    generation INTEGER NOT NULL DEFAULT 1 CHECK (generation > 0),
    executor_kind TEXT NOT NULL
        CHECK (executor_kind IN ('agent', 'human_gate', 'deterministic')),
    agent_id UUID,
    runtime_pool_id UUID,
    state TEXT NOT NULL DEFAULT 'pending'
        CHECK (state IN (
            'pending', 'ready', 'claimed', 'running', 'submitted',
            'integrating', 'waiting_human', 'completed', 'failed', 'blocked',
            'cancelled'
        )),
    priority INTEGER NOT NULL DEFAULT 0,
    preferred_daemon_id TEXT,
    stealable_at TIMESTAMPTZ,
    claim_epoch BIGINT NOT NULL DEFAULT 0 CHECK (claim_epoch >= 0),
    current_attempt_id UUID,
    input_digest TEXT,
    law_digest TEXT,
    output_contract JSONB NOT NULL DEFAULT '{}',
    max_attempts INTEGER NOT NULL DEFAULT 3
        CHECK (max_attempts > 0 AND max_attempts <= 10),
    ready_at TIMESTAMPTZ,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS workflow_node_dependency (
    node_id UUID NOT NULL,
    depends_on_node_id UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (node_id <> depends_on_node_id)
);

CREATE TABLE IF NOT EXISTS workflow_node_attempt (
    id UUID NOT NULL DEFAULT gen_random_uuid(),
    node_id UUID NOT NULL,
    claim_epoch BIGINT NOT NULL CHECK (claim_epoch > 0),
    task_id UUID,
    runtime_id UUID,
    daemon_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'claimed'
        CHECK (status IN (
            'claimed', 'running', 'submitted', 'failed', 'expired',
            'cancelled', 'integrated', 'rejected'
        )),
    lease_expires_at TIMESTAMPTZ NOT NULL,
    base_commit TEXT,
    result_commit TEXT,
    artifact_key TEXT,
    artifact_digest TEXT,
    artifact_size BIGINT,
    manifest JSONB NOT NULL DEFAULT '{}',
    error TEXT,
    claimed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at TIMESTAMPTZ,
    submitted_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS workflow_node_result (
    node_id UUID NOT NULL,
    attempt_id UUID NOT NULL,
    generation INTEGER NOT NULL CHECK (generation > 0),
    claim_epoch BIGINT NOT NULL CHECK (claim_epoch > 0),
    canonical_commit TEXT NOT NULL,
    artifact_digest TEXT NOT NULL,
    manifest JSONB NOT NULL DEFAULT '{}',
    accepted_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS workflow_node_resource (
    node_id UUID NOT NULL,
    resource_key TEXT NOT NULL,
    mode TEXT NOT NULL DEFAULT 'exclusive'
        CHECK (mode IN ('exclusive', 'shared')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS workflow_resource_claim (
    resource_key TEXT NOT NULL,
    node_id UUID NOT NULL,
    attempt_id UUID NOT NULL,
    acquired_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS workflow_outbox (
    id UUID NOT NULL DEFAULT gen_random_uuid(),
    run_id UUID NOT NULL,
    node_id UUID,
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}',
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'processing', 'completed', 'failed')),
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    available_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    claimed_at TIMESTAMPTZ,
    claimed_runtime_id UUID,
    claimed_daemon_id TEXT,
    lease_expires_at TIMESTAMPTZ,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ
);

ALTER TABLE agent_task_queue
    ADD COLUMN IF NOT EXISTS workflow_node_id UUID,
    ADD COLUMN IF NOT EXISTS workflow_attempt_id UUID,
    ADD COLUMN IF NOT EXISTS workflow_claim_epoch BIGINT,
    ADD COLUMN IF NOT EXISTS workflow_input_digest TEXT,
    ADD COLUMN IF NOT EXISTS workflow_law_digest TEXT;
