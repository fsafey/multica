-- Immutable, issue-scoped gate review requests and member decisions.
--
-- Relationships are enforced in the application layer. This repository does
-- not add database foreign keys or cascading actions.

CREATE TABLE gate_review_request (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id UUID NOT NULL,
    issue_id UUID NOT NULL,
    gate TEXT NOT NULL CHECK (gate <> '' AND length(gate) <= 64),
    revision INTEGER NOT NULL CHECK (revision > 0),
    subject_digest TEXT NOT NULL CHECK (subject_digest ~ '^sha256:[0-9a-f]{64}$'),
    review_data JSONB NOT NULL CHECK (jsonb_typeof(review_data) = 'object'),
    actor_type TEXT NOT NULL CHECK (actor_type IN ('member', 'agent')),
    actor_id UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE gate_review_decision (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id UUID NOT NULL,
    issue_id UUID NOT NULL,
    request_id UUID NOT NULL,
    outcome TEXT NOT NULL CHECK (outcome IN ('approved', 'changes_requested')),
    reason TEXT NOT NULL DEFAULT '',
    actor_id UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE gate_decision_wake (
    decision_id UUID PRIMARY KEY,
    workspace_id UUID NOT NULL,
    issue_id UUID NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending' CHECK (state IN ('pending', 'delivered')),
    task_id UUID,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    delivered_at TIMESTAMPTZ
);

ALTER TABLE agent_task_queue ADD COLUMN gate_decision_id UUID;
