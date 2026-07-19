CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_gate_review_request_issue_gate_revision
    ON gate_review_request (workspace_id, issue_id, gate, revision DESC, created_at DESC);
