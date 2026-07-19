CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS idx_gate_review_decision_request_unique
    ON gate_review_decision (request_id);
