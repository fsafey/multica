CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_gate_decision_wake_retry ON gate_decision_wake (next_attempt_at, created_at, decision_id) WHERE state = 'pending';
