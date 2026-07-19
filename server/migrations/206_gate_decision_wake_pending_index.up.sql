CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_gate_decision_wake_pending ON gate_decision_wake (created_at, decision_id) WHERE state = 'pending';
