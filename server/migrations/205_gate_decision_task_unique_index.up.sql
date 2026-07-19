CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS idx_agent_task_gate_decision_unique
    ON agent_task_queue (gate_decision_id)
    WHERE gate_decision_id IS NOT NULL;
