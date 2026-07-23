CREATE INDEX CONCURRENTLY IF NOT EXISTS workflow_node_claim_candidates ON workflow_node(runtime_pool_id, priority DESC, ready_at ASC) WHERE state = 'ready';
