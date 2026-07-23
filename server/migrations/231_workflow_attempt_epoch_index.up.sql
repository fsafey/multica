CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS workflow_attempt_epoch_unique ON workflow_node_attempt(node_id, claim_epoch);
