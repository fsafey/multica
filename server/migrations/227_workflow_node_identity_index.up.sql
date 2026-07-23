CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS workflow_node_identity_unique ON workflow_node(run_id, issue_id, node_key, generation);
