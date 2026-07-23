CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS workflow_dependency_pair_unique ON workflow_node_dependency(node_id, depends_on_node_id);
