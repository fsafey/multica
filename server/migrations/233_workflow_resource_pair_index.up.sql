CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS workflow_resource_pair_unique ON workflow_node_resource(node_id, resource_key);
