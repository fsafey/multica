CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS runtime_pool_workspace_name_unique ON runtime_pool(workspace_id, lower(name));
