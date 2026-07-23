CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_workflow_node_issue_agent
ON workflow_node (issue_id, agent_id)
WHERE agent_id IS NOT NULL;
