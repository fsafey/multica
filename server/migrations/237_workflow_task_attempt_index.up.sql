CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS workflow_task_attempt_unique ON agent_task_queue(workflow_attempt_id) WHERE workflow_attempt_id IS NOT NULL;
