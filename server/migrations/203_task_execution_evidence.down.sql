DROP TRIGGER IF EXISTS task_execution_evidence_immutable ON task_execution_evidence;
DROP FUNCTION IF EXISTS reject_task_execution_evidence_mutation();
DROP TABLE IF EXISTS task_execution_evidence;
