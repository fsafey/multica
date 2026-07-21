DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'task_execution_evidence'::regclass
          AND conname = 'task_execution_evidence_pkey'
    ) THEN
        ALTER TABLE task_execution_evidence
            ADD CONSTRAINT task_execution_evidence_pkey
            PRIMARY KEY USING INDEX task_execution_evidence_task_id_unique;
    END IF;
END
$$;
