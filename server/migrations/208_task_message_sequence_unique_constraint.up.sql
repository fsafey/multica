DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'task_message'::regclass
          AND conname = 'task_message_task_id_seq_unique'
    ) THEN
        ALTER TABLE task_message
            ADD CONSTRAINT task_message_task_id_seq_unique
            UNIQUE USING INDEX task_message_task_id_seq_unique;
    END IF;
END
$$;
