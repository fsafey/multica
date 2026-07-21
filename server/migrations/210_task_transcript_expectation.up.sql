ALTER TABLE agent_task_queue
    ADD COLUMN transcript_expected_message_count INTEGER,
    ADD COLUMN transcript_expected_last_seq INTEGER,
    ADD COLUMN transcript_delivery_confirmed BOOLEAN;
