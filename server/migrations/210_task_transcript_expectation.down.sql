ALTER TABLE agent_task_queue
    DROP COLUMN transcript_delivery_confirmed,
    DROP COLUMN transcript_expected_last_seq,
    DROP COLUMN transcript_expected_message_count;
