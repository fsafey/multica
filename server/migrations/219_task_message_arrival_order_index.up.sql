CREATE INDEX CONCURRENTLY IF NOT EXISTS task_message_task_id_arrival_order_idx
    ON task_message(task_id, arrival_order);
