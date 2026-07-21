# Task execution evidence migration

Hold the new server binary out of service until migrations 203 through 219 have all applied and been verified.
If migration 206 stops on historical duplicates, keep serving the old server binary or roll back the new binary until the duplicate groups are reconciled and the full migration set succeeds.
Only then upgrade any standalone daemon or Desktop bundle that ships the new daemon.
The daemon deliberately refuses to launch a provider when the server cannot persist the current evidence schema.
This server-first order preserves the guarantee that every future provider execution has immutable claim-time evidence.

Migration 206 stops when historical `task_message` rows share a `(task_id, seq)` pair.
Inspect the duplicate groups without exposing message content:

```sql
SELECT task_id, seq, COUNT(*) AS row_count
FROM task_message
GROUP BY task_id, seq
HAVING COUNT(*) > 1
ORDER BY task_id, seq;
```

Remove only byte-identical retransmissions.
The following statement retains the earliest row when every row in the sequence group has identical persisted content:

```sql
WITH classified AS (
    SELECT
        id,
        ROW_NUMBER() OVER (
            PARTITION BY task_id, seq, type, tool, content, input, output
            ORDER BY created_at, id
        ) AS identical_rank,
        COUNT(*) OVER (PARTITION BY task_id, seq) AS sequence_count,
        COUNT(*) OVER (
            PARTITION BY task_id, seq, type, tool, content, input, output
        ) AS identical_count
    FROM task_message
)
DELETE FROM task_message AS message
USING classified
WHERE message.id = classified.id
  AND classified.identical_rank > 1
  AND classified.identical_count = classified.sequence_count;
```

Run the duplicate query again.
If any group remains, its content conflicts and requires owner adjudication before the migration may continue.
Never select an arbitrary winner for a conflicting sequence.

PostgreSQL can leave an invalid index after an interrupted `CREATE INDEX CONCURRENTLY`.
The migration runner also records a migration version after executing its SQL, so a connection loss can leave a valid constraint attached while its migration version remains unrecorded.
Migrations 205 and 208 check for their constraints and are safe to replay in that state.

Check the two migration indexes and attached constraints before retrying:

```sql
SELECT indexrelid::regclass AS index_name, indisvalid, indisready
FROM pg_index
WHERE indexrelid IN (
    to_regclass('task_execution_evidence_task_id_unique'),
    to_regclass('task_execution_evidence_pkey'),
    to_regclass('task_message_task_id_seq_unique')
);

SELECT conrelid::regclass AS table_name, conname, contype
FROM pg_constraint
WHERE (conrelid, conname) IN (
    ('task_execution_evidence'::regclass, 'task_execution_evidence_pkey'),
    ('task_message'::regclass, 'task_message_task_id_seq_unique')
);
```

Drop only an index reported as invalid, then rerun migrations:

```sql
DROP INDEX CONCURRENTLY IF EXISTS task_execution_evidence_task_id_unique;
DROP INDEX CONCURRENTLY IF EXISTS task_message_task_id_seq_unique;
```

Do not drop a valid index or an attached primary-key or unique constraint.
Migration 204 and migration 207 use `IF NOT EXISTS` so a valid concurrently created index survives a bookkeeping retry.
Migration 205 and migration 208 skip an already attached constraint so a version-recording retry converges without manual schema edits.
