-- Chat sessions may opt into one project's durable context. Keep this as a
-- soft reference: adding a foreign key would validate the established,
-- write-active chat_session table and take a cross-table lock during deploy.
-- Create/delete handlers serialize on the project row, project deletion clears
-- existing references, and daemon claim revalidates workspace ownership before
-- injecting any context.
--
-- IF NOT EXISTS because upstream first shipped this as 213_chat_session_project
-- and later renumbered it to 214 (#5868, dodging a prefix collision). This fork
-- lands it at 250 after its own 213-248 migrations. Environments that applied
-- an earlier upstream name have the column but not the 250 version row, so the
-- runner safely re-applies it under the fork-local name.
ALTER TABLE chat_session
  ADD COLUMN IF NOT EXISTS project_id UUID;
