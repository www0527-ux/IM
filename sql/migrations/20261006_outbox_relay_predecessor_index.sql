-- Apply once to the intended existing database after checking SHOW INDEX.
-- Fresh installations already include this index in outbox_event.sql.
-- Example: mysql [connection options] infiniteChat < this-file.sql
-- No status, lease, payload or business rows are modified.
ALTER TABLE `outbox_event`
    ADD INDEX `idx_relay_predecessor`
        (`aggregate_type`, `aggregate_id`, `event_type`, `status`, `aggregate_seq`);

-- Rollback, if required:
-- ALTER TABLE `outbox_event` DROP INDEX `idx_relay_predecessor`;
