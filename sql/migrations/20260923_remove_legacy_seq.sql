-- Run once on an existing database after stopping the old message pipeline.
-- Fresh installations use sql/conversation_sequence.sql directly.
USE `infiniteChat`;

-- Existing messages may have been numbered by the retired Redis allocator.
-- Preserve their highest committed Seq before accepting new writes.
INSERT INTO `conversation_sequence` (`session_id`, `last_seq`)
SELECT `session_id`, MAX(`seq`)
FROM `message`
GROUP BY `session_id`
ON DUPLICATE KEY UPDATE
    `last_seq` = GREATEST(`last_seq`, VALUES(`last_seq`));

ALTER TABLE `conversation_sequence`
    DROP COLUMN `reserved_max_seq`;
