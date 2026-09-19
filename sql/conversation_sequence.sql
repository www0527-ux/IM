USE `infiniteChat`;

CREATE TABLE `conversation_sequence`
(
    `session_id`       bigint NOT NULL COMMENT 'conversation id',
    `reserved_max_seq` bigint NOT NULL DEFAULT 0 COMMENT 'durably reserved Seq upper bound',

    PRIMARY KEY (`session_id`)
) ENGINE = InnoDB
  CHARACTER SET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci
  COMMENT = 'conversation Seq segment allocator';
