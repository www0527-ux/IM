USE `infiniteChat`;

CREATE TABLE `conversation_sequence`
(
    `session_id`       bigint NOT NULL COMMENT 'conversation id',
    `last_seq`         bigint NOT NULL DEFAULT 0 COMMENT 'committed Seq counter',
    `updated_time`     datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
                                     ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',

    PRIMARY KEY (`session_id`)
) ENGINE = InnoDB
  CHARACTER SET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci
  COMMENT = 'conversation Seq counters';
