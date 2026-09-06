USE `infiniteChat`;

CREATE TABLE `red_packet`
(
    `red_packet_id`           bigint      NOT NULL COMMENT '红包ID',
    `sender_id`               bigint      NOT NULL COMMENT '发送者用户ID',
    `session_id`              bigint      NOT NULL COMMENT '发送到的会话ID',
    `session_type`            tinyint     NOT NULL COMMENT '会话类型：0单聊 1群聊',
    `red_packet_wrapper_text` varchar(50) NOT NULL
                                          DEFAULT '恭喜发财，大吉大利'
                                          COMMENT '红包封面文案',
    `red_packet_type`         tinyint     NOT NULL COMMENT '红包类型：0普通红包 1拼手气红包',
    `total_amount`            bigint      NOT NULL COMMENT '总金额，单位为分',
    `total_count`             int         NOT NULL COMMENT '红包总份数',
    `status`                  tinyint     NOT NULL DEFAULT 0
                                          COMMENT '状态：0领取中 1已领完 2已过期',
    `created_time`            datetime    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time`            datetime    NOT NULL DEFAULT CURRENT_TIMESTAMP
                                             ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',

    PRIMARY KEY (`red_packet_id`),

    INDEX `idx_session_id` (`session_id`),

    INDEX `idx_sender_id` (`sender_id`)
) ENGINE = InnoDB
  CHARACTER SET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci
  COMMENT = '红包主表';