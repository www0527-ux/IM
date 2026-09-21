USE `infiniteChat`;

CREATE TABLE `red_packet`
(
    `red_packet_id`   bigint       NOT NULL COMMENT '红包业务聚合ID',
    `message_id`      bigint       NOT NULL COMMENT '对应的红包聊天消息ID',
    `sender_id`       bigint       NOT NULL COMMENT '发送者用户ID',
    `receiver_id`     bigint       NULL COMMENT '单聊指定接收者；群聊为空',
    `client_message_id` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin
                                    NOT NULL COMMENT '客户端消息ID，同时作为发红包幂等键',
    `session_id`      bigint       NOT NULL COMMENT '发送到的会话ID',
    `session_type`    tinyint      NOT NULL COMMENT '会话类型：0单聊 1群聊',
    `red_packet_type` tinyint      NOT NULL COMMENT '红包类型：0普通红包 1拼手气红包',
    `wrapper_text`    varchar(50)  NOT NULL DEFAULT '恭喜发财，大吉大利' COMMENT '红包封面文案',
    `total_amount`    bigint       NOT NULL COMMENT '总金额，单位为分',
    `total_count`     int          NOT NULL COMMENT '红包总份数',
    `received_amount` bigint       NOT NULL DEFAULT 0 COMMENT '已领取金额，单位为分',
    `received_count`  int          NOT NULL DEFAULT 0 COMMENT '已领取份数',
    `status`          tinyint      NOT NULL DEFAULT 0 COMMENT '状态：0 ACTIVE 1 COMPLETED 2 EXPIRED',
    `version`         bigint       NOT NULL DEFAULT 0 COMMENT '缓存投影版本',
    `expire_at`       datetime(3)  NOT NULL COMMENT '过期时间',
    `created_time`    datetime(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
    `updated_time`    datetime(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
                                           ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',

    PRIMARY KEY (`red_packet_id`),
    UNIQUE INDEX `uk_sender_client_message` (`sender_id`, `client_message_id`),
    UNIQUE INDEX `uk_message_id` (`message_id`),
    INDEX `idx_session_created` (`session_id`, `created_time`),
    INDEX `idx_sender_created` (`sender_id`, `created_time`),
    INDEX `idx_status_expire` (`status`, `expire_at`)
) ENGINE = InnoDB
  CHARACTER SET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci
  COMMENT = '红包主表';
