USE `infiniteChat`;

CREATE TABLE `outbox_event`
(
    `event_id`       bigint       NOT NULL COMMENT '事件ID',
    `event_type`     varchar(64)  NOT NULL COMMENT '事件类型',
    `aggregate_type` varchar(32)  NOT NULL COMMENT '聚合类型',
    `aggregate_id`   bigint       NOT NULL COMMENT '聚合ID',
    `aggregate_seq`  bigint       NULL COMMENT '会话内事件顺序；MESSAGE_PERSISTED填写message.seq',
    `topic`          varchar(128) NOT NULL COMMENT 'Kafka Topic',
    `message_key`    varchar(128) NOT NULL COMMENT 'Kafka消息Key',
    `payload`        json         NOT NULL COMMENT '完整事件载荷',
    `status`         tinyint      NOT NULL DEFAULT 0 COMMENT '状态：0 PENDING 1 PROCESSING 2 PUBLISHED',
    `retry_count`    int          NOT NULL DEFAULT 0 COMMENT '已重试次数',
    `next_retry_at`  datetime(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '下次允许重试时间',
    `lease_owner`    varchar(64)  NULL COMMENT '当前租约持有者',
    `lease_until`    datetime(3)  NULL COMMENT '处理租约到期时间',
    `last_error`     text         NULL COMMENT '最后一次发布错误',
    `created_time`   datetime(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
    `updated_time`   datetime(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
                                           ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
    `published_time` datetime(3)  NULL COMMENT 'Broker确认接收时间',

    PRIMARY KEY (`event_id`),
    INDEX `idx_pending_schedule` (`status`, `next_retry_at`, `lease_until`, `created_time`),
    INDEX `idx_aggregate` (`aggregate_type`, `aggregate_id`),
    INDEX `idx_aggregate_seq` (`aggregate_type`, `aggregate_id`, `aggregate_seq`)
) ENGINE = InnoDB
  CHARACTER SET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci
  COMMENT = '事务Outbox事件表';
