USE `infiniteChat`;

CREATE TABLE `system_notification`
(
    `id`           bigint      NOT NULL COMMENT '通知主键ID（雪花ID）',
    `message_id`   varchar(64) NOT NULL COMMENT '事件消息ID，用于防止重复通知',
    `receiver_id`  bigint      NOT NULL COMMENT '接收通知的用户ID',
    `type`         int         NOT NULL COMMENT '类型：101好友申请 102新会话 103群聊邀请',
    `content`      json        NOT NULL COMMENT '完整通知内容',
    `is_read`      tinyint     NOT NULL DEFAULT 0 COMMENT '是否已读：0未读 1已读',
    `created_time` datetime    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time` datetime    NOT NULL DEFAULT CURRENT_TIMESTAMP
                                   ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',

    PRIMARY KEY (`id`),

    UNIQUE INDEX `uk_message_id` (`message_id`),

    INDEX `idx_receiver_read` (`receiver_id`, `is_read`)
) ENGINE = InnoDB
  CHARACTER SET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci
  COMMENT = '系统通知表';