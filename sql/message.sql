USE `infiniteChat`;

CREATE TABLE `message`
(
    `message_id`   bigint   NOT NULL COMMENT '消息ID',
    `sender_id`    bigint   NOT NULL COMMENT '发送者ID',
    `session_id`   bigint   NOT NULL COMMENT '所属会话ID',
    `type`         tinyint  NOT NULL COMMENT '类型：0文本 1图片 2表情包 3红包',
    `content`      text     NOT NULL COMMENT '消息内容',
    `reply_id`     bigint   NULL DEFAULT NULL COMMENT '引用的消息ID',
    `session_type` tinyint  NOT NULL COMMENT '会话类型：0单聊 1群聊 2机器人',
    `created_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '发送时间',
    `updated_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP
                                ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',

    PRIMARY KEY (`message_id`),

    INDEX `idx_session_time` (`session_id`, `created_time`),

    INDEX `idx_sender_time` (`sender_id`, `created_time`),

    INDEX `idx_reply_id` (`reply_id`)
) ENGINE = InnoDB
  CHARACTER SET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci
  COMMENT = '消息表';