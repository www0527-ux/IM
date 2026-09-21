USE `infiniteChat`;

CREATE TABLE `message`
(
    `message_id`        bigint      NOT NULL COMMENT '服务端消息ID',
    `client_message_id` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin
                                    NOT NULL COMMENT '客户端消息ID',
    `sender_id`         bigint      NOT NULL COMMENT '发送者ID',
    `session_id`        bigint      NOT NULL COMMENT '所属会话ID',
    `seq`               bigint      NOT NULL COMMENT '会话内递增序号',
    `session_type`      tinyint     NOT NULL COMMENT '会话类型：0单聊 1群聊 2机器人',
    `message_type`      tinyint     NOT NULL COMMENT '消息类型：0文本 1图片 2表情 3红包',
    `red_packet_id`     bigint      NULL COMMENT '红包消息关联的红包ID',
    `content`           text        NOT NULL COMMENT '消息内容',
    `reply_id`          bigint      NULL DEFAULT NULL COMMENT '引用的消息ID',
    `created_time`      datetime(3) NOT NULL COMMENT '服务端消息创建时间',

    PRIMARY KEY (`message_id`),
    UNIQUE INDEX `uk_sender_client_message` (`sender_id`, `client_message_id`),
    UNIQUE INDEX `uk_session_seq` (`session_id`, `seq`),
    UNIQUE INDEX `uk_red_packet_id` (`red_packet_id`),
    INDEX `idx_reply_id` (`reply_id`)
) ENGINE = InnoDB
  CHARACTER SET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci
  COMMENT = '聊天消息表';
