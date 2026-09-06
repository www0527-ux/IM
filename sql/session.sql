USE `infiniteChat`;

CREATE TABLE `session`
(
    `session_id`   bigint       NOT NULL COMMENT '会话ID',
    `name`         varchar(255) NOT NULL COMMENT '会话名称',
    `type`         tinyint      NOT NULL COMMENT '类别：0单聊 1群聊 2机器人',
    `single_key` varchar(64) NULL DEFAULT NULL COMMENT '单聊唯一键：较小用户ID:较大用户ID，群聊为空',
    `status`       tinyint      NOT NULL DEFAULT 0 COMMENT '状态：0正常 1删除',
    `avatar`       varchar(512) NULL DEFAULT NULL COMMENT '会话头像URL',
    `created_time` datetime     NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time` datetime     NOT NULL DEFAULT CURRENT_TIMESTAMP
                                   ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',

    PRIMARY KEY (`session_id`),

    UNIQUE INDEX `uk_single_key` (`single_key`)
) ENGINE = InnoDB
  CHARACTER SET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci
  COMMENT = '会话表';
  