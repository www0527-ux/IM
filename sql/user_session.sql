USE `infiniteChat`;

CREATE TABLE `user_session`
(
    `user_id`      bigint   NOT NULL COMMENT '用户ID',
    `session_id`   bigint   NOT NULL COMMENT '会话ID',
    `role`         tinyint  NOT NULL COMMENT '角色：0群主 1管理员 2普通成员',
    `status`       tinyint  NOT NULL DEFAULT 0 COMMENT '状态：0正常 1已退出',
    `created_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '加入时间',
    `updated_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP
                                ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',

    PRIMARY KEY (`user_id`, `session_id`),

    INDEX `idx_session_status` (`session_id`, `status`)
) ENGINE = InnoDB
  CHARACTER SET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci
  COMMENT = '用户会话关系表';