USE `infiniteChat`;

CREATE TABLE `apply_friend`
(
    `apply_friend_id` bigint       NOT NULL COMMENT '申请ID',
    `sender_id`       bigint       NOT NULL COMMENT '发送者用户ID',
    `receiver_id`     bigint       NOT NULL COMMENT '接收者用户ID',
    `message`         varchar(255) NOT NULL DEFAULT '请求添加好友' COMMENT '申请信息',
    `status`          tinyint      NOT NULL DEFAULT 0
                                   COMMENT '状态：0未读 1通过 2拒绝 3已读 4过期',
    `created_time`    datetime     NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time`    datetime     NOT NULL DEFAULT CURRENT_TIMESTAMP
                                      ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',

    PRIMARY KEY (`apply_friend_id`),

    UNIQUE INDEX `uk_sender_receiver` (`sender_id`, `receiver_id`),

    INDEX `idx_sender_status` (`sender_id`, `status`),

    INDEX `idx_receiver_status` (`receiver_id`, `status`),

    CONSTRAINT `chk_apply_not_self`
        CHECK (`sender_id` <> `receiver_id`)
) ENGINE = InnoDB
  CHARACTER SET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci
  COMMENT = '好友申请表';