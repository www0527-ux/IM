USE `infiniteChat`;

CREATE TABLE `balance_log`
(
    `balance_log_id` bigint   NOT NULL COMMENT '流水ID',
    `user_id`        bigint   NOT NULL COMMENT '用户ID',
    `amount`         bigint   NOT NULL COMMENT '变动金额，单位为分；正数增加，负数减少',
    `type`           tinyint  NOT NULL COMMENT '类型：0发送红包 1领取红包 2红包退回',
    `related_id`     bigint   NULL DEFAULT NULL COMMENT '关联业务ID，例如红包ID',
    `created_time`   datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time`   datetime NOT NULL DEFAULT CURRENT_TIMESTAMP
                                  ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',

    PRIMARY KEY (`balance_log_id`),
    INDEX `idx_user_id` (`user_id`),
    INDEX `idx_related_id` (`related_id`)
) ENGINE = InnoDB
  CHARACTER SET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci
  COMMENT = '余额变动记录表';