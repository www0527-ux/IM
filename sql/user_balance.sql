USE `infiniteChat`;

CREATE TABLE `user_balance`
(
    `user_id`      bigint   NOT NULL COMMENT '用户ID',
    `balance`      bigint   NOT NULL DEFAULT 0 COMMENT '余额，单位为分',
    `created_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP
                                  ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',

    PRIMARY KEY (`user_id`)
) ENGINE = InnoDB
  CHARACTER SET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci
  COMMENT = '用户余额表';
  DESC `user_balance`;