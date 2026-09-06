USE `infiniteChat`;

CREATE TABLE `user`
(
    `user_id`      bigint       NOT NULL COMMENT '用户ID（分布式ID）',
    `phone`        char(11)     NULL DEFAULT NULL COMMENT '手机号',
    `email`        varchar(128) NULL DEFAULT NULL COMMENT '邮箱',
    `password`     varchar(256) NOT NULL COMMENT '加密后的密码',
    `nickname`     varchar(128) NOT NULL COMMENT '昵称',
    `avatar`       varchar(512) NULL DEFAULT NULL COMMENT '头像URL',
    `gender`       tinyint      NOT NULL DEFAULT 2 COMMENT '性别：0女 1男 2未知',
    `description`  text         NULL COMMENT '个性签名',
    `state`        tinyint      NOT NULL DEFAULT 0 COMMENT '状态：0正常 1封禁 2注销',
    `role`         tinyint      NOT NULL DEFAULT 0 COMMENT '角色：0用户 1管理员 2超级管理员',
    `created_time` datetime     NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time` datetime     NOT NULL DEFAULT CURRENT_TIMESTAMP
                                  ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    `is_delete`    tinyint      NOT NULL DEFAULT 0 COMMENT '删除标记：0未删除 1已删除',

    PRIMARY KEY (`user_id`),
    UNIQUE INDEX `idx_email` (`email`),
    UNIQUE INDEX `idx_phone` (`phone`),
    INDEX `idx_state` (`state`),
    INDEX `idx_role` (`role`),
    INDEX `idx_created_time` (`created_time`)
) ENGINE = InnoDB
  CHARACTER SET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci
  COMMENT = '用户表';

  DESC `user`;
  SHOW INDEX FROM `user`;