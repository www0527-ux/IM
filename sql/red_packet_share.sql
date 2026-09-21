USE `infiniteChat`;

CREATE TABLE `red_packet_share`
(
    `share_id`      bigint      NOT NULL COMMENT '红包份额ID',
    `red_packet_id` bigint      NOT NULL COMMENT '所属红包ID',
    `ordinal`       int         NOT NULL COMMENT '红包内份额序号，从1开始',
    `amount`        bigint      NOT NULL COMMENT '金额，单位为分',
    `status`        tinyint     NOT NULL DEFAULT 0 COMMENT '状态：0 UNCLAIMED 1 CLAIMED',
    `receiver_id`   bigint      NULL COMMENT '领取者用户ID',
    `received_at`   datetime(3) NULL COMMENT '领取时间',
    `created_time`  datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
    `updated_time`  datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
                                        ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',

    PRIMARY KEY (`share_id`),
    UNIQUE INDEX `uk_red_packet_ordinal` (`red_packet_id`, `ordinal`),
    INDEX `idx_red_packet_status` (`red_packet_id`, `status`, `share_id`)
) ENGINE = InnoDB
  CHARACTER SET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci
  COMMENT = '红包预分配份额表';
