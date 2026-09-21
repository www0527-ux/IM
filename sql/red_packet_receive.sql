USE `infiniteChat`;

CREATE TABLE `red_packet_receive`
(
    `red_packet_receive_id` bigint   NOT NULL COMMENT '领取记录ID',
    `red_packet_id`         bigint   NOT NULL COMMENT '红包ID',
    `share_id`              bigint      NOT NULL COMMENT '领取的份额ID',
    `receiver_id`           bigint      NOT NULL COMMENT '领取者用户ID',
    `amount`                bigint      NOT NULL COMMENT '领取金额，单位为分',
    `received_at`           datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '实际领取时间',
    `created_time`          datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '记录创建时间',
    `updated_time`          datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
                                            ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',

    PRIMARY KEY (`red_packet_receive_id`),

    UNIQUE INDEX `uk_red_packet_receiver`
        (`red_packet_id`, `receiver_id`),

    UNIQUE INDEX `uk_share_id` (`share_id`),

    INDEX `idx_red_packet_id` (`red_packet_id`),

    INDEX `idx_receiver_id` (`receiver_id`)
) ENGINE = InnoDB
  CHARACTER SET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci
  COMMENT = '红包领取记录表';
