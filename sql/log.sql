CREATE TABLE `slice_balance_example` (
    `id`            INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `sid`           BIGINT UNSIGNED NOT NULL,
    `t`             TINYINT UNSIGNED NOT NULL COMMENT '1-balance 2-equity 3-margin 4-margin_free',
    `balance`       DECIMAL(20,2) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `slice_position_example` (
    `id`            BIGINT UNSIGNED NOT NULL PRIMARY KEY,
    `sid`           BIGINT UNSIGNED NOT NULL,
    `external`      BIGINT UNSIGNED NOT NULL,
    `side`          TINYINT UNSIGNED NOT NULL COMMENT '0-buy 1-sell',
    `create_time`   DOUBLE NOT NULL,
    `update_time`   DOUBLE NOT NULL,
    `symbol`        VARCHAR(30) NOT NULL,
    `comment`       TEXT NOT NULL,
    `price`         DECIMAL(20,8) NOT NULL,
    `lot`           DECIMAL(10,2) NOT NULL,
    `margin`        DECIMAL(10,2) NOT NULL,
    `fee`           DECIMAL(10,2) NOT NULL,
    `swap`          DECIMAL(10,4) NOT NULL,
    `swaps`         DECIMAL(10,2) NOT NULL,
    `tp`            DECIMAL(20,8) NOT NULL,
    `sl`            DECIMAL(20,8) NOT NULL,
    `margin_price`  DECIMAL(20,8) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `slice_limit_example` (
    `id`            BIGINT UNSIGNED NOT NULL PRIMARY KEY,
    `sid`           BIGINT UNSIGNED NOT NULL,
    `external`      BIGINT UNSIGNED NOT NULL,
    `side`          TINYINT UNSIGNED NOT NULL COMMENT '0-buy 1-sell',
    `create_time`   DOUBLE NOT NULL,
    `expire_time`   BIGINT UNSIGNED NOT NULL,
    `symbol`        VARCHAR(30) NOT NULL,
    `comment`       TEXT NOT NULL,
    `price`         DECIMAL(20,8) NOT NULL,
    `lot`           DECIMAL(10,2) NOT NULL,
    `margin`        DECIMAL(10,2) NOT NULL,
    `fee`           DECIMAL(10,2) NOT NULL,
    `swap`          DECIMAL(10,4) NOT NULL,
    `tp`            DECIMAL(20,8) NOT NULL,
    `sl`            DECIMAL(20,8) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `slice_history` (
    `id`            INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `time`          BIGINT NOT NULL,
    `end_oper_id`   BIGINT UNSIGNED NOT NULL,
    `end_order_id`  BIGINT UNSIGNED NOT NULL,
    `end_deals_id`  BIGINT UNSIGNED NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `operlog_example` (
    `id`            BIGINT UNSIGNED NOT NULL PRIMARY KEY,
    `time`          DOUBLE NOT NULL,
    `detail`        TEXT
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
