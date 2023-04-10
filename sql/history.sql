-- split by sid
CREATE TABLE `balance_history_example` (
    `id`            BIGINT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `time`          DOUBLE NOT NULL,
    `sid`           BIGINT UNSIGNED NOT NULL,
    `business`      TINYINT UNSIGNED NOT NULL COMMENT '1-update 2-trade',
    `change`        DECIMAL(20,2) NOT NULL,
    `balance`       DECIMAL(20,2) NOT NULL,
    `order`         BIGINT UNSIGNED NOT NULL,
    `comment`       TEXT NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- split by sid
CREATE TABLE `order_history_example` (
    `id`            BIGINT UNSIGNED NOT NULL PRIMARY KEY,
    `sid`           BIGINT UNSIGNED NOT NULL,
    `external`      BIGINT UNSIGNED NOT NULL,
    `side`          TINYINT UNSIGNED NOT NULL COMMENT '0-buy 1-sell',
    `create_time`   DOUBLE NOT NULL,
    `update_time`   DOUBLE NOT NULL,
    `finish_time`   DOUBLE NOT NULL,
    `symbol`        VARCHAR(30) NOT NULL,
    `comment`       TEXT NOT NULL,
    `price`         DECIMAL(20,8) NOT NULL,
    `close_price`   DECIMAL(20,8) NOT NULL,
    `lot`           DECIMAL(10,2) NOT NULL,
    `margin`        DECIMAL(10,2) NOT NULL,
    `fee`           DECIMAL(10,2) NOT NULL,
    `swaps`         DECIMAL(10,2) NOT NULL,
    `profit`        DECIMAL(10,2) NOT NULL,
    `tp`            DECIMAL(20,8) NOT NULL,
    `sl`            DECIMAL(20,8) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `balance_daily_example` (
    `id`            BIGINT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `sid`           BIGINT UNSIGNED NOT NULL,
    `deposit`       VARCHAR(50) NOT NULL,
    `profit`        VARCHAR(50) NOT NULL,
    `balance`       VARCHAR(50) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `position` (
    `id`            BIGINT UNSIGNED NOT NULL PRIMARY KEY,
    `sid`           BIGINT UNSIGNED NOT NULL,
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
    `tp`            DECIMAL(20,8) NOT NULL,
    `sl`            DECIMAL(20,8) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `pending` (
    `id`            BIGINT UNSIGNED NOT NULL PRIMARY KEY,
    `sid`           BIGINT UNSIGNED NOT NULL,
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
