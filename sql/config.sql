CREATE TABLE `group` (
    `id`            INT UNSIGNED NOT NULL PRIMARY KEY,
    `group`         VARCHAR(50) NOT NULL,
    `leverage`      INT UNSIGNED NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `symbol` (
    `id`            INT UNSIGNED NOT NULL PRIMARY KEY,
    `symbol`        VARCHAR(30) NOT NULL,
    `security`      VARCHAR(30) NOT NULL,
    `digit`         INT UNSIGNED NOT NULL,
    `currency`      VARCHAR(30) NOT NULL,
    `contract_size` INT UNSIGNED NOT NULL,
    `percentage`    INT UNSIGNED NOT NULL,
    `margin_calc`   TINYINT UNSIGNED NOT NULL COMMENT '1-Forex 2-CFD',
    `profit_calc`   TINYINT UNSIGNED NOT NULL COMMENT '1-Forex 2-CFD 3-Futures',
    `swap_calc`     TINYINT UNSIGNED NOT NULL COMMENT '1-money 2-USD',
    `tick_size`     DECIMAL(10,2) NOT NULL,
    `tick_price`    INT UNSIGNED NOT NULL,
    `monday`        VARCHAR(50) NOT NULL DEFAULT ,
    `tuesday`       VARCHAR(50) NOT NULL DEFAULT ,
    `wednesday`     VARCHAR(50) NOT NULL DEFAULT ,
    `thursday`      VARCHAR(50) NOT NULL DEFAULT ,
    `friday`        VARCHAR(50) NOT NULL DEFAULT
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `fee` (
    `id`            INT UNSIGNED NOT NULL PRIMARY KEY,
    `symbol`        VARCHAR(30) NOT NULL,
    `group`         VARCHAR(50) NOT NULL,
    `percentage`    INT UNSIGNED NOT NULL,
    `fee`           DECIMAL(10,2) NOT NULL COMMENT '$/lot',
    `swap_long`     DECIMAL(10,4) NOT NULL,
    `swap_short`    DECIMAL(10,4) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
