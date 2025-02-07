CREATE TABLE IF NOT EXISTS `native_field` (
    `id`                  INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
    `database`            VARCHAR(64) NOT NULL,
    `table_name`          VARCHAR(64) NOT NULL,
    `name`                VARCHAR(64) NOT NULL,
    `field_name`          VARCHAR(64) NOT NULL,
    `field_value_type`    VARCHAR(64) NOT NULL,
    `lcuuid`              CHAR(36) NOT NULL,
    `status`              INTEGER DEFAULT 0 COMMENT '0: normal 1: abnormal',
    `created_at`          DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at`          DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
)ENGINE=innodb DEFAULT CHARSET=utf8;

-- update db_version to latest, remeber update DB_VERSION_EXPECT in migrate/init.go
UPDATE db_version SET version='7.0.1.9';
-- modify end

