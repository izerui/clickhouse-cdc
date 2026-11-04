-- 本地端到端测试：初始化 MySQL 测试库（会删除并重建 cdc_test 库）
DROP DATABASE IF EXISTS cdc_test;
CREATE DATABASE cdc_test DEFAULT CHARSET utf8mb4;

-- 同步账号：使用 mysql_native_password，避免 binlog 客户端在非 SSL 连接下的 caching_sha2_password 认证问题
CREATE USER IF NOT EXISTS 'cdc'@'%' IDENTIFIED WITH mysql_native_password BY 'Cdc_Test#2026';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdc'@'%';
-- 测试脚本会在 MySQL 中执行 DDL，用 root 执行即可，cdc 账号只需读权限

USE cdc_test;

-- 1. 全类型 + 边界值
CREATE TABLE t_all_types (
    id          BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
    c_tinyint   TINYINT,
    c_tinyint_u TINYINT UNSIGNED,
    c_smallint  SMALLINT,
    c_int       INT,
    c_bigint    BIGINT,
    c_bigint_u  BIGINT UNSIGNED,
    c_float     FLOAT,
    c_double    DOUBLE,
    c_decimal   DECIMAL(18, 4),
    c_bit1      BIT(1),
    c_bit8      BIT(8),
    c_bool      BOOL,
    c_date      DATE,
    c_datetime  DATETIME,
    c_datetime3 DATETIME(3),
    c_datetime6 DATETIME(6),
    c_timestamp TIMESTAMP(3) NULL,
    c_time      TIME,
    c_year      YEAR,
    c_char      CHAR(10),
    c_varchar   VARCHAR(100) COMMENT '变长字符串',
    c_text      TEXT,
    c_json      JSON,
    c_enum      ENUM ('a', 'b', 'c'),
    c_set       SET ('x', 'y', 'z'),
    c_binary    VARBINARY(16),
    c_blob      BLOB,
    c_point     POINT
) COMMENT '全类型测试表';

INSERT INTO t_all_types VALUES
(1, -128, 255, -32768, -2147483648, -9223372036854775808, 18446744073709551615,
 1.1, 3.141592653589793, 12345678901234.5678, b'1', b'10000001', TRUE,
 '2024-02-29', '2024-05-01 10:20:30', '2024-05-01 10:20:30.123', '2024-05-01 10:20:30.123456', '2024-05-01 10:20:30.789',
 '-12:34:56', 2024, 'abc', '制表\t换行\n反斜杠\\单引号''中文', REPEAT('长文本', 100), '{"k": [1, 2, "中"]}', 'b', 'x,z',
 X'00FF0A09', X'DEADBEEF', ST_GeomFromText('POINT(116.397128 39.916527)')),
(2, 127, 0, 32767, 2147483647, 9223372036854775807, 0,
 -0.5, -1e300, -0.0001, b'0', b'00000000', FALSE,
 '1000-01-01', '9999-12-31 23:59:59', '1970-01-01 00:00:00.000', '2299-12-31 23:59:59.999999', '1970-01-01 08:00:01.000',
 '838:59:59', 1901, '', '', '', 'null', 'a', '', '', '', ST_GeomFromText('POINT(0 0)')),
(3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
 NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

-- 2. 大表：10 万行，分片大小 8096，用于验证全量阶段多分片并行读取
CREATE TABLE t_order (
    id         INT PRIMARY KEY,
    user_id    BIGINT        NOT NULL,
    amount     DECIMAL(12, 2) NOT NULL,
    status     VARCHAR(20)   NOT NULL,
    created_at DATETIME(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
) COMMENT '订单';

SET SESSION cte_max_recursion_depth = 200000;
INSERT INTO t_order (id, user_id, amount, status, created_at)
WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 100000)
SELECT n, n % 1000, ROUND(n * 1.25, 2), ELT(n % 3 + 1, 'NEW', 'PAID', 'DONE'),
       TIMESTAMP('2024-01-01') + INTERVAL n SECOND
FROM seq;

-- 3. 联合主键
CREATE TABLE t_composite (
    a INT,
    b VARCHAR(10),
    v INT,
    PRIMARY KEY (a, b)
);
INSERT INTO t_composite VALUES (1, 'x', 1), (1, 'y', 2), (2, 'x', 3);

-- 4. 无主键表：应被跳过，不同步
CREATE TABLE t_nopk (x INT);
INSERT INTO t_nopk VALUES (1);
