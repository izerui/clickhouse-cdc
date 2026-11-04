-- 本地端到端测试：同步运行中执行的增量变更与 DDL
USE cdc_test;

-- 增删改
INSERT INTO t_order (id, user_id, amount, status) VALUES (100001, 1, 9.99, 'NEW');
UPDATE t_order SET status = 'PAID', amount = amount + 1 WHERE id <= 10;
DELETE FROM t_order WHERE id BETWEEN 11 AND 20;
-- 修改主键：ClickHouse 中旧主键行应标记删除、新主键行写入
UPDATE t_composite SET b = 'z' WHERE a = 1 AND b = 'x';
UPDATE t_all_types SET c_varchar = '已更新', c_json = '{"updated": true}' WHERE id = 1;

-- DDL（DDL 屏障：会在下一个 checkpoint 完成后才作用到 ClickHouse）
ALTER TABLE t_order ADD COLUMN remark VARCHAR(50) NOT NULL DEFAULT '无' COMMENT '备注';
ALTER TABLE t_order RENAME COLUMN status TO order_status;
ALTER TABLE t_order MODIFY COLUMN user_id VARCHAR(32) NOT NULL;
INSERT INTO t_order (id, user_id, amount, order_status, remark) VALUES (100002, 'u2', 1.00, 'NEW', '加列后写入');

-- 新建表、改表名
CREATE TABLE t_new (id INT PRIMARY KEY, name VARCHAR(20));
INSERT INTO t_new VALUES (1, 'n1'), (2, 'n2');
RENAME TABLE t_new TO t_renamed;
INSERT INTO t_renamed VALUES (3, 'n3');

-- 清空表
TRUNCATE TABLE t_composite;
INSERT INTO t_composite VALUES (9, 'k', 9);
