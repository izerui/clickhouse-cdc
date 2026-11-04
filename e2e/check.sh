#!/usr/bin/env bash
# 查看 ClickHouse 中的同步结果：./e2e/check.sh
CH=${CH:-http://127.0.0.1:8123}
q() { echo "-- $1"; curl -sS "$CH/" --data-binary "$1 FORMAT PrettyCompactMonoBlock"; echo; }

q "SELECT name, engine, sorting_key, comment FROM system.tables WHERE database = 'cdc_test' ORDER BY name"
q "SELECT 't_all_types' t, count() FROM cdc_test.t_all_types FINAL WHERE _is_deleted = 0
   UNION ALL SELECT 't_order', count() FROM cdc_test.t_order FINAL WHERE _is_deleted = 0
   UNION ALL SELECT 't_composite', count() FROM cdc_test.t_composite FINAL WHERE _is_deleted = 0"
q "SELECT name, type, default_expression, comment FROM system.columns WHERE database = 'cdc_test' AND table = 't_order' ORDER BY position"
q "SELECT * EXCEPT (_synced_at) FROM cdc_test.t_order FINAL WHERE _is_deleted = 0 AND (id <= 12 OR id >= 100000) ORDER BY id"
q "SELECT * EXCEPT (_synced_at) FROM cdc_test.t_composite FINAL ORDER BY a, b"
echo "-- 全类型表（逐列查看）"
curl -sS "$CH/" --data-binary "SELECT * FROM cdc_test.t_all_types FINAL ORDER BY id FORMAT Vertical"
