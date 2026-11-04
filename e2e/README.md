# 本地端到端测试

在本机用 MySQL + ClickHouse 单文件版，验证全量、增量、DDL、续传。以下命令均在**工程根目录**执行。

## 0. 准备

```bash
# 打包
mvn clean package -DskipTests

# 下载 ClickHouse 官方单文件版（放在工程外的单独目录），前台启动，HTTP 端口 8123，default 用户无密码
# 注意：官方构建站只保留最新版本的 macOS 构建，与线上版本可能不同
mkdir -p ~/clickhouse-local && cd ~/clickhouse-local
curl https://clickhouse.com/ | sh
./clickhouse server
# 另开终端确认：curl http://127.0.0.1:8123/   -> Ok.

# 初始化 MySQL 测试库与同步账号（会删除并重建 cdc_test 库）
MYSQL=/opt/homebrew/opt/mysql@8.0/bin/mysql
$MYSQL -uroot -p < e2e/mysql-init.sql   # 同步账号 cdc / Cdc_Test#2026（满足 MySQL 默认密码强度策略）
# 确认 binlog 与时区（time_zone 需与 e2e/application.properties 中 source.server-time-zone 一致）
$MYSQL -uroot -p -e "SELECT @@log_bin, @@binlog_format, @@binlog_row_image, @@global.time_zone, @@system_time_zone"
```

## 1. 全量同步

```bash
rm -rf e2e/data
java -jar target/clickhouse-cdc.jar --config e2e/application.properties
```

另开终端：

```bash
./e2e/check.sh
java -jar target/clickhouse-cdc.jar --config e2e/application.properties --verify
```

预期：
- 日志出现「以下 1 张表没有主键，不参与同步: [cdc_test.t_nopk]」
- ClickHouse 中有 `t_all_types`、`t_order`、`t_composite`，引擎 `ReplacingMergeTree`，排序键为主键
- 行数：t_all_types 3、t_order 100000、t_composite 3；`--verify` 全部一致（退出码 0）
- `t_all_types` 逐列核对：制表符 / 换行 / 反斜杠 / 中文原样；无符号 BIGINT 最大值 `18446744073709551615`；
  `c_time` 为 `-12:34:56`、`838:59:59`；`c_date` `1000-01-01` 截断为 `1900-01-01`；`c_point` 为 `POINT(116.397128 39.916527)`；第 3 行全为 NULL
- Flink Web UI：http://localhost:8081

## 2. 增量与 DDL

同步保持运行，执行变更：

```bash
$MYSQL -uroot -p < e2e/mysql-changes.sql
```

等待 20 秒左右（DDL 在下一个 checkpoint 完成后才执行，测试配置为 10 秒一次）后 `./e2e/check.sh`，预期：
- t_order：id 1~10 为 `PAID` 且金额 +1；id 11~20 不再出现；新增 100001、100002
- t_order 列：新增 `remark`（默认值 `'无'`，**旧行也显示 `无`**）；`status` 改名为 `order_status` 且**历史数据保留**；`user_id` 变为 `String`
- `t_renamed` 有 3 行（改表名保留了 `t_new` 的数据，不存在 `t_new`）
- t_composite 只剩 `(9, 'k', 9)`
- `t_all_types` id=1 的 `c_varchar` 为 `已更新`（`c_json` 为紧凑格式 `{"updated":true}`，全量阶段的 JSON 为带空格的 MySQL 显示格式，语义相同）

## 3. 续传

```bash
# 3.1 正常停止：Ctrl+C（或 kill <pid>），日志出现「savepoint 已保存」
# 3.2 停机期间在 MySQL 做变更
$MYSQL -uroot -p -e "USE cdc_test; INSERT INTO t_order (id, user_id, amount, order_status) VALUES (200001, 'down', 1, 'NEW'); DELETE FROM t_order WHERE id = 1;"
# 3.3 重新启动
java -jar target/clickhouse-cdc.jar --config e2e/application.properties
```

预期：日志出现「从 checkpoint 恢复: .../savepoint-...」（**不是**重新全量），200001 出现、id=1 被删除，`--verify` 一致。

再试一次强制杀进程：`kill -9 <pid>` 后重启，预期从最近的 `chk-N` 恢复，`--verify` 一致。

## 4. 其他场景（可选）

| 场景 | 操作 | 预期 |
|---|---|---|
| 重新全量 | 加 `--fresh` 启动 | 删除旧 checkpoint，清空目标表后重新全量 |
| 修改同步范围 | 停止后加 `--source.tables=t_order` 启动 | 拒绝启动并提示范围变化；加 `--force-resume` 可继续 |
| ClickHouse 短暂不可用 | 停掉 ClickHouse 1 分钟后再启动 | 日志出现重试 / 作业重启，ClickHouse 恢复后自动继续，数据一致 |

## 清理

```bash
$MYSQL -uroot -p -e "DROP DATABASE cdc_test; DROP USER 'cdc'@'%';"
curl -sS http://127.0.0.1:8123/ --data-binary "DROP DATABASE IF EXISTS cdc_test"
rm -rf e2e/data
```
