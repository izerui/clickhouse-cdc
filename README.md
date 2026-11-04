# clickhouse-cdc

MySQL → ClickHouse 通用 CDC 实时同步工具。内嵌 Flink 本地集群，`java -jar` 单进程运行，不需要部署 Flink 集群。

- 全量 + 增量（基于 Flink CDC 无锁增量快照，可并行读取）或仅增量
- 自动建库建表、自动同步表结构变更（加列 / 删列 / 改类型 / 改列名 / 注释 / TRUNCATE / DROP）
- 目标表使用标准 `ReplacingMergeTree(_version, _is_deleted)`，增删改都转换为带版本号的 INSERT，重复写入幂等
- 本地 checkpoint 断点续传，进程重启后自动从上次位置继续

## 环境要求

| 组件 | 版本 |
|---|---|
| JDK | 11+ |
| MySQL | 5.7 / 8.x，开启 `binlog_format=ROW`、`binlog_row_image=FULL` |
| ClickHouse | 23.2+（开源版，使用 HTTP 接口） |

MySQL 同步账号需要的权限：

```sql
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdc'@'%';
```

## 快速开始

```bash
# 1. 构建
mvn clean package
# 产物：target/clickhouse-cdc.jar

# 2. 修改配置
vi conf/application.properties

# 3. 运行
java -jar target/clickhouse-cdc.jar --config conf/application.properties
```

本地联调可以用 `docker compose up -d` 启动 MySQL 8 和 ClickHouse。

### 选择要同步的表

| 配置 | 说明 |
|---|---|
| `source.databases` | 要同步的库，逗号分隔 |
| `source.tables` | 只同步指定的表，逗号分隔：表名（在每个库中匹配），或 `库名.表名`。配置后 `source.tables.include` 不再生效；启动时若指定的表不存在会直接报错 |
| `source.tables.include` | 未配置 `source.tables` 时按此正则过滤表名，默认 `.*`（全部） |
| `source.tables.exclude` | 排除的表名正则，始终生效 |

没有主键的表无论如何配置都不同步（启动时打印被跳过的表）。

### 命令行参数

| 参数 | 说明 |
|---|---|
| `--config <path>` | 配置文件，默认 `conf/application.properties` |
| `--fresh` | 删除已有 checkpoint，按 `startup.mode` 重新开始（initial 模式会清空目标表重新全量） |
| `--force-resume` | 续传时跳过同步范围变化检查（确认新纳入的表只需增量时使用） |
| `--restore <目录>` | 从指定的 checkpoint / savepoint 目录（含 `_metadata`）恢复，不自动清理其他 checkpoint |
| `--verify` | 逐表比对 MySQL 与 ClickHouse 行数后退出，全部一致退出码 0，否则 3（不启动同步） |
| `--key=value` | 覆盖配置文件中的任意配置项，如 `--job.parallelism=8` |

配置值可以引用环境变量：`source.password=${MYSQL_PASSWORD}`、`clickhouse.password=${CH_PASSWORD:默认值}`。

## 镜像发布（GitHub Actions）

`.github/workflows/docker-image.yml`：每次提交推送到 `main`，打包 jar（含单元测试）后构建 `linux/amd64`、`linux/arm64` 镜像，推送到 GHCR：`ghcr.io/<owner>/clickhouse-cdc:latest`（使用仓库自带的 `GITHUB_TOKEN`，无需额外配置）。不需要打 tag。

K8s 通过 Harbor 的 `ghcr` 代理项目拉取：`registry.bstllm.com/ghcr/<owner>/clickhouse-cdc:latest`。Harbor 代理匿名拉取上游，**GHCR 镜像包需设为公开**（GitHub 仓库 Packages → clickhouse-cdc → Package settings → Change visibility），或在 Harbor 的 ghcr 代理端点配置 GitHub 凭据。清单使用 `imagePullPolicy: Always`，执行 `kubectl rollout restart` 即可更新到最新镜像。

## K8s 部署

清单位于 `deploy/k8s`（kustomize）：

| 文件 | 说明 |
|---|---|
| `application.properties` | 同步配置，生成带内容哈希的 ConfigMap 挂载到 `/app/conf`，修改后 apply 会自动滚动重启 |
| `secret.example.yaml` | `MYSQL_PASSWORD` / `CLICKHOUSE_PASSWORD` 示例，以环境变量注入，配置中用 `${MYSQL_PASSWORD}` 引用。**不在 kustomize 资源中**，避免 apply 时用示例值覆盖真实密码 |
| `statefulset.yaml` | 单副本 StatefulSet + PVC（`/app/data` 保存同步进度） |
| `service.yaml` | StatefulSet 所需的 headless Service，暴露健康检查端口 |

```bash
# 1. 构建镜像并推送到镜像仓库
mvn clean package -DskipTests
docker build -t ghcr.io/<owner>/clickhouse-cdc:latest .
docker push ghcr.io/<owner>/clickhouse-cdc:latest

# 2. 创建密码 Secret（只需一次）
kubectl create secret generic clickhouse-cdc-secret \
  --from-literal=MYSQL_PASSWORD='***' --from-literal=CLICKHOUSE_PASSWORD='***'

# 3. 修改 deploy/k8s/application.properties，以及 kustomization.yaml 中的镜像地址 / 命名空间，然后部署（kubectl >= 1.27）
kubectl apply -k deploy/k8s

# 查看同步状态 / 日志
kubectl get pod -l app.kubernetes.io/name=clickhouse-cdc
kubectl port-forward clickhouse-cdc-0 8080:8080 &   # 然后 curl localhost:8080/status
kubectl logs -f clickhouse-cdc-0

# 数据核对
kubectl exec clickhouse-cdc-0 -- sh -c 'exec java -Dlogback.configurationFile=logback-console.xml -jar /app/clickhouse-cdc.jar --config /app/conf/application.properties --verify'
```

部署要点：

- **只能单副本**：多个实例会重复消费 binlog 并争用同一份进度，不要调大 `replicas`。
- **同步进度在 PVC 上**：Pod 重建、节点迁移后从原 binlog 位点续传；删除 PVC 等于丢失进度，下次启动会重新全量。
- **优雅退出**：Pod 删除 / 滚动更新时 java 进程收到 SIGTERM，先 stop-with-savepoint 再退出；`terminationGracePeriodSeconds: 180` 需大于 savepoint 等待时间（120 秒）。
- **探针**：`/healthz` 只检查进程存活（MySQL / ClickHouse 暂时不可用时作业在进程内自动重试，不重建 Pod）；`/readyz` 在作业 RUNNING 时就绪；启动阶段预建表较慢，`startupProbe` 最长等待 10 分钟。启动失败时进程以非 0 退出码结束，由 K8s 重启。
- **重新全量**：在 `statefulset.yaml` 的 `args` 中临时加上 `--fresh` 后 apply，确认新作业已产生 checkpoint 后再去掉并 apply（否则之后每次重启都会重新全量）。`--fresh` 会删除旧进度，中途退出也不会退回旧位点。
- **时区**：`source.server-time-zone` 必须与 MySQL 一致（容器默认 UTC，已在清单中设置 `TZ=Asia/Shanghai`）。
- **server-id**：K8s 中建议固定 `source.server-id` 区间，避免与其他从库 / CDC 任务冲突。
- **资源**：默认 requests 1C/2Gi、limits 4C/2Gi，JVM 堆占容器内存 60%，其余留给 Flink 网络缓冲与托管内存；表多、批次大时适当调大。

本地运行时健康检查默认关闭（`health.port=0`），日志同时输出到控制台和 `logs/`；容器内通过 `-Dlogback.configurationFile=logback-console.xml` 只输出到标准输出。

## 启动与续传

| 场景 | 行为 |
|---|---|
| 首次启动，`startup.mode=initial` | 建表 →（默认）清空目标表 → 全量快照 → 自动切换增量 |
| 首次启动，`latest / earliest / timestamp` | 建表 → 从指定 binlog 位置开始增量 |
| 再次启动（存在 checkpoint） | 自动从最近的 checkpoint / savepoint 续传，忽略 `startup.mode` |
| `--fresh` | 删除全部旧 checkpoint，按 `startup.mode` 重新开始 |
| 同步范围变化后再次启动 | 拒绝启动。`source.databases`、`source.tables`、`source.tables.*` 变化，或上次因无主键未同步的表现已有主键，续传会让新纳入的表缺少全量数据。需恢复原配置、使用 `--fresh`，或确认只需增量时加 `--force-resume` |

- checkpoint 保存在 `checkpoint.dir`（默认 `./data/checkpoints`），每 `checkpoint.interval-ms` 一次；**该目录就是同步进度，请持久化保存**。
- 每次启动都是一个新的 Flink 作业；新作业产生第一个 checkpoint 后，自动清理旧作业留下的 checkpoint / savepoint。
- 续传时不会按 MySQL 当前结构校正目标表，停机期间的表结构变更随 binlog 中的 DDL 按顺序回放。
- 续传依赖 MySQL 上对应位置的 binlog 仍然存在。停机时间超过 binlog 保留期（`binlog_expire_logs_seconds`）时无法续传，只能 `--fresh` 重新全量。
- 进程收到 `SIGTERM`（`kill <pid>`、Ctrl+C）时会尝试执行 stop-with-savepoint 后退出。
- 运行中出错（如 MySQL / ClickHouse 暂时不可用）时，作业在进程内从最近的 checkpoint 自动重启。默认 `exponential-delay` 策略无限次重试，间隔从 10 秒指数增长到 5 分钟；若改为 `fixed-delay`，超过次数后进程以退出码 1 结束，需由 systemd / supervisor 等拉起。

## 目标表设计

以源表 `demo.t_order(id bigint pk, amount decimal(12,2), created_at datetime(3))` 为例，自动创建：

```sql
CREATE TABLE IF NOT EXISTS `demo`.`t_order`
(
    `id` Int64,
    `amount` Nullable(Decimal(12, 2)),
    `created_at` DateTime64(3, 'Asia/Shanghai'),
    `_version` UInt64,                          -- 版本号：全量为 0，增量为单调递增的微秒时间戳
    `_is_deleted` UInt8,                        -- 1 表示该主键已删除
    `_op` LowCardinality(String),               -- 源端操作 r/c/u/d
    `_source_ts` DateTime64(3, 'Asia/Shanghai'),-- 源端事件时间（全量为 1970-01-01）
    `_synced_at` DateTime64(3, 'Asia/Shanghai') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(`_version`, `_is_deleted`)
ORDER BY (`id`)
```

ReplacingMergeTree 在后台合并时去重，**查询最新数据请使用 `FINAL`**：

```sql
SELECT * FROM demo.t_order FINAL WHERE _is_deleted = 0;
SELECT count() FROM demo.t_order FINAL WHERE _is_deleted = 0;
```

### 类型映射

| MySQL | ClickHouse |
|---|---|
| tinyint / smallint / mediumint / int / bigint（含 unsigned） | Int8~Int64 / UInt8~UInt64 |
| float / double | Float32 / Float64 |
| decimal(p,s) | Decimal(p, s) |
| bit(1) / bit(n) | Bool / UInt64 |
| year | UInt16 |
| date | Date32 |
| datetime(p) / timestamp(p) | DateTime64(p, '`source.server-time-zone`') |
| time | String（如 `838:59:59`） |
| enum | LowCardinality(String) |
| char / varchar / text / json / set | String |
| binary / varbinary / blob | String（原始字节） |
| geometry 等空间类型 | String（WKT 文本，如 `POINT(1 2)`） |

- 可空列使用 `Nullable(...)`，主键列（ORDER BY）不使用。
- 超出 ClickHouse 范围的日期时间（早于 1900-01-01 或晚于 2299-12-31）会截断到边界。
- JSON 的文本格式在全量与增量阶段不同：全量为 MySQL 的显示格式（`{"k": [1, 2]}`，带空格），增量为 Debezium 解析 binlog 后的紧凑格式（`{"k":[1,2]}`）。两者语义相同，`JSONExtract*` 等函数均可正确解析，但不要直接按字符串比较。

### 表结构变更

同步方式是“结构比对”：拿到源表变更后的完整结构，与目标表比较后补齐差异，不翻译 MySQL DDL 原文。

| 源端操作 | 目标端 |
|---|---|
| CREATE TABLE | 自动建表（需有主键） |
| ADD COLUMN | `ADD COLUMN ... AFTER`，带上可安全翻译的默认值（数字、字符串、日期），已有的行与 MySQL 一样取到默认值；`CURRENT_TIMESTAMP` 等表达式默认值不传播 |
| DROP COLUMN | `DROP COLUMN`（`sink.drop-column-enabled`） |
| MODIFY / CHANGE 类型 | `MODIFY COLUMN`（主键列不支持，打印警告） |
| RENAME COLUMN / CHANGE 改名 | `RENAME COLUMN` |
| 修改注释 | `COMMENT COLUMN` |
| TRUNCATE TABLE | `TRUNCATE TABLE`（`sink.truncate-table-enabled`） |
| DROP TABLE | 默认保留目标表（`sink.drop-table-enabled`） |

## 数据一致性说明

- **不丢数据**：每次 checkpoint 前先把缓冲的数据全部写入 ClickHouse，checkpoint 中记录的 binlog 位点之前的数据一定已经落库；写入失败会重试，仍失败则作业重启并从上一个 checkpoint 重放。
- **重复数据**：重放与重试会产生重复写入，由 `ReplacingMergeTree(_version, _is_deleted)` 按主键保留最新版本消化。去重发生在后台合并，查询时请使用 `FINAL`。
- **版本号**：全量快照为 0（Flink CDC 只会发出快照高水位之后的 binlog，增量一定比快照新）；增量为单调递增的微秒时间戳，已用过的最大值随 checkpoint 保存，重启后时钟回拨也不会变小。
- **按库顺序处理**：写入算子按库分区，同一个库的 DDL 与数据由同一个写入实例严格按 binlog 顺序处理（RENAME TABLE 等 DDL 涉及多张表，按表分区会失去先后顺序），实例内部仍按表分别缓冲、批量写入。因此每个库只有一个写入实例，全量阶段的读取仍然并行。
- **DDL 屏障**：DDL 到达后先写出该表缓冲，DDL 及该表之后的事件暂存在 Flink 状态中，等下一个 checkpoint 完成后再执行 DDL、写入暂存数据（该表的变更会延迟最多一个 checkpoint 间隔）。这样任何故障恢复都只会回放 DDL 之后的数据，不会出现"目标表结构已变更、又回放变更前的数据"——否则列改名会被当成删除列而丢失历史数据，改类型后回放旧值会写入失败。
- **停机期间的表结构变更**：续传时随 binlog 中的 DDL 按顺序回放，与运行中的处理方式相同。

## 数据核对

```bash
java -jar target/clickhouse-cdc.jar --config conf/application.properties --verify
```

按同步范围逐表比对 `MySQL COUNT(*)` 与 ClickHouse `count() ... FINAL WHERE _is_deleted = 0`。同步运行中执行时，写入频繁的表可能因同步延迟出现少量差异，可在低峰期执行或多次比对确认。

## 与官方方案的对比

Apache Flink CDC 官方目前没有 ClickHouse pipeline 连接器；ClickHouse 官方的 Flink 连接器只是一个通用写入 Sink（至少一次），不处理建表、表结构变更、去重与删除。因此以 **ClickHouse 官方的 MySQL CDC（ClickPipes）** 作为对照，本工程在表设计与同步语义上与其保持一致：

| 方面 | ClickPipes（官方） | 本工程 |
|---|---|---|
| 表引擎 | `ReplacingMergeTree(_peerdb_version)` | `ReplacingMergeTree(_version, _is_deleted)` |
| 附加字段 | `_peerdb_version`、`_peerdb_is_deleted`、`_peerdb_synced_at` | `_version`、`_is_deleted`、`_synced_at`，另有 `_op`、`_source_ts` 便于排查 |
| 更新 / 删除 | 写入新版本行；删除写 `is_deleted = 1` | 相同 |
| 排序键 | 源表主键 | 相同 |
| 查询 | `FINAL WHERE _peerdb_is_deleted = 0` | `FINAL WHERE _is_deleted = 0` |
| 全量 + 增量 | 支持 | 支持（Flink CDC 无锁增量快照） |
| ADD COLUMN | 同步，带可翻译的默认值 | 相同 |
| DROP COLUMN | 不同步（保留列） | 默认同步删除，`sink.drop-column-enabled=false` 时与官方一致 |
| MODIFY / RENAME COLUMN、TRUNCATE | 不支持 | 支持 |
| 全量阶段加列 | 不支持（需手工处理） | 自动按源表当前结构补列 |
| 整数 / 浮点 / 字符串 / JSON / SET / 二进制 | 同宽整数（含无符号）、Float32/64、String | 相同 |
| ENUM | LowCardinality(String) | 相同 |
| 空间类型 | String（WKT） | 相同 |
| DATE | Date32 | 相同 |
| DATETIME / TIMESTAMP | DateTime64(6) | DateTime64(源列精度, 源库时区) |
| DECIMAL | 统一 Decimal(76,38)，整数部分超过 38 位写为 0 | 按源列 Decimal(p,s)，不丢精度 |
| TIME | DateTime64(6)（以 1970-01-01 为日期） | String（如 `838:59:59`，支持负数与超过 24 小时） |
| TINYINT(1) / BIT | Bool / UInt64 | Int8 / BIT(1) 为 Bool、BIT(n) 为 UInt64 |
| YEAR | Int16 | UInt16 |

## 约束与说明

- **无主键表不同步**：启动时会打印被跳过的表。
- **库名、表名与 MySQL 完全一致**，不做任何改名、映射或多表合并。
- **修改主键列值**：会写入一条旧主键的删除行和一条新主键的新增行。
- **主键定义变化**（增减主键列）：ClickHouse 无法修改排序键，需要人工重建目标表后用 `--fresh` 重新全量。
- **分区**：默认不分区。如需分区，`clickhouse.partition-by` 必须使用同一主键下取值不变的列，否则 ReplacingMergeTree 无法跨分区去重。
- **表名过滤规则**中不能包含逗号（Debezium 用逗号分隔多条规则），多个规则用 `|` 连接。
- 源端 `RENAME TABLE` 时，目标端同步执行 `RENAME TABLE`，保留历史数据（原表不在同步范围内时按新表创建）。
- 源端 DROP 后又 CREATE 同名表时，若 `sink.drop-table-enabled=false`，目标表会保留旧数据（MySQL 的 `CREATE TABLE IF NOT EXISTS` 在表已存在时也会写入 binlog，无法据此安全清空目标表）。

### 已知限制

以下情况属于"至少一次写入 + Flink CDC 增量快照"的固有限制（ClickHouse 官方 ClickPipes 同样不支持全量期间的表结构变更），可用 `--verify` 发现，用 `--fresh` 修复：

- **全量阶段发生故障恢复**：某个快照分片已写入、但未被 checkpoint 记录时作业失败，恢复后该分片会重新读取；若这期间 MySQL 删除了其中的行，ClickHouse 中会残留这些行。
- **全量阶段对正在同步的表执行 TRUNCATE 或删除列**：binlog 阶段回放这些 DDL 时，会作用到之后读取的快照数据上，造成数据缺失。请避免在全量同步期间对源表执行 DDL / TRUNCATE。
- **系统时钟回拨**：版本号基于系统时间并随 checkpoint 持久化，若故障恢复时时钟回拨超过停机时长，之后的更新可能被旧版本覆盖。请保证服务器时钟同步。
- **有夏令时的时区**：TIMESTAMP 以 `source.server-time-zone` 的墙上时间写入，夏令时回拨的重叠小时内存在 1 小时歧义（Asia/Shanghai 等无夏令时时区不受影响）。
- 修改列类型在 ClickHouse 中是异步 mutation，若历史数据无法转换为新类型，可在 `system.mutations` 中查看失败原因。

## 工程结构

```
src/main/java/com/github/izerui
├── CdcApplication.java              # 入口：加载配置 → 预建表 → 组装并运行作业
├── config
│   ├── CdcConfig.java               # 配置项
│   └── CheckpointLocator.java       # 查找最近的 checkpoint / savepoint
├── model                            # ChangeEvent / TableDef / ColumnDef
├── source
│   ├── MySqlSourceFactory.java      # 构建 Flink CDC MySqlSource
│   ├── ChangeEventDeserializer.java # Debezium SourceRecord → ChangeEvent
│   ├── DebeziumValueConverter.java  # Debezium 字段值 → 普通 Java 值
│   ├── DebeziumSchemaChange.java    # DDL 事件解析
│   ├── MySqlSchemaReader.java       # information_schema 读取表结构
│   └── TableFilter.java             # 表过滤规则
├── health
│   └── HealthServer.java            # K8s 探针用的健康检查端点
├── verify
│   └── DataVerifier.java            # --verify 逐表对数
├── clickhouse
│   ├── ClickHouseClient.java        # HTTP 客户端（TabSeparated 写入）
│   ├── ClickHouseTypeMapper.java    # 类型映射
│   ├── ClickHouseSql.java           # 建表 / ALTER 语句
│   ├── SchemaSynchronizer.java      # 表结构比对同步
│   ├── TargetTableInitializer.java  # 启动前预建表
│   └── TsvEncoder.java              # TabSeparated 编码
└── sink
    ├── ClickHouseWriter.java        # 按库顺序处理 DDL 与 DML、按表缓冲批量写入、DDL 屏障
    └── PendingEntry.java            # DDL 屏障中暂存的事件
```

数据流：

```
MySqlSource ──keyBy(库)──► ClickHouseWriter
                              ├─ DDL：先写出该库缓冲，暂存到下一个 checkpoint 完成后再同步表结构
                              └─ DML：按表缓冲，满批 / 定时 / checkpoint 时写出
```
