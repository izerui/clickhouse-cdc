package com.github.izerui.sink;

import com.github.izerui.clickhouse.ClickHouseClient;
import com.github.izerui.clickhouse.ClickHouseException;
import com.github.izerui.clickhouse.ClickHouseSql;
import com.github.izerui.clickhouse.ExposedByteArrayOutputStream;
import com.github.izerui.clickhouse.SchemaSynchronizer;
import com.github.izerui.clickhouse.TsvEncoder;
import com.github.izerui.config.CdcConfig;
import com.github.izerui.model.ChangeEvent;
import com.github.izerui.model.TableDef;
import com.github.izerui.source.MySqlSchemaReader;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 按目标表写入 ClickHouse。
 * <p>
 * <ul>
 *     <li>上游按库 keyBy，同一个库的 DDL 与 DML 由同一个实例严格按 binlog 顺序处理（跨表的 DDL 如 RENAME TABLE
 *     涉及两张表，按表分区会落到不同实例而失去先后顺序）；实例内部仍按表分别缓冲、批量写入；</li>
 *     <li>增删改统一写成 INSERT：带单调递增的 {@code _version}，删除写 {@code _is_deleted=1}，
 *     由 ReplacingMergeTree 合并为每个主键的最新状态，重复写入是幂等的；</li>
 *     <li>按表缓冲，达到批次大小或间隔时间后写出；checkpoint 时写出全部缓冲，保证至少一次；</li>
 *     <li>DDL 屏障：DDL 到达后先写出该表缓冲，并暂存 DDL 及该表之后的事件，等下一个 checkpoint 完成后再执行 DDL、
 *     写入暂存事件。这样任何故障恢复都只会回放 DDL 之后的数据，不会出现“目标表结构已变更、又回放变更前的数据”
 *     （否则列改名会被当成删除、类型变更后回放旧值会写入失败）；</li>
 *     <li>只写目标表中存在的列：源表已删除的列在回放的旧数据中出现时忽略该列而不是写入失败。</li>
 *     <li>内存上限：单表缓冲不超过 sink.batch.max-bytes，本实例所有表的缓冲合计不超过
 *     sink.buffer.max-total-bytes / 并行度（超过时全部写出）。写出是同步的，写得慢时 Flink 反压会让上游暂停读取，
 *     积压留在 MySQL binlog 中而不是内存里。</li>
 * </ul>
 */
public class ClickHouseWriter extends KeyedProcessFunction<String, ChangeEvent, Void>
        implements CheckpointedFunction, CheckpointListener {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseWriter.class);
    private static final DateTimeFormatter TS_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    private static final long STATS_INTERVAL_MS = 60_000;
    /** DDL 屏障中检查 checkpoint 是否已完成的间隔 */
    private static final long PENDING_CHECK_MS = 1_000;

    private final CdcConfig config;

    private transient ClickHouseClient client;
    private transient SchemaSynchronizer synchronizer;
    private transient ZoneId timeZone;
    private transient Map<String, TableBuffer> buffers;
    private transient Set<String> warnedNoPrimaryKey;
    private transient Set<String> warnedMissingColumns;
    /** 目标表现有列缓存：key → 列名集合 */
    private transient Map<String, Set<String>> targetColumns;
    private transient ListState<Long> versionState;
    /** DDL 屏障暂存的事件（按表的 keyed state） */
    private transient ListState<PendingEntry> pendingState;
    /** 最近一次执行 snapshotState 的 checkpoint 编号 */
    private transient long lastSnapshotId;
    /** 最近完成的 checkpoint 编号 */
    private transient long lastCompletedId;
    /** 本次作业运行的编号，用于识别从 checkpoint 恢复出来的暂存条目 */
    private transient long attemptId;
    private transient long lastVersion;
    private transient long writtenRows;
    private transient long lastStatsTime;
    /** 本实例所有表缓冲的字节数合计 */
    private transient long bufferedBytes;
    /** 本实例缓冲合计上限 */
    private transient long maxBufferedBytes;

    public ClickHouseWriter(CdcConfig config) {
        this.config = config;
    }

    @Override
    public void open(Configuration parameters) {
        client = new ClickHouseClient(config);
        synchronizer = new SchemaSynchronizer(client, config);
        timeZone = config.getSourceServerTimeZone();
        buffers = new HashMap<>();
        warnedNoPrimaryKey = new HashSet<>();
        warnedMissingColumns = new HashSet<>();
        targetColumns = new HashMap<>();
        attemptId = ThreadLocalRandom.current().nextLong();
        pendingState = getRuntimeContext().getListState(
                new ListStateDescriptor<>("pending-events", TypeInformation.of(PendingEntry.class)));
        lastStatsTime = System.currentTimeMillis();
        // 同一进程内的各写入实例平分缓冲额度
        maxBufferedBytes = config.getSinkBufferMaxTotalBytes() / getRuntimeContext().getNumberOfParallelSubtasks();
    }

    @Override
    public void processElement(ChangeEvent event, Context ctx, Collector<Void> out) throws Exception {
        String database = ctx.getCurrentKey();
        if (pendingState.get().iterator().hasNext()) {
            // 该库有尚未执行的 DDL：后续事件按顺序继续暂存
            pendingState.add(new PendingEntry(event, event.isDdl() ? lastSnapshotId : -1, attemptId));
            return;
        }
        if (event.isDdl()) {
            // 先写出该库所有表已缓冲的数据（RENAME TABLE 等会涉及其他表），再暂存 DDL，等下一个 checkpoint 完成后执行
            flushDatabase(database);
            pendingState.add(new PendingEntry(event, lastSnapshotId, attemptId));
            registerPendingCheck(ctx.timerService());
            LOG.info("收到 DDL [{} {}]，将在下一个 checkpoint 完成后执行: {}", event.ddlType, event.key(), event.ddl);
            return;
        }
        writeDml(event.key(), event, ctx.timerService());
    }

    private void writeDml(String key, ChangeEvent event, TimerService timerService) throws Exception {
        if (event.primaryKeys == null || event.primaryKeys.isEmpty()) {
            if (warnedNoPrimaryKey.add(key)) {
                LOG.warn("表 {} 没有主键，忽略其数据变更", key);
            }
            return;
        }

        TableBuffer buffer = buffers.get(key);
        if (buffer != null && !buffer.columns.equals(event.columns)) {
            // 列发生变化（表结构变更后），先写出旧结构的数据
            flush(key);
            buffer = null;
        }
        if (buffer == null) {
            boolean[] writable = writableColumns(key, event);
            if (writable == null) {
                return;
            }
            buffer = new TableBuffer(event.database, event.table, event.columns, writable);
            buffers.put(key, buffer);
            timerService.registerProcessingTimeTimer(timerService.currentProcessingTime() + config.getSinkBatchIntervalMs());
        }

        long sizeBefore = buffer.data.size();
        switch (event.op) {
            case READ:
                // 全量快照版本固定为 0：增量阶段的任何变更都比快照新
                buffer.write(event.after, 0L, 0, "r", event.sourceTsMs);
                break;
            case CREATE:
                buffer.write(event.after, nextVersion(), 0, "c", event.sourceTsMs);
                break;
            case UPDATE:
                if (event.before != null && primaryKeyChanged(event)) {
                    // 主键被修改：旧主键行标记删除，再写入新主键行
                    buffer.write(event.before, nextVersion(), 1, "d", event.sourceTsMs);
                }
                buffer.write(event.after, nextVersion(), 0, "u", event.sourceTsMs);
                break;
            case DELETE:
                buffer.write(event.before, nextVersion(), 1, "d", event.sourceTsMs);
                break;
            default:
                return;
        }
        bufferedBytes += buffer.data.size() - sizeBefore;
        if (buffer.rows >= config.getSinkBatchSize() || buffer.data.size() >= config.getSinkBatchMaxBytes()) {
            flush(key);
        } else if (bufferedBytes > maxBufferedBytes) {
            // 表很多、每张表都没满批时，合计也不能超过上限
            for (String k : new ArrayList<>(buffers.keySet())) {
                flush(k);
            }
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Void> out) throws Exception {
        String database = ctx.getCurrentKey();
        drainPending(database, ctx.timerService());
        flushDatabase(database);
    }

    /** 写出某个库所有表的缓冲 */
    private void flushDatabase(String database) throws Exception {
        for (String key : new ArrayList<>(buffers.keySet())) {
            TableBuffer buffer = buffers.get(key);
            if (buffer != null && buffer.database.equals(database)) {
                flush(key);
            }
        }
    }

    private void registerPendingCheck(TimerService timerService) {
        timerService.registerProcessingTimeTimer(timerService.currentProcessingTime() + PENDING_CHECK_MS);
    }

    /**
     * 执行已满足条件的暂存 DDL，并写入其后的暂存事件；遇到尚未满足条件的 DDL 则停下，剩余部分继续暂存。
     */
    private void drainPending(String database, TimerService timerService) throws Exception {
        List<PendingEntry> entries = new ArrayList<>();
        pendingState.get().forEach(entries::add);
        if (entries.isEmpty()) {
            return;
        }
        int i = 0;
        for (; i < entries.size(); i++) {
            PendingEntry entry = entries.get(i);
            if (entry.event.isDdl()) {
                if (!entry.isReady(attemptId, lastCompletedId)) {
                    break;
                }
                flushDatabase(database);
                applyDdl(entry.event);
                targetColumns.remove(entry.event.key());
                if (entry.event.previousTable != null) {
                    targetColumns.remove(entry.event.previousDatabase + "." + entry.event.previousTable);
                }
            } else {
                writeDml(entry.event.key(), entry.event, timerService);
            }
        }
        if (i == entries.size()) {
            pendingState.clear();
        } else {
            pendingState.update(new ArrayList<>(entries.subList(i, entries.size())));
            registerPendingCheck(timerService);
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        // checkpoint 前写出全部缓冲：checkpoint 成功即代表之前的数据都已写入 ClickHouse
        for (String key : new ArrayList<>(buffers.keySet())) {
            flush(key);
        }
        versionState.update(List.of(lastVersion));
        lastSnapshotId = Math.max(lastSnapshotId, context.getCheckpointId());
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        // 暂存的 DDL 由各表的定时器检查并执行（此处没有 key 上下文）
        lastCompletedId = Math.max(lastCompletedId, checkpointId);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // 缓冲在 checkpoint 时已全部写出，只需保存已用过的最大版本号
        versionState = context.getOperatorStateStore()
                .getUnionListState(new ListStateDescriptor<>("last-version", Types.LONG));
        for (Long v : versionState.get()) {
            lastVersion = Math.max(lastVersion, v);
        }
        // 后续 checkpoint 编号从恢复点继续；恢复出来的暂存 DDL 通过 attemptId 识别并立即执行
        long restoredId = context.getRestoredCheckpointId().orElse(0L);
        lastSnapshotId = restoredId;
        lastCompletedId = restoredId;
    }

    /**
     * 单调递增的版本号（微秒时间戳）。最大版本号随 checkpoint 保存，即使重启后系统时钟回拨也不会变小。
     */
    private long nextVersion() {
        lastVersion = Math.max(lastVersion + 1, System.currentTimeMillis() * 1000);
        return lastVersion;
    }

    private static boolean primaryKeyChanged(ChangeEvent event) {
        for (String pk : event.primaryKeys) {
            int i = event.columns.indexOf(pk);
            if (i >= 0 && !Objects.deepEquals(event.before[i], event.after[i])) {
                return true;
            }
        }
        return false;
    }

    private void flush(String key) throws Exception {
        TableBuffer buffer = buffers.get(key);
        if (buffer == null || buffer.rows == 0) {
            buffers.remove(key);
            return;
        }
        int maxRetries = config.getSinkMaxRetries();
        boolean schemaResynced = false;
        for (int attempt = 0; ; attempt++) {
            try {
                client.insert(config.getClickHouseDatabase(buffer.database), buffer.table, buffer.quotedColumns(), buffer.data.array(), buffer.data.size());
                break;
            } catch (ClickHouseException e) {
                if (!schemaResynced && isSchemaMismatch(e)) {
                    // 例如启动建表后、全量开始前源表又发生了结构变更：按源表当前结构校正后重试
                    LOG.warn("写入 {} 时结构不一致，按源表结构校正后重试: {}", key, e.getMessage());
                    resyncFromSource(buffer.database, buffer.table, key);
                    targetColumns.remove(key);
                    schemaResynced = true;
                    continue;
                }
                if (attempt >= maxRetries) {
                    throw new IllegalStateException("写入 ClickHouse 失败（已重试 " + maxRetries + " 次）: " + key + ": " + e.getMessage(), e);
                }
                long backoff = Math.min(30_000L, 1000L << attempt);
                LOG.warn("写入 {} 失败，{}ms 后第 {} 次重试: {}", key, backoff, attempt + 1, e.getMessage());
                Thread.sleep(backoff);
            }
        }
        buffers.remove(key);
        bufferedBytes -= buffer.data.size();
        writtenRows += buffer.rows;
        LOG.debug("写入 {} {} 行", key, buffer.rows);
        long now = System.currentTimeMillis();
        if (now - lastStatsTime >= STATS_INTERVAL_MS) {
            LOG.info("[subtask {}] 最近 {} 秒写入 {} 行",
                    getRuntimeContext().getIndexOfThisSubtask(), (now - lastStatsTime) / 1000, writtenRows);
            writtenRows = 0;
            lastStatsTime = now;
        }
    }

    /**
     * 计算事件中哪些列可以写入目标表；目标表不存在且无法从源表创建时返回 null（跳过该事件）。
     */
    private boolean[] writableColumns(String key, ChangeEvent event) throws Exception {
        Set<String> columns = targetColumns.get(key);
        if (columns == null) {
            columns = describeTarget(event, key);
            if (columns == null) {
                return null;
            }
        }
        // 事件中有目标表没有的列：可能是全量阶段源表新加了列（DDL 要到 binlog 阶段才会到达），
        // 先按源表当前结构补列，避免这些值被丢弃；补完仍缺失的才是源表已删除的列
        boolean unknownMissing = false;
        for (String column : event.columns) {
            if (!columns.contains(column) && !warnedMissingColumns.contains(key + "." + column)) {
                unknownMissing = true;
                break;
            }
        }
        if (unknownMissing) {
            resyncFromSource(event.database, event.table, key);
            columns = describeTarget(event, key);
            if (columns == null) {
                return null;
            }
        }
        boolean[] writable = new boolean[event.columns.size()];
        for (int i = 0; i < writable.length; i++) {
            String column = event.columns.get(i);
            writable[i] = columns.contains(column);
            if (!writable[i] && warnedMissingColumns.add(key + "." + column)) {
                LOG.warn("目标表 {} 中没有列 {}，且源表当前也没有该列（已删除），回放的旧数据写入时忽略该列", key, column);
            }
        }
        return writable;
    }

    /** 读取目标表现有列；目标表不存在时尝试按源表结构创建，仍不存在返回 null */
    private Set<String> describeTarget(ChangeEvent event, String key) throws Exception {
        Map<String, String[]> described = synchronizer.describe(event.database, event.table);
        if (described == null) {
            // 目标表不存在（例如被手工删除），尝试按源表结构重建
            resyncFromSource(event.database, event.table, key);
            described = synchronizer.describe(event.database, event.table);
        }
        if (described == null) {
            if (warnedNoPrimaryKey.add(key)) {
                LOG.warn("目标表 {} 不存在且无法创建，忽略其数据变更", key);
            }
            targetColumns.remove(key);
            return null;
        }
        Set<String> columns = described.keySet();
        targetColumns.put(key, columns);
        return columns;
    }

    private static boolean isSchemaMismatch(ClickHouseException e) {
        String msg = e.getMessage();
        return msg != null && (msg.contains("NO_SUCH_COLUMN_IN_TABLE") || msg.contains("UNKNOWN_TABLE")
                || msg.contains("UNKNOWN_IDENTIFIER"));
    }

    private void resyncFromSource(String database, String table, String key) throws Exception {
        TableDef def = readSourceTable(database, table);
        if (def == null) {
            LOG.warn("未在源库中找到 {} 的表结构", key);
            return;
        }
        synchronizer.addMissing(def);
    }

    /** 从 MySQL 读取表当前结构，不存在时返回 null */
    protected TableDef readSourceTable(String database, String table) throws Exception {
        MySqlSchemaReader reader = new MySqlSchemaReader(config);
        try (Connection conn = reader.openConnection()) {
            for (TableDef def : reader.readTables(conn, database)) {
                if (def.table.equals(table)) {
                    return def;
                }
            }
        }
        return null;
    }

    private void applyDdl(ChangeEvent event) {
        String key = event.key();
        LOG.info("源端 DDL [{} {}]: {}", event.ddlType, key, event.ddl);
        try {
            switch (event.ddlType == null ? "" : event.ddlType.toUpperCase()) {
                case "TRUNCATE":
                    if (config.isSinkTruncateTableEnabled()) {
                        synchronizer.truncate(event.database, event.table);
                    }
                    break;
                case "DROP":
                    if (config.isSinkDropTableEnabled()) {
                        synchronizer.drop(event.database, event.table);
                    } else {
                        LOG.info("sink.drop-table-enabled=false，保留目标表 {}", key);
                    }
                    break;
                case "CREATE":
                case "ALTER":
                    if (event.previousTable != null) {
                        renameTable(event);
                    }
                    if (event.tableDef != null) {
                        synchronizer.sync(event.tableDef, event.ddl);
                    }
                    break;
                default:
                    LOG.warn("未处理的表结构变更类型 {}: {}", event.ddlType, event.ddl);
            }
        } catch (RuntimeException e) {
            if (config.isSinkIgnoreDdlErrors()) {
                LOG.error("同步 DDL 失败（已按配置忽略）{}: {}", key, event.ddl, e);
            } else {
                throw new IllegalStateException("同步 DDL 失败 " + key + ": " + event.ddl, e);
            }
        }
    }

    /**
     * RENAME TABLE：把目标端的旧表改名，保留历史数据。旧表此前的数据已在 DDL 屏障等待的 checkpoint 中全部写出。
     */
    private void renameTable(ChangeEvent event) {
        String from = event.previousDatabase + "." + event.previousTable;
        if (from.equals(event.key())) {
            return;
        }
        if (!synchronizer.exists(event.previousDatabase, event.previousTable)) {
            LOG.info("RENAME TABLE {} -> {}：目标端没有原表（未在同步范围内），按新表创建", from, event.key());
        } else if (synchronizer.exists(event.database, event.table)) {
            LOG.warn("RENAME TABLE {} -> {}：目标端已存在同名新表，保留原表 {} 不做改名", from, event.key(), from);
        } else {
            synchronizer.renameTable(event.previousDatabase, event.previousTable, event.database, event.table);
        }
    }

    /**
     * 单张表的写入缓冲（TabSeparated 格式）。
     */
    private final class TableBuffer {
        final String database;
        final String table;
        final List<String> columns;
        /** 与 columns 对应，false 表示目标表没有该列、不写入 */
        final boolean[] writable;
        final ExposedByteArrayOutputStream data = new ExposedByteArrayOutputStream(64 * 1024);
        int rows;

        TableBuffer(String database, String table, List<String> columns, boolean[] writable) {
            this.database = database;
            this.table = table;
            this.columns = columns;
            this.writable = writable;
        }

        List<String> quotedColumns() {
            List<String> quoted = new ArrayList<>(columns.size() + 4);
            for (int i = 0; i < columns.size(); i++) {
                if (writable[i]) {
                    quoted.add(ClickHouseSql.quote(columns.get(i)));
                }
            }
            quoted.add(ClickHouseSql.quote(ClickHouseSql.COL_VERSION));
            quoted.add(ClickHouseSql.quote(ClickHouseSql.COL_IS_DELETED));
            quoted.add(ClickHouseSql.quote(ClickHouseSql.COL_OP));
            quoted.add(ClickHouseSql.quote(ClickHouseSql.COL_SOURCE_TS));
            return quoted;
        }

        void write(Object[] row, long version, int deleted, String op, long sourceTsMs) {
            for (int i = 0; i < row.length; i++) {
                if (writable[i]) {
                    TsvEncoder.writeValue(data, row[i]);
                    TsvEncoder.writeTab(data);
                }
            }
            TsvEncoder.writeValue(data, version);
            TsvEncoder.writeTab(data);
            TsvEncoder.writeValue(data, deleted);
            TsvEncoder.writeTab(data);
            TsvEncoder.writeValue(data, op);
            TsvEncoder.writeTab(data);
            TsvEncoder.writeValue(data, TS_FORMAT.format(Instant.ofEpochMilli(sourceTsMs).atZone(timeZone)));
            TsvEncoder.writeNewLine(data);
            rows++;
        }
    }
}
