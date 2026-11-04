package com.github.izerui.clickhouse;

import com.github.izerui.config.CdcConfig;
import com.github.izerui.model.ColumnDef;
import com.github.izerui.model.TableDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.ZoneId;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 让 ClickHouse 目标表结构与 MySQL 源表保持一致（库名、表名相同）。
 * <p>
 * 采用“结构比对”而不是“翻译 DDL 语句”：拿到源表变更后的完整结构，与目标表现有列比较，
 * 补齐新增列、修改类型变化的列、删除多余列。只有列重命名无法通过比对识别，需要从 DDL 中解析。
 */
public class SchemaSynchronizer implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(SchemaSynchronizer.class);

    private static final Pattern RENAME_COLUMN = Pattern.compile(
            "\\brename\\s+column\\s+`?([^`\\s,]+)`?\\s+to\\s+`?([^`\\s,;]+)`?", Pattern.CASE_INSENSITIVE);
    private static final Pattern CHANGE_COLUMN = Pattern.compile(
            "\\bchange\\s+(?:column\\s+)?`?([^`\\s,]+)`?\\s+`?([^`\\s,]+)`?\\s", Pattern.CASE_INSENSITIVE);

    private final ClickHouseClient client;
    private final ZoneId timeZone;
    private final String partitionBy;
    private final String tableSettings;
    private final boolean dropColumnEnabled;

    private final Set<String> knownDatabases = new HashSet<>();

    public SchemaSynchronizer(ClickHouseClient client, CdcConfig config) {
        this.client = client;
        this.timeZone = config.getSourceServerTimeZone();
        this.partitionBy = config.getClickHousePartitionBy();
        this.tableSettings = config.getClickHouseTableSettings();
        this.dropColumnEnabled = config.isSinkDropColumnEnabled();
    }

    /**
     * 目标表现有列：列名 → [类型, 注释]，表不存在时返回 null。
     */
    public Map<String, String[]> describe(String database, String table) {
        List<String[]> rows = client.query("SELECT name, type, comment FROM system.columns WHERE database = "
                + ClickHouseSql.literal(database) + " AND table = " + ClickHouseSql.literal(table) + " ORDER BY position");
        if (rows.isEmpty()) {
            return null;
        }
        Map<String, String[]> columns = new LinkedHashMap<>();
        for (String[] row : rows) {
            columns.put(row[0], new String[]{row[1], row[2]});
        }
        return columns;
    }

    public void ensureDatabase(String database) {
        if (knownDatabases.add(database)) {
            client.execute(ClickHouseSql.createDatabase(database));
        }
    }

    /**
     * 同步表结构：目标表不存在则创建，存在则按差异调整。
     *
     * @param ddl 触发本次同步的源端 DDL，用于识别列重命名；启动时为 null
     */
    public void sync(TableDef source, String ddl) {
        String database = source.database;
        String table = source.table;
        if (!source.hasPrimaryKey()) {
            LOG.warn("表 {} 没有主键，跳过同步", source.fullName());
            return;
        }
        ensureDatabase(database);
        Map<String, String[]> existing = describe(database, table);
        if (existing == null) {
            String sql = ClickHouseSql.createTable(source, timeZone, partitionBy, tableSettings);
            LOG.info("创建表 {}.{}:\n{}", database, table, sql);
            client.execute(sql);
            return;
        }

        warnIfPrimaryKeyChanged(source);

        if (ddl != null) {
            for (Map.Entry<String, String> rename : parseRenames(ddl).entrySet()) {
                String from = rename.getKey();
                String to = rename.getValue();
                if (existing.containsKey(from) && !existing.containsKey(to) && source.getColumn(to) != null) {
                    execute(ClickHouseSql.renameColumn(database, table, from, to));
                    existing = describe(database, table);
                }
            }
        }

        String previous = null;
        for (ColumnDef column : source.columns) {
            boolean pk = source.primaryKeys.contains(column.name);
            String expectedType = ClickHouseTypeMapper.toClickHouseType(column, pk, timeZone);
            String expectedComment = column.comment == null ? "" : column.comment;
            String[] current = existing.get(column.name);
            if (current != null && !pk && ClickHouseTypeMapper.isNullable(current[0]) && !ClickHouseTypeMapper.isNullable(expectedType)) {
                // 不把 Nullable 收紧为非空：目标表中历史版本行可能仍有 NULL，收紧会导致 mutation 失败
                column = copyAsNullable(column);
                expectedType = ClickHouseTypeMapper.toClickHouseType(column, false, timeZone);
            }
            if (current == null) {
                execute(ClickHouseSql.addColumn(database, table, column, pk, timeZone, previous));
            } else if (!expectedType.equals(current[0])) {
                if (pk) {
                    LOG.warn("主键列 {}.{}.{} 类型变化 {} -> {}，ClickHouse 排序键列不支持自动修改，请人工处理",
                            database, table, column.name, current[0], expectedType);
                } else {
                    execute(ClickHouseSql.modifyColumn(database, table, column, false, timeZone));
                }
            } else if (!expectedComment.equals(Objects.toString(current[1], ""))) {
                execute(ClickHouseSql.commentColumn(database, table, column.name, expectedComment));
            }
            previous = column.name;
        }

        for (String name : existing.keySet()) {
            if (ClickHouseSql.META_COLUMNS.contains(name) || source.getColumn(name) != null) {
                continue;
            }
            if (dropColumnEnabled) {
                execute(ClickHouseSql.dropColumn(database, table, name));
            } else {
                LOG.info("源表已无列 {}，sink.drop-column-enabled=false，保留目标列 {}.{}.{}", name, database, table, name);
            }
        }
    }

    /**
     * 只补齐目标表缺失的列（表不存在则创建），不删除、不修改、不重命名任何列。
     * <p>
     * 用于没有 DDL 上下文的场景（如全量阶段源表新增了列、目标表被手工删除）：此时无法区分“改名”与“删除 + 新增”，
     * 做删除或修改可能误删历史数据，只补列是安全的。
     */
    public void addMissing(TableDef source) {
        if (!source.hasPrimaryKey()) {
            LOG.warn("表 {} 没有主键，跳过同步", source.fullName());
            return;
        }
        ensureDatabase(source.database);
        Map<String, String[]> existing = describe(source.database, source.table);
        if (existing == null) {
            String sql = ClickHouseSql.createTable(source, timeZone, partitionBy, tableSettings);
            LOG.info("创建表 {}:\n{}", source.fullName(), sql);
            client.execute(sql);
            return;
        }
        String previous = null;
        for (ColumnDef column : source.columns) {
            if (!existing.containsKey(column.name)) {
                execute(ClickHouseSql.addColumn(source.database, source.table, column,
                        source.primaryKeys.contains(column.name), timeZone, previous));
            }
            previous = column.name;
        }
    }

    public boolean exists(String database, String table) {
        List<String[]> rows = client.query("SELECT count() FROM system.tables WHERE database = "
                + ClickHouseSql.literal(database) + " AND name = " + ClickHouseSql.literal(table));
        return !rows.isEmpty() && !"0".equals(rows.get(0)[0]);
    }

    public void renameTable(String fromDatabase, String fromTable, String toDatabase, String toTable) {
        ensureDatabase(toDatabase);
        execute(ClickHouseSql.renameTable(fromDatabase, fromTable, toDatabase, toTable));
    }

    /**
     * ClickHouse 不支持修改排序键，主键定义变化时只能提示人工处理。
     */
    private void warnIfPrimaryKeyChanged(TableDef source) {
        List<String[]> rows = client.query("SELECT sorting_key FROM system.tables WHERE database = "
                + ClickHouseSql.literal(source.database) + " AND name = " + ClickHouseSql.literal(source.table));
        if (rows.isEmpty() || rows.get(0)[0] == null) {
            return;
        }
        String current = rows.get(0)[0].replace("`", "").replace(" ", "");
        String expected = String.join(",", source.primaryKeys);
        if (!current.equals(expected)) {
            LOG.warn("表 {}.{} 主键已变化：目标排序键 ({})，源表主键 ({})。ClickHouse 无法修改排序键，"
                            + "请删除目标表后使用 --fresh 重新全量同步，否则数据去重会按旧主键进行",
                    source.database, source.table, current, expected);
        }
    }

    public void truncate(String database, String table) {
        execute(ClickHouseSql.truncateTable(database, table));
    }

    public void drop(String database, String table) {
        execute(ClickHouseSql.dropTable(database, table));
    }

    private static ColumnDef copyAsNullable(ColumnDef c) {
        ColumnDef copy = new ColumnDef(c.name, c.dataType, c.unsigned, c.length, c.scale, true, c.comment);
        copy.defaultValue = c.defaultValue;
        return copy;
    }

    private void execute(String sql) {
        LOG.info("执行: {}", sql);
        client.execute(sql);
    }

    /**
     * 从 DDL 中解析列重命名：旧列名 → 新列名。
     */
    static Map<String, String> parseRenames(String ddl) {
        Map<String, String> renames = new LinkedHashMap<>();
        Matcher m = RENAME_COLUMN.matcher(ddl);
        while (m.find()) {
            renames.put(m.group(1), m.group(2));
        }
        m = CHANGE_COLUMN.matcher(ddl);
        while (m.find()) {
            if (!m.group(1).equals(m.group(2))) {
                renames.put(m.group(1), m.group(2));
            }
        }
        return renames;
    }
}
