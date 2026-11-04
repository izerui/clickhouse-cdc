package com.github.izerui.clickhouse;

import com.github.izerui.model.ColumnDef;
import com.github.izerui.model.TableDef;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * ClickHouse SQL 生成。
 */
public final class ClickHouseSql {

    /** 版本号：ReplacingMergeTree 保留版本最大的行 */
    public static final String COL_VERSION = "_version";
    /** 删除标记：1 表示该主键已删除 */
    public static final String COL_IS_DELETED = "_is_deleted";
    /** 源端操作类型 r/c/u/d */
    public static final String COL_OP = "_op";
    /** 源端事件时间 */
    public static final String COL_SOURCE_TS = "_source_ts";
    /** 写入 ClickHouse 的时间 */
    public static final String COL_SYNCED_AT = "_synced_at";

    private static final Pattern NUMBER = Pattern.compile("[+-]?\\d+(\\.\\d+)?([eE][+-]?\\d+)?");
    private static final Pattern DATE = Pattern.compile("\\d{4}-\\d{2}-\\d{2}");
    private static final Pattern DATETIME = Pattern.compile("\\d{4}-\\d{2}-\\d{2}[ T]\\d{2}:\\d{2}:\\d{2}(\\.\\d{1,6})?");

    public static final List<String> META_COLUMNS = List.of(COL_VERSION, COL_IS_DELETED, COL_OP, COL_SOURCE_TS, COL_SYNCED_AT);

    private ClickHouseSql() {
    }

    /** 标识符转义 */
    public static String quote(String identifier) {
        return "`" + identifier.replace("\\", "\\\\").replace("`", "\\`") + "`";
    }

    /** 字符串字面量 */
    public static String literal(String value) {
        return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'";
    }

    public static String table(String database, String table) {
        return quote(database) + "." + quote(table);
    }

    public static String createDatabase(String database) {
        return "CREATE DATABASE IF NOT EXISTS " + quote(database);
    }

    public static String createTable(TableDef def, ZoneId timeZone, String partitionBy, String settings) {
        List<String> lines = new ArrayList<>();
        for (ColumnDef c : def.columns) {
            lines.add("    " + columnDefinition(c, def.primaryKeys.contains(c.name), timeZone));
        }
        String tz = literal(timeZone.getId());
        lines.add("    " + quote(COL_VERSION) + " UInt64");
        lines.add("    " + quote(COL_IS_DELETED) + " UInt8");
        lines.add("    " + quote(COL_OP) + " LowCardinality(String)");
        lines.add("    " + quote(COL_SOURCE_TS) + " DateTime64(3, " + tz + ")");
        lines.add("    " + quote(COL_SYNCED_AT) + " DateTime64(3, " + tz + ") DEFAULT now64(3)");

        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE IF NOT EXISTS ").append(table(def.database, def.table)).append("\n(\n")
                .append(String.join(",\n", lines)).append("\n)\n")
                .append("ENGINE = ReplacingMergeTree(").append(quote(COL_VERSION)).append(", ").append(quote(COL_IS_DELETED)).append(")\n");
        if (partitionBy != null && !partitionBy.isBlank()) {
            sql.append("PARTITION BY ").append(partitionBy).append("\n");
        }
        sql.append("ORDER BY (").append(def.primaryKeys.stream().map(ClickHouseSql::quote).collect(Collectors.joining(", "))).append(")");
        if (settings != null && !settings.isBlank()) {
            sql.append("\nSETTINGS ").append(settings);
        }
        if (def.comment != null && !def.comment.isBlank()) {
            sql.append("\nCOMMENT ").append(literal(def.comment));
        }
        return sql.toString();
    }

    public static String columnDefinition(ColumnDef c, boolean primaryKey, ZoneId timeZone) {
        String sql = quote(c.name) + " " + ClickHouseTypeMapper.toClickHouseType(c, primaryKey, timeZone);
        if (c.comment != null && !c.comment.isBlank()) {
            sql += " COMMENT " + literal(c.comment);
        }
        return sql;
    }

    /**
     * 加列时带上默认值，让已存在的行与 MySQL 一样取到默认值（与 ClickHouse 官方 MySQL CDC 的行为一致）。
     *
     * @param after 新列插入到该列之后；为 null 时插入到最前面
     */
    public static String addColumn(String database, String table, ColumnDef c, boolean primaryKey, ZoneId timeZone, String after) {
        String type = ClickHouseTypeMapper.toClickHouseType(c, primaryKey, timeZone);
        String defaultLiteral = defaultLiteral(c);
        String sql = "ALTER TABLE " + table(database, table) + " ADD COLUMN IF NOT EXISTS " + quote(c.name) + " " + type;
        if (defaultLiteral != null) {
            sql += " DEFAULT " + defaultLiteral;
        }
        if (c.comment != null && !c.comment.isBlank()) {
            sql += " COMMENT " + literal(c.comment);
        }
        return sql + (after == null ? " FIRST" : " AFTER " + quote(after));
    }

    /**
     * 把 MySQL 默认值翻译为 ClickHouse 字面量；无法安全翻译时返回 null（不设置默认值）。
     */
    static String defaultLiteral(ColumnDef c) {
        String v = c.defaultValue;
        if (v == null || c.dataType == null) {
            return null;
        }
        switch (c.dataType.toLowerCase()) {
            case "tinyint":
            case "int1":
            case "bool":
            case "boolean":
                if (v.equalsIgnoreCase("true")) {
                    return "1";
                }
                if (v.equalsIgnoreCase("false")) {
                    return "0";
                }
                return NUMBER.matcher(v).matches() ? v : null;
            case "smallint":
            case "int2":
            case "mediumint":
            case "middleint":
            case "int3":
            case "int":
            case "integer":
            case "int4":
            case "bigint":
            case "int8":
            case "float":
            case "float4":
            case "double":
            case "float8":
            case "real":
            case "decimal":
            case "numeric":
            case "dec":
            case "fixed":
            case "year":
                return NUMBER.matcher(v).matches() ? v : null;
            case "date":
                return DATE.matcher(v).matches() && !v.startsWith("0000") ? literal(v) : null;
            case "datetime":
            case "timestamp":
                // Debezium 把 CURRENT_TIMESTAMP 记为 1970-01-01 00:00:00，其真实含义（执行 ALTER 的时刻）无法还原，不传播
                return DATETIME.matcher(v).matches() && !v.startsWith("0000") && !v.startsWith("1970-01-01 00:00:00")
                        ? literal(v.replace('T', ' ')) : null;
            case "char":
            case "varchar":
            case "tinytext":
            case "text":
            case "mediumtext":
            case "longtext":
            case "enum":
            case "set":
            case "time":
                return literal(v);
            default:
                // bit / binary / blob / json / 空间类型等不传播默认值
                return null;
        }
    }

    public static String modifyColumn(String database, String table, ColumnDef c, boolean primaryKey, ZoneId timeZone) {
        return "ALTER TABLE " + table(database, table) + " MODIFY COLUMN " + columnDefinition(c, primaryKey, timeZone);
    }

    public static String commentColumn(String database, String table, String column, String comment) {
        return "ALTER TABLE " + table(database, table) + " COMMENT COLUMN " + quote(column) + " " + literal(comment == null ? "" : comment);
    }

    public static String dropColumn(String database, String table, String column) {
        return "ALTER TABLE " + table(database, table) + " DROP COLUMN IF EXISTS " + quote(column);
    }

    public static String renameColumn(String database, String table, String from, String to) {
        return "ALTER TABLE " + table(database, table) + " RENAME COLUMN IF EXISTS " + quote(from) + " TO " + quote(to);
    }

    public static String renameTable(String fromDatabase, String fromTable, String toDatabase, String toTable) {
        return "RENAME TABLE " + table(fromDatabase, fromTable) + " TO " + table(toDatabase, toTable);
    }

    public static String truncateTable(String database, String table) {
        return "TRUNCATE TABLE IF EXISTS " + table(database, table);
    }

    public static String dropTable(String database, String table) {
        return "DROP TABLE IF EXISTS " + table(database, table);
    }
}
