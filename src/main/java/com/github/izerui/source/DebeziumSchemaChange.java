package com.github.izerui.source;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.izerui.model.ColumnDef;
import com.github.izerui.model.TableDef;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Debezium 表结构变更事件（historyRecord 字段的 JSON）。
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class DebeziumSchemaChange {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private static final Pattern TRUNCATE = Pattern.compile(
            "^\\s*truncate\\s+(?:table\\s+)?(?:`?([^`\\s.]+)`?\\.)?`?([^`\\s;]+)`?", Pattern.CASE_INSENSITIVE);

    public String databaseName;
    public String ddl;
    public List<TableChange> tableChanges = new ArrayList<>();

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class TableChange {
        /** CREATE / ALTER / DROP */
        public String type;
        /** "db"."table" */
        public String id;
        /** RENAME TABLE 时的原表 "db"."table" */
        public String previousId;
        public Table table;
        /** 表注释（与 table 同级） */
        public String comment;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Table {
        public List<String> primaryKeyColumnNames = new ArrayList<>();
        public List<Column> columns = new ArrayList<>();
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Column {
        public String name;
        public String typeName;
        public Integer length;
        public Integer scale;
        public Integer position;
        public Boolean optional;
        public String comment;
        /** 默认值（已去掉引号）；Debezium 会把 CURRENT_TIMESTAMP 记为 1970-01-01 00:00:00 */
        public String defaultValueExpression;
    }

    public static DebeziumSchemaChange parse(String json) throws IOException {
        return MAPPER.readValue(json, DebeziumSchemaChange.class);
    }

    /** 从 "db"."table" 中解析出表名 */
    public static String tableOf(String id) {
        String[] parts = id.replace("`", "\"").split("\"\\.\"");
        String last = parts[parts.length - 1];
        if (parts.length == 1 && last.contains(".")) {
            last = last.substring(last.indexOf('.') + 1);
        }
        return last.replace("\"", "");
    }

    /** 从 "db"."table" 中解析出库名 */
    public static String databaseOf(String id, String defaultDatabase) {
        String[] parts = id.replace("`", "\"").split("\"\\.\"");
        if (parts.length >= 2) {
            return parts[0].replace("\"", "");
        }
        return defaultDatabase;
    }

    /**
     * TRUNCATE 语句没有 tableChanges，从 DDL 中解析 [库名, 表名]，不是 TRUNCATE 时返回 null。
     */
    public String[] parseTruncateTarget() {
        if (ddl == null) {
            return null;
        }
        Matcher m = TRUNCATE.matcher(ddl);
        if (!m.find()) {
            return null;
        }
        return new String[]{m.group(1) != null ? m.group(1) : databaseName, m.group(2)};
    }

    public static TableDef toTableDef(String database, String tableName, Table table, String comment) {
        TableDef def = new TableDef(database, tableName);
        def.comment = comment;
        def.primaryKeys.addAll(table.primaryKeyColumnNames);
        List<Column> columns = new ArrayList<>(table.columns);
        columns.sort(Comparator.comparingInt(c -> c.position == null ? 0 : c.position));
        for (Column c : columns) {
            def.columns.add(toColumnDef(c));
        }
        return def;
    }

    static ColumnDef toColumnDef(Column c) {
        // typeName 形如 "INT UNSIGNED"、"DECIMAL"、"DATETIME"
        String typeName = c.typeName == null ? "varchar" : c.typeName.toLowerCase(Locale.ROOT).trim();
        boolean unsigned = typeName.contains("unsigned");
        String dataType = typeName.split("\\s+")[0];
        Integer length = c.length != null && c.length >= 0 ? c.length : null;
        Integer scale = c.scale != null && c.scale >= 0 ? c.scale : null;
        switch (dataType) {
            case "decimal":
            case "numeric":
            case "dec":
            case "fixed":
            case "float":
            case "bit":
            case "char":
            case "varchar":
            case "binary":
            case "varbinary":
                break;
            case "datetime":
            case "timestamp":
            case "time":
                // 未声明小数位时为 0
                length = length == null ? 0 : length;
                scale = null;
                break;
            default:
                // 整数显示宽度等信息对目标类型无意义
                length = null;
                scale = null;
        }
        boolean decimal = "decimal".equals(dataType) || "numeric".equals(dataType) || "dec".equals(dataType) || "fixed".equals(dataType);
        if (decimal && length != null && scale == null) {
            scale = 0;
        }
        ColumnDef column = new ColumnDef(c.name, dataType, unsigned, length, scale, c.optional == null || c.optional, c.comment);
        column.defaultValue = c.defaultValueExpression;
        return column;
    }
}
