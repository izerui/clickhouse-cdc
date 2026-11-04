package com.github.izerui.clickhouse;

import com.github.izerui.model.ColumnDef;

import java.time.ZoneId;

/**
 * MySQL 列类型到 ClickHouse 列类型的标准映射。
 * <p>
 * 生成的类型字符串与 ClickHouse system.columns.type 的格式一致，便于比较结构差异。
 */
public final class ClickHouseTypeMapper {

    private ClickHouseTypeMapper() {
    }

    /**
     * @param primaryKey 主键列不使用 Nullable（ORDER BY 列不允许为 Nullable）
     */
    public static String toClickHouseType(ColumnDef column, boolean primaryKey, ZoneId timeZone) {
        String base = baseType(column, timeZone);
        boolean nullable = column.nullable && !primaryKey;
        if (base.startsWith("LowCardinality(")) {
            // ClickHouse 要求 Nullable 位于 LowCardinality 内部
            String inner = base.substring("LowCardinality(".length(), base.length() - 1);
            return nullable ? "LowCardinality(Nullable(" + inner + "))" : base;
        }
        return nullable ? "Nullable(" + base + ")" : base;
    }

    /** 类型是否可为 NULL（含 LowCardinality(Nullable(...))） */
    public static boolean isNullable(String clickHouseType) {
        return clickHouseType.startsWith("Nullable(") || clickHouseType.startsWith("LowCardinality(Nullable(");
    }

    static String baseType(ColumnDef c, ZoneId timeZone) {
        String type = c.dataType == null ? "" : c.dataType.toLowerCase();
        switch (type) {
            case "tinyint":
            case "int1":
            case "bool":
            case "boolean":
                return c.unsigned ? "UInt8" : "Int8";
            case "smallint":
            case "int2":
                return c.unsigned ? "UInt16" : "Int16";
            case "mediumint":
            case "middleint":
            case "int3":
            case "int":
            case "integer":
            case "int4":
                return c.unsigned ? "UInt32" : "Int32";
            case "bigint":
            case "int8":
                return c.unsigned ? "UInt64" : "Int64";
            case "serial":
                return "UInt64";
            case "float":
            case "float4":
                // MySQL 中 FLOAT(p) 在 p > 24 时实际为 DOUBLE
                return c.length != null && c.length > 24 && c.scale == null ? "Float64" : "Float32";
            case "double":
            case "float8":
            case "real":
                return "Float64";
            case "decimal":
            case "numeric":
            case "dec":
            case "fixed": {
                int precision = c.length == null || c.length <= 0 ? 10 : c.length;
                int scale = c.scale == null ? 0 : Math.min(c.scale, precision);
                return "Decimal(" + precision + ", " + scale + ")";
            }
            case "bit":
                return c.length == null || c.length <= 1 ? "Bool" : "UInt64";
            case "year":
                return "UInt16";
            case "date":
                return "Date32";
            case "datetime":
            case "timestamp": {
                int precision = c.length == null ? 0 : Math.max(0, Math.min(6, c.length));
                return "DateTime64(" + precision + ", '" + timeZone.getId() + "')";
            }
            case "enum":
                // 与 ClickHouse 官方 MySQL CDC（ClickPipes）一致
                return "LowCardinality(String)";
            default:
                // char/varchar/text/blob/binary/json/set/time/geometry 等统一使用 String
                return "String";
        }
    }
}
