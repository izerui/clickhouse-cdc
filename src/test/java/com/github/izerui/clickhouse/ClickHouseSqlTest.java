package com.github.izerui.clickhouse;

import com.github.izerui.model.ColumnDef;
import com.github.izerui.model.TableDef;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ClickHouseSqlTest {

    private static final ZoneId TZ = ZoneId.of("Asia/Shanghai");

    private static String type(String dataType, boolean unsigned, Integer length, Integer scale, boolean nullable) {
        return ClickHouseTypeMapper.toClickHouseType(new ColumnDef("c", dataType, unsigned, length, scale, nullable, null), false, TZ);
    }

    @Test
    void typeMapping() {
        assertEquals("Int8", type("tinyint", false, null, null, false));
        assertEquals("UInt8", type("tinyint", true, null, null, false));
        assertEquals("Int32", type("mediumint", false, null, null, false));
        assertEquals("UInt64", type("bigint", true, null, null, false));
        assertEquals("Float32", type("float", false, null, null, false));
        assertEquals("Float64", type("double", false, null, null, false));
        assertEquals("Float64", type("float", false, 30, null, false));
        assertEquals("Float32", type("float", false, 10, 2, false));
        assertEquals("Int32", type("int4", false, null, null, false));
        assertEquals("UInt64", type("int8", true, null, null, false));
        assertEquals("Float64", type("float8", false, null, null, false));
        assertEquals("Decimal(18, 4)", type("decimal", false, 18, 4, false));
        assertEquals("Decimal(10, 0)", type("decimal", false, null, null, false));
        assertEquals("Bool", type("bit", false, 1, null, false));
        assertEquals("UInt64", type("bit", false, 8, null, false));
        assertEquals("Date32", type("date", false, null, null, false));
        assertEquals("DateTime64(0, 'Asia/Shanghai')", type("datetime", false, 0, null, false));
        assertEquals("Nullable(DateTime64(6, 'Asia/Shanghai'))", type("timestamp", false, 6, null, true));
        assertEquals("String", type("time", false, 3, null, false));
        assertEquals("Nullable(String)", type("json", false, null, null, true));
        assertEquals("String", type("longblob", false, null, null, false));
        assertEquals("UInt16", type("year", false, null, null, false));
        assertEquals("LowCardinality(String)", type("enum", false, null, null, false));
        assertEquals("LowCardinality(Nullable(String))", type("enum", false, null, null, true));
        assertEquals(true, ClickHouseTypeMapper.isNullable("LowCardinality(Nullable(String))"));
        assertEquals(false, ClickHouseTypeMapper.isNullable("LowCardinality(String)"));
    }

    @Test
    void primaryKeyIsNeverNullable() {
        ColumnDef id = new ColumnDef("id", "varchar", false, 32, null, true, null);
        assertEquals("String", ClickHouseTypeMapper.toClickHouseType(id, true, TZ));
    }

    @Test
    void createTable() {
        TableDef t = new TableDef("shop", "order");
        t.comment = "订单";
        t.columns.add(new ColumnDef("id", "bigint", true, null, null, false, "主键"));
        t.columns.add(new ColumnDef("tenant", "varchar", false, 32, null, false, null));
        t.columns.add(new ColumnDef("amount", "decimal", false, 12, 2, true, "it's"));
        t.primaryKeys.add("tenant");
        t.primaryKeys.add("id");
        String expected = "CREATE TABLE IF NOT EXISTS `shop`.`order`\n(\n"
                + "    `id` UInt64 COMMENT '主键',\n"
                + "    `tenant` String,\n"
                + "    `amount` Nullable(Decimal(12, 2)) COMMENT 'it\\'s',\n"
                + "    `_version` UInt64,\n"
                + "    `_is_deleted` UInt8,\n"
                + "    `_op` LowCardinality(String),\n"
                + "    `_source_ts` DateTime64(3, 'Asia/Shanghai'),\n"
                + "    `_synced_at` DateTime64(3, 'Asia/Shanghai') DEFAULT now64(3)\n"
                + ")\n"
                + "ENGINE = ReplacingMergeTree(`_version`, `_is_deleted`)\n"
                + "PARTITION BY tenant\n"
                + "ORDER BY (`tenant`, `id`)\n"
                + "SETTINGS index_granularity = 8192\n"
                + "COMMENT '订单'";
        assertEquals(expected, ClickHouseSql.createTable(t, TZ, "tenant", "index_granularity = 8192"));
    }

    private static ColumnDef withDefault(String dataType, String defaultValue, boolean nullable) {
        ColumnDef c = new ColumnDef("c", dataType, false, null, null, nullable, null);
        c.defaultValue = defaultValue;
        return c;
    }

    @Test
    void addColumnPropagatesSafeDefaults() {
        assertEquals("ALTER TABLE `db`.`t` ADD COLUMN IF NOT EXISTS `c` String DEFAULT 'it\\'s' COMMENT '备注' AFTER `id`",
                ClickHouseSql.addColumn("db", "t", new ColumnDef("c", "varchar", false, 20, null, false, "备注") {{
                    defaultValue = "it's";
                }}, false, TZ, "id"));
        assertEquals("ALTER TABLE `db`.`t` ADD COLUMN IF NOT EXISTS `c` Nullable(Int32) FIRST",
                ClickHouseSql.addColumn("db", "t", withDefault("int", null, true), false, TZ, null));

        assertEquals("-1", ClickHouseSql.defaultLiteral(withDefault("int", "-1", false)));
        assertEquals("12.50", ClickHouseSql.defaultLiteral(withDefault("decimal", "12.50", false)));
        assertEquals("1", ClickHouseSql.defaultLiteral(withDefault("tinyint", "true", false)));
        assertEquals("'a'", ClickHouseSql.defaultLiteral(withDefault("enum", "a", false)));
        assertEquals("'2024-01-01'", ClickHouseSql.defaultLiteral(withDefault("date", "2024-01-01", false)));
        assertEquals("'2024-01-01 08:00:00'", ClickHouseSql.defaultLiteral(withDefault("datetime", "2024-01-01 08:00:00", false)));
        // Debezium 把 CURRENT_TIMESTAMP 记为 1970-01-01 00:00:00，不传播
        assertEquals(null, ClickHouseSql.defaultLiteral(withDefault("datetime", "1970-01-01 00:00:00", false)));
        assertEquals(null, ClickHouseSql.defaultLiteral(withDefault("date", "0000-00-00", false)));
        assertEquals(null, ClickHouseSql.defaultLiteral(withDefault("int", "abc", false)));
        assertEquals(null, ClickHouseSql.defaultLiteral(withDefault("blob", "x", false)));
        assertEquals(null, ClickHouseSql.defaultLiteral(withDefault("bit", "1", false)));
    }

    @Test
    void quoteIdentifier() {
        assertEquals("`a\\`b`", ClickHouseSql.quote("a`b"));
    }

    @Test
    void parseRenames() {
        assertEquals(Map.of("old_name", "new_name"),
                SchemaSynchronizer.parseRenames("ALTER TABLE t RENAME COLUMN `old_name` TO `new_name`"));
        assertEquals(Map.of("a", "b"),
                SchemaSynchronizer.parseRenames("alter table t change column a b varchar(20) not null"));
        assertEquals(Map.of(),
                SchemaSynchronizer.parseRenames("alter table t change `a` `a` bigint"));
    }
}
