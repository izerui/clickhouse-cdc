package com.github.izerui.source;

import com.github.izerui.config.CdcConfig;
import com.github.izerui.model.ChangeEvent;
import com.github.izerui.model.ColumnDef;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ChangeEventDeserializerTest {

    private static final Schema ROW = SchemaBuilder.struct().name("demo.t_user.Value").optional()
            .field("id", Schema.INT64_SCHEMA)
            .field("name", Schema.OPTIONAL_STRING_SCHEMA)
            .field("updated_at", SchemaBuilder.string().name("io.debezium.time.ZonedTimestamp").optional().build())
            .build();
    private static final Schema KEY = SchemaBuilder.struct().name("demo.t_user.Key").field("id", Schema.INT64_SCHEMA).build();
    private static final Schema SOURCE = SchemaBuilder.struct().name("io.debezium.connector.mysql.Source")
            .field("db", Schema.STRING_SCHEMA)
            .field("table", Schema.OPTIONAL_STRING_SCHEMA)
            .field("ts_ms", Schema.INT64_SCHEMA)
            .build();
    private static final Schema ENVELOPE = SchemaBuilder.struct().name("demo.t_user.Envelope")
            .field("before", ROW).field("after", ROW).field("source", SOURCE).field("op", Schema.STRING_SCHEMA)
            .build();
    private static final Schema SCHEMA_CHANGE = SchemaBuilder.struct().name("io.debezium.connector.mysql.SchemaChangeValue")
            .field("source", SOURCE).field("historyRecord", Schema.STRING_SCHEMA)
            .build();

    private final CdcConfig config = new CdcConfig(Map.of(
            "source.hostname", "localhost",
            "source.username", "root",
            "source.databases", "demo",
            "source.server-time-zone", "Asia/Shanghai",
            "clickhouse.url", "http://localhost:8123"));

    private final ChangeEventDeserializer deserializer =
            new ChangeEventDeserializer(config, new TableFilter(".*", "ignored_.*", List.of()));

    private static Struct row(long id, String name, String updatedAt) {
        return new Struct(ROW).put("id", id).put("name", name).put("updated_at", updatedAt);
    }

    private static SourceRecord dml(String table, String op, Struct before, Struct after) {
        Struct source = new Struct(SOURCE).put("db", "demo").put("table", table).put("ts_ms", 1714557600000L);
        Struct value = new Struct(ENVELOPE).put("source", source).put("op", op);
        if (before != null) value.put("before", before);
        if (after != null) value.put("after", after);
        Struct key = new Struct(KEY).put("id", after != null ? after.get("id") : before.get("id"));
        return new SourceRecord(Map.of(), Map.of(), "topic", null, KEY, key, ENVELOPE, value);
    }

    private List<ChangeEvent> deserialize(SourceRecord record) throws Exception {
        List<ChangeEvent> out = new ArrayList<>();
        deserializer.deserialize(record, new Collector<>() {
            @Override
            public void collect(ChangeEvent record) {
                out.add(record);
            }

            @Override
            public void close() {
            }
        });
        return out;
    }

    @Test
    void update() throws Exception {
        List<ChangeEvent> events = deserialize(dml("t_user", "u",
                row(1, "a", "2024-05-01T00:00:00Z"), row(1, "b\tc", "2024-05-01T10:00:00Z")));
        assertEquals(1, events.size());
        ChangeEvent e = events.get(0);
        assertEquals(ChangeEvent.Op.UPDATE, e.op);
        assertEquals("demo.t_user", e.key());
        assertEquals(List.of("id", "name", "updated_at"), e.columns);
        assertEquals(List.of("id"), e.primaryKeys);
        assertArrayEquals(new Object[]{1L, "a", "2024-05-01 08:00:00"}, e.before);
        assertArrayEquals(new Object[]{1L, "b\tc", "2024-05-01 18:00:00"}, e.after);
        assertEquals(1714557600000L, e.sourceTsMs);
    }

    @Test
    void snapshotReadAndDelete() throws Exception {
        ChangeEvent read = deserialize(dml("t_user", "r", null, row(2, null, null))).get(0);
        assertEquals(ChangeEvent.Op.READ, read.op);
        assertEquals(0L, read.sourceTsMs);
        assertNull(read.before);

        ChangeEvent delete = deserialize(dml("t_user", "d", row(2, "x", null), null)).get(0);
        assertEquals(ChangeEvent.Op.DELETE, delete.op);
        assertNull(delete.after);
        assertArrayEquals(new Object[]{2L, "x", null}, delete.before);
    }

    @Test
    void filteredTableIsDropped() throws Exception {
        assertTrue(deserialize(dml("ignored_log", "c", null, row(1, "a", null))).isEmpty());
    }

    private SourceRecord schemaChange(String historyRecord) {
        Struct source = new Struct(SOURCE).put("db", "demo").put("ts_ms", 0L);
        Struct value = new Struct(SCHEMA_CHANGE).put("source", source).put("historyRecord", historyRecord);
        return new SourceRecord(Map.of(), Map.of(), "topic", null, null, null, SCHEMA_CHANGE, value);
    }

    @Test
    void alterTable() throws Exception {
        String history = "{\"source\":{\"server\":\"mysql_binlog_source\"},\"position\":{},"
                + "\"databaseName\":\"demo\",\"ddl\":\"ALTER TABLE t_user ADD COLUMN age INT UNSIGNED NULL COMMENT '年龄'\","
                + "\"tableChanges\":[{\"type\":\"ALTER\",\"id\":\"\\\"demo\\\".\\\"t_user\\\"\",\"comment\":\"用户\",\"table\":{"
                + "\"defaultCharsetName\":\"utf8mb4\",\"primaryKeyColumnNames\":[\"id\"],\"columns\":["
                + "{\"name\":\"id\",\"jdbcType\":-5,\"typeName\":\"BIGINT\",\"typeExpression\":\"BIGINT\",\"length\":20,\"position\":1,\"optional\":false},"
                + "{\"name\":\"price\",\"jdbcType\":3,\"typeName\":\"DECIMAL\",\"typeExpression\":\"DECIMAL\",\"length\":12,\"scale\":2,\"position\":3,\"optional\":true},"
                + "{\"name\":\"age\",\"jdbcType\":4,\"typeName\":\"INT UNSIGNED\",\"typeExpression\":\"INT UNSIGNED\",\"position\":4,\"optional\":true,\"comment\":\"年龄\",\"hasDefaultValue\":true,\"defaultValueExpression\":\"18\"},"
                + "{\"name\":\"ratio\",\"jdbcType\":6,\"typeName\":\"FLOAT\",\"typeExpression\":\"FLOAT\",\"length\":30,\"position\":5,\"optional\":true},"
                + "{\"name\":\"created_at\",\"jdbcType\":93,\"typeName\":\"DATETIME\",\"typeExpression\":\"DATETIME\",\"length\":3,\"position\":2,\"optional\":false}"
                + "]}}]}";
        List<ChangeEvent> events = deserialize(schemaChange(history));
        assertEquals(1, events.size());
        ChangeEvent e = events.get(0);
        assertEquals(ChangeEvent.Op.DDL, e.op);
        assertEquals("ALTER", e.ddlType);
        assertEquals("demo.t_user", e.key());
        assertEquals("用户", e.tableDef.comment);
        assertEquals(List.of("id"), e.tableDef.primaryKeys);
        List<ColumnDef> cols = e.tableDef.columns;
        assertEquals(List.of("id", "created_at", "price", "age", "ratio"), cols.stream().map(c -> c.name).collect(java.util.stream.Collectors.toList()));
        assertEquals(new ColumnDef("id", "bigint", false, null, null, false, null), cols.get(0));
        assertEquals(new ColumnDef("created_at", "datetime", false, 3, null, false, null), cols.get(1));
        assertEquals(new ColumnDef("price", "decimal", false, 12, 2, true, null), cols.get(2));
        ColumnDef age = new ColumnDef("age", "int", true, null, null, true, "年龄");
        age.defaultValue = "18";
        assertEquals(age, cols.get(3));
        assertEquals(new ColumnDef("ratio", "float", false, 30, null, true, null), cols.get(4));
    }

    @Test
    void truncateTable() throws Exception {
        String history = "{\"databaseName\":\"demo\",\"ddl\":\"TRUNCATE TABLE `t_user`\",\"tableChanges\":[]}";
        ChangeEvent e = deserialize(schemaChange(history)).get(0);
        assertEquals("TRUNCATE", e.ddlType);
        assertEquals("demo.t_user", e.key());
        assertNull(e.tableDef);

        String other = "{\"databaseName\":\"demo\",\"ddl\":\"truncate demo.ignored_log\",\"tableChanges\":[]}";
        assertTrue(deserialize(schemaChange(other)).isEmpty());
    }
}
