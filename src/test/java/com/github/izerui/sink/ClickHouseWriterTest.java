package com.github.izerui.sink;

import com.github.izerui.config.CdcConfig;
import com.github.izerui.model.ChangeEvent;
import com.github.izerui.model.ColumnDef;
import com.github.izerui.model.TableDef;
import com.sun.net.httpserver.HttpServer;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 使用 Flink 算子测试工具 + 模拟的 ClickHouse HTTP 接口测试写入逻辑。
 */
class ClickHouseWriterTest {

    /** 模拟 MySQL 当前的表结构（库.表 → 结构），供写入算子补列时读取 */
    static final Map<String, TableDef> SOURCE_TABLES = new java.util.concurrent.ConcurrentHashMap<>();

    /** 用 SOURCE_TABLES 代替真实的 MySQL 读取 */
    static class TestWriter extends ClickHouseWriter {
        TestWriter(CdcConfig config) {
            super(config);
        }

        @Override
        protected TableDef readSourceTable(String database, String table) {
            return SOURCE_TABLES.get(database + "." + table);
        }
    }

    private static TableDef sourceTable(String... columns) {
        TableDef def = new TableDef("demo", "t_user");
        for (String c : columns) {
            def.columns.add(new ColumnDef(c, "varchar", false, 20, null, false, null));
        }
        def.primaryKeys.add("id");
        return def;
    }

    /** 模拟 ClickHouse：记录收到的语句和写入数据 */
    private HttpServer server;
    private final List<String> statements = Collections.synchronizedList(new ArrayList<>());
    private final List<String[]> inserts = Collections.synchronizedList(new ArrayList<>());
    /** 目标表现有列：库.表 → 列名 */
    private final Map<String, List<String>> tables = new HashMap<>();
    private final AtomicInteger failNextInserts = new AtomicInteger();

    private CdcConfig config;

    @BeforeEach
    void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            String query = exchange.getRequestURI().getRawQuery();
            byte[] body = readAll(exchange.getRequestBody(), "gzip".equals(exchange.getRequestHeaders().getFirst("Content-Encoding")));
            String response = "";
            int status = 200;
            if (query != null && query.contains("query=")) {
                String sql = URLDecoder.decode(query.substring(query.indexOf("query=") + 6), StandardCharsets.UTF_8);
                if (failNextInserts.getAndDecrement() > 0) {
                    status = 500;
                    response = "Code: 241. DB::Exception: Memory limit exceeded. (MEMORY_LIMIT_EXCEEDED)";
                } else {
                    statements.add(sql);
                    inserts.add(new String[]{sql, new String(body, StandardCharsets.UTF_8)});
                }
            } else {
                String sql = new String(body, StandardCharsets.UTF_8);
                if (sql.startsWith("SELECT name, type, comment FROM system.columns")) {
                    String key = sql.replaceAll(".*database = '([^']+)' AND table = '([^']+)'.*", "$1.$2");
                    StringBuilder sb = new StringBuilder();
                    for (String c : tables.getOrDefault(key, List.of())) {
                        sb.append(c).append("\tString\t\n");
                    }
                    response = sb.toString();
                } else if (sql.startsWith("SELECT sorting_key")) {
                    response = "id\n";
                } else if (sql.startsWith("SELECT count() FROM system.tables")) {
                    String key = sql.replaceAll(".*database = '([^']+)' AND name = '([^']+)'.*", "$1.$2");
                    response = tables.containsKey(key) ? "1\n" : "0\n";
                } else {
                    statements.add(sql);
                    java.util.regex.Matcher m = java.util.regex.Pattern
                            .compile("ALTER TABLE `([^`]+)`\\.`([^`]+)` ADD COLUMN IF NOT EXISTS `([^`]+)`").matcher(sql);
                    if (m.find()) {
                        List<String> cols = new ArrayList<>(tables.get(m.group(1) + "." + m.group(2)));
                        cols.add(cols.indexOf("_version"), m.group(3));
                        tables.put(m.group(1) + "." + m.group(2), cols);
                    }
                    java.util.regex.Matcher c = java.util.regex.Pattern
                            .compile("^CREATE TABLE IF NOT EXISTS `([^`]+)`\\.`([^`]+)`").matcher(sql);
                    if (c.find()) {
                        List<String> cols = new ArrayList<>();
                        java.util.regex.Matcher col = java.util.regex.Pattern.compile("(?m)^    `([^`]+)`").matcher(sql);
                        while (col.find()) {
                            cols.add(col.group(1));
                        }
                        tables.put(c.group(1) + "." + c.group(2), cols);
                    }
                    java.util.regex.Matcher r = java.util.regex.Pattern
                            .compile("RENAME TABLE `([^`]+)`\\.`([^`]+)` TO `([^`]+)`\\.`([^`]+)`").matcher(sql);
                    if (r.find()) {
                        tables.put(r.group(3) + "." + r.group(4), tables.remove(r.group(1) + "." + r.group(2)));
                    }
                }
            }
            byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(status, bytes.length == 0 ? -1 : bytes.length);
            if (bytes.length > 0) {
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(bytes);
                }
            }
            exchange.close();
        });
        server.start();
        config = new CdcConfig(Map.of(
                "source.hostname", "127.0.0.1",
                "source.username", "root",
                "source.databases", "demo",
                "source.server-time-zone", "Asia/Shanghai",
                "clickhouse.url", "http://127.0.0.1:" + server.getAddress().getPort(),
                "sink.batch.size", "100",
                "sink.batch.interval-ms", "1000"));
        tables.put("demo.t_user", List.of("id", "name", "_version", "_is_deleted", "_op", "_source_ts", "_synced_at"));
        SOURCE_TABLES.clear();
        SOURCE_TABLES.put("demo.t_user", sourceTable("id", "name"));
    }

    @AfterEach
    void stopServer() {
        server.stop(0);
    }

    private static byte[] readAll(InputStream in, boolean gzip) throws IOException {
        InputStream source = gzip ? new GZIPInputStream(in) : in;
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        source.transferTo(out);
        return out.toByteArray();
    }

    private KeyedOneInputStreamOperatorTestHarness<String, ChangeEvent, Void> harness() throws Exception {
        return new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(new TestWriter(config)), ChangeEvent::partitionKey, Types.STRING);
    }

    private static ChangeEvent dml(ChangeEvent.Op op, Object[] before, Object[] after, String... columns) {
        ChangeEvent e = new ChangeEvent();
        e.op = op;
        e.database = "demo";
        e.table = "t_user";
        e.sourceTsMs = op == ChangeEvent.Op.READ ? 0 : 1714557600000L;
        e.columns = new ArrayList<>(Arrays.asList(columns.length == 0 ? new String[]{"id", "name"} : columns));
        e.primaryKeys = new ArrayList<>(List.of("id"));
        e.before = before;
        e.after = after;
        return e;
    }

    /** 解析写入的 TSV：每行 [业务列..., _version, _is_deleted, _op, _source_ts] */
    private List<String[]> rows() {
        List<String[]> rows = new ArrayList<>();
        for (String[] insert : inserts) {
            for (String line : insert[1].split("\n")) {
                rows.add(line.split("\t", -1));
            }
        }
        return rows;
    }

    @Test
    void writesUpsertsAndDeletesWithVersions() throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<String, ChangeEvent, Void> h = harness()) {
            h.open();
            h.processElement(dml(ChangeEvent.Op.READ, null, new Object[]{1L, "a"}), 0);
            h.processElement(dml(ChangeEvent.Op.CREATE, null, new Object[]{2L, "b\tc"}), 0);
            h.processElement(dml(ChangeEvent.Op.UPDATE, new Object[]{2L, "b\tc"}, new Object[]{2L, null}), 0);
            h.processElement(dml(ChangeEvent.Op.UPDATE, new Object[]{1L, "a"}, new Object[]{3L, "a"}), 0);
            h.processElement(dml(ChangeEvent.Op.DELETE, new Object[]{2L, null}, null), 0);
            assertTrue(inserts.isEmpty(), "未到批次大小和间隔，不应写出");

            // checkpoint 时写出全部缓冲
            h.snapshot(1L, 1L);
            assertEquals(1, inserts.size());
            assertEquals("INSERT INTO `demo`.`t_user` (`id`, `name`, `_version`, `_is_deleted`, `_op`, `_source_ts`) FORMAT TabSeparated",
                    inserts.get(0)[0]);

            List<String[]> rows = rows();
            assertEquals(6, rows.size());
            assertEquals(List.of("1", "a", "0", "0", "r", "1970-01-01 08:00:00.000"), Arrays.asList(rows.get(0)));
            assertEquals(List.of("2", "b\\tc"), Arrays.asList(rows.get(1)).subList(0, 2));
            assertEquals(List.of("0", "c", "2024-05-01 18:00:00.000"), Arrays.asList(rows.get(1)).subList(3, 6));
            assertEquals(List.of("2", "\\N"), Arrays.asList(rows.get(2)).subList(0, 2));
            assertEquals("u", rows.get(2)[4]);
            // 主键 1 -> 3：旧主键删除 + 新主键写入
            assertEquals(List.of("1", "a"), Arrays.asList(rows.get(3)).subList(0, 2));
            assertEquals(List.of("1", "d"), Arrays.asList(rows.get(3)).subList(3, 5));
            assertEquals(List.of("3", "a"), Arrays.asList(rows.get(4)).subList(0, 2));
            assertEquals(List.of("0", "u"), Arrays.asList(rows.get(4)).subList(3, 5));
            // 删除使用 before 数据
            assertEquals(List.of("2", "\\N"), Arrays.asList(rows.get(5)).subList(0, 2));
            assertEquals(List.of("1", "d"), Arrays.asList(rows.get(5)).subList(3, 5));

            // 增量版本严格递增，且都大于快照版本 0
            long previous = 0;
            for (int i = 1; i < rows.size(); i++) {
                long version = Long.parseLong(rows.get(i)[2]);
                assertTrue(version > previous, "版本号必须递增");
                previous = version;
            }
        }
    }

    @Test
    void flushesOnTimerAndBatchSize() throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<String, ChangeEvent, Void> h = harness()) {
            h.open();
            h.setProcessingTime(0);
            h.processElement(dml(ChangeEvent.Op.CREATE, null, new Object[]{1L, "a"}), 0);
            h.setProcessingTime(999);
            assertTrue(inserts.isEmpty());
            h.setProcessingTime(1000);
            assertEquals(1, inserts.size());

            for (long i = 0; i < 100; i++) {
                h.processElement(dml(ChangeEvent.Op.CREATE, null, new Object[]{i, "x"}), 0);
            }
            assertEquals(2, inserts.size(), "达到 sink.batch.size 立即写出");
        }
    }

    @Test
    void flushesAllBuffersWhenTotalBufferedBytesExceedLimit() throws Exception {
        config = config.withOverrides(Map.of("sink.buffer.max-total-bytes", "60"));
        tables.put("demo.t_order", List.of("id", "name", "_version", "_is_deleted", "_op", "_source_ts", "_synced_at"));
        try (KeyedOneInputStreamOperatorTestHarness<String, ChangeEvent, Void> h = harness()) {
            h.open();
            h.processElement(dml(ChangeEvent.Op.CREATE, null, new Object[]{1L, "a"}), 0);
            assertTrue(inserts.isEmpty());

            // 两张表都没满批，但合计超过 60 字节：全部写出
            ChangeEvent order = dml(ChangeEvent.Op.CREATE, null, new Object[]{2L, "b"});
            order.table = "t_order";
            h.processElement(order, 0);
            assertEquals(2, inserts.size());

            // 计数随写出归零，之后的小数据继续攒批
            h.processElement(dml(ChangeEvent.Op.CREATE, null, new Object[]{3L, "c"}), 0);
            assertEquals(2, inserts.size());
        }
    }

    @Test
    void writesToMappedClickHouseDatabase() throws Exception {
        // demo 库映射到 ClickHouse 的 demo_ch 库；目标表不存在，按 MySQL 源表结构在 demo_ch 中创建
        config = config.withOverrides(Map.of("clickhouse.database-mapping", "demo:demo_ch"));
        tables.clear();
        try (KeyedOneInputStreamOperatorTestHarness<String, ChangeEvent, Void> h = harness()) {
            h.open();
            h.processElement(dml(ChangeEvent.Op.CREATE, null, new Object[]{1L, "a"}), 0);
            h.snapshot(1L, 1L);
        }
        assertTrue(statements.stream().anyMatch(sql -> sql.startsWith("CREATE DATABASE IF NOT EXISTS `demo_ch`")), statements.toString());
        assertTrue(statements.stream().anyMatch(sql -> sql.startsWith("CREATE TABLE IF NOT EXISTS `demo_ch`.`t_user`")), statements.toString());
        assertEquals(1, inserts.size());
        assertTrue(inserts.get(0)[0].startsWith("INSERT INTO `demo_ch`.`t_user`"), inserts.get(0)[0]);
        assertTrue(statements.stream().noneMatch(sql -> sql.contains("`demo`.")), "不应访问与 MySQL 同名的库");
    }

    @Test
    void skipsColumnsMissingInTarget() throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<String, ChangeEvent, Void> h = harness()) {
            h.open();
            // 源表已删除的列 dropped 仍出现在回放的旧 binlog 中
            h.processElement(dml(ChangeEvent.Op.CREATE, null, new Object[]{1L, "gone", "a"}, "id", "dropped", "name"), 0);
            h.snapshot(1L, 1L);
            assertEquals("INSERT INTO `demo`.`t_user` (`id`, `name`, `_version`, `_is_deleted`, `_op`, `_source_ts`) FORMAT TabSeparated",
                    inserts.get(0)[0]);
            assertEquals(List.of("1", "a"), Arrays.asList(rows().get(0)).subList(0, 2));
        }
    }

    @Test
    void columnAddedDuringSnapshotIsNotLost() throws Exception {
        // 全量期间源表新增了 age 列：快照行已带 age，但目标表要等 binlog 阶段的 DDL 才会加列
        SOURCE_TABLES.put("demo.t_user", sourceTable("id", "name", "age"));
        try (KeyedOneInputStreamOperatorTestHarness<String, ChangeEvent, Void> h = harness()) {
            h.open();
            h.processElement(dml(ChangeEvent.Op.READ, null, new Object[]{1L, "a", "18"}, "id", "name", "age"), 0);
            h.snapshot(1L, 1L);
            assertEquals(List.of(
                    "CREATE DATABASE IF NOT EXISTS `demo`",
                    "ALTER TABLE `demo`.`t_user` ADD COLUMN IF NOT EXISTS `age` String AFTER `name`",
                    "INSERT INTO `demo`.`t_user` (`id`, `name`, `age`, `_version`, `_is_deleted`, `_op`, `_source_ts`) FORMAT TabSeparated"),
                    statements, "先补列，再写入完整数据");
            assertEquals(List.of("1", "a", "18"), Arrays.asList(rows().get(0)).subList(0, 3));
        }
    }

    @Test
    void retriesFailedInsert() throws Exception {
        failNextInserts.set(1);
        try (KeyedOneInputStreamOperatorTestHarness<String, ChangeEvent, Void> h = harness()) {
            h.open();
            h.processElement(dml(ChangeEvent.Op.CREATE, null, new Object[]{1L, "a"}), 0);
            h.snapshot(1L, 1L);
            assertEquals(1, inserts.size());
        }
    }

    private static ChangeEvent alterAddAge(String table, String previousTable) {
        ChangeEvent ddl = new ChangeEvent();
        ddl.op = ChangeEvent.Op.DDL;
        ddl.ddlType = "ALTER";
        ddl.ddl = "ALTER TABLE " + table + " ADD COLUMN age INT NULL";
        ddl.database = "demo";
        ddl.table = table;
        ddl.previousDatabase = previousTable == null ? null : "demo";
        ddl.previousTable = previousTable;
        TableDef def = new TableDef("demo", table);
        // 模拟服务器的现有列类型都是 String，这里定义成对应的 varchar 非空列，只有 age 是新增列
        def.columns.add(new ColumnDef("id", "varchar", false, 32, null, false, null));
        def.columns.add(new ColumnDef("name", "varchar", false, 20, null, false, null));
        def.columns.add(new ColumnDef("age", "int", false, null, null, true, null));
        def.primaryKeys.add("id");
        ddl.tableDef = def;
        return ddl;
    }

    @Test
    void ddlWaitsForCheckpointAndKeepsOrder() throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<String, ChangeEvent, Void> h = harness()) {
            h.open();
            h.setProcessingTime(0);
            h.processElement(dml(ChangeEvent.Op.CREATE, null, new Object[]{1L, "a"}), 0);
            h.processElement(alterAddAge("t_user", null), 0);
            // DDL 之后的数据（已带新列）进入暂存
            h.processElement(dml(ChangeEvent.Op.CREATE, null, new Object[]{2L, "b", "18"}, "id", "name", "age"), 0);
            assertEquals(1, statements.size(), "DDL 到达时只写出之前缓冲的数据");
            assertTrue(statements.get(0).startsWith("INSERT INTO `demo`.`t_user` (`id`, `name`,"));

            // checkpoint 尚未完成：DDL 不执行
            h.snapshot(1L, 1L);
            h.setProcessingTime(5_000);
            assertEquals(1, statements.size());

            // checkpoint 完成后：先执行 DDL，再写入暂存的数据
            h.notifyOfCompletedCheckpoint(1L);
            h.setProcessingTime(10_000);
            assertEquals(List.of(
                    "CREATE DATABASE IF NOT EXISTS `demo`",
                    "ALTER TABLE `demo`.`t_user` ADD COLUMN IF NOT EXISTS `age` Nullable(Int32) AFTER `name`",
                    "INSERT INTO `demo`.`t_user` (`id`, `name`, `age`, `_version`, `_is_deleted`, `_op`, `_source_ts`) FORMAT TabSeparated"),
                    statements.subList(1, statements.size()));
        }
    }

    @Test
    void pendingDdlSurvivesRestore() throws Exception {
        OperatorSubtaskState state;
        try (KeyedOneInputStreamOperatorTestHarness<String, ChangeEvent, Void> h = harness()) {
            h.open();
            h.processElement(alterAddAge("t_user", null), 0);
            h.processElement(dml(ChangeEvent.Op.CREATE, null, new Object[]{2L, "b", "18"}, "id", "name", "age"), 0);
            // 在 DDL 执行前做了 checkpoint，随后故障
            state = h.snapshot(1L, 1L);
        }
        assertTrue(statements.isEmpty());
        try (KeyedOneInputStreamOperatorTestHarness<String, ChangeEvent, Void> h = harness()) {
            h.initializeState(state);
            h.open();
            h.setProcessingTime(Long.MAX_VALUE / 2);
            assertTrue(statements.contains("ALTER TABLE `demo`.`t_user` ADD COLUMN IF NOT EXISTS `age` Nullable(Int32) AFTER `name`"),
                    "恢复后执行暂存的 DDL: " + statements);
            assertEquals(List.of("2", "b", "18"), Arrays.asList(rows().get(0)).subList(0, 3));
        }
    }

    @Test
    void createThenRenameTableKeepsOrderAcrossTables() throws Exception {
        // 端到端测试发现的问题：按表分区时 CREATE t_new 与 RENAME t_new -> t_renamed 落在不同实例，RENAME 先执行导致数据留在旧表
        ChangeEvent create = alterAddAge("t_new", null);
        create.ddlType = "CREATE";
        create.ddl = "CREATE TABLE t_new (...)";
        ChangeEvent rename = alterAddAge("t_renamed", "t_new");
        rename.ddl = "RENAME TABLE t_new TO t_renamed";
        ChangeEvent row1 = dml(ChangeEvent.Op.CREATE, null, new Object[]{"1", "n1", null}, "id", "name", "age");
        row1.table = "t_new";
        ChangeEvent row3 = dml(ChangeEvent.Op.CREATE, null, new Object[]{"3", "n3", null}, "id", "name", "age");
        row3.table = "t_renamed";
        try (KeyedOneInputStreamOperatorTestHarness<String, ChangeEvent, Void> h = harness()) {
            h.open();
            h.setProcessingTime(0);
            h.processElement(create, 0);
            h.processElement(row1, 0);
            h.processElement(rename, 0);
            h.processElement(row3, 0);
            h.snapshot(1L, 1L);
            h.notifyOfCompletedCheckpoint(1L);
            h.setProcessingTime(10_000);
        }
        List<String> relevant = new ArrayList<>();
        for (String s : statements) {
            // 模拟服务器把所有列类型都报为 String，会触发多余的 MODIFY COLUMN，与本测试关注的顺序无关
            if ((s.contains("t_new") || s.contains("t_renamed")) && !s.contains("MODIFY COLUMN")) {
                relevant.add(s.startsWith("CREATE TABLE") ? s.substring(0, s.indexOf('\n')) : s);
            }
        }
        assertEquals(List.of(
                "CREATE TABLE IF NOT EXISTS `demo`.`t_new`",
                "INSERT INTO `demo`.`t_new` (`id`, `name`, `age`, `_version`, `_is_deleted`, `_op`, `_source_ts`) FORMAT TabSeparated",
                "RENAME TABLE `demo`.`t_new` TO `demo`.`t_renamed`",
                "INSERT INTO `demo`.`t_renamed` (`id`, `name`, `age`, `_version`, `_is_deleted`, `_op`, `_source_ts`) FORMAT TabSeparated"),
                relevant, "同一个库的跨表 DDL 与数据必须严格按顺序执行");
    }

    @Test
    void renameTableKeepsData() throws Exception {
        tables.put("demo.t_old", tables.remove("demo.t_user"));
        try (KeyedOneInputStreamOperatorTestHarness<String, ChangeEvent, Void> h = harness()) {
            h.open();
            h.setProcessingTime(0);
            h.processElement(alterAddAge("t_new", "t_old"), 0);
            h.snapshot(1L, 1L);
            h.notifyOfCompletedCheckpoint(1L);
            h.setProcessingTime(10_000);
            assertEquals(List.of(
                    "CREATE DATABASE IF NOT EXISTS `demo`",
                    "RENAME TABLE `demo`.`t_old` TO `demo`.`t_new`",
                    "ALTER TABLE `demo`.`t_new` ADD COLUMN IF NOT EXISTS `age` Nullable(Int32) AFTER `name`"),
                    statements, "原表改名保留数据，而不是新建空表");
        }
    }

    @Test
    void versionContinuesAfterRestore() throws Exception {
        OperatorSubtaskState state;
        long maxVersion;
        try (KeyedOneInputStreamOperatorTestHarness<String, ChangeEvent, Void> h = harness()) {
            h.open();
            h.processElement(dml(ChangeEvent.Op.CREATE, null, new Object[]{1L, "a"}), 0);
            state = h.snapshot(1L, 1L);
            maxVersion = Long.parseLong(rows().get(0)[2]);
        }
        inserts.clear();
        try (KeyedOneInputStreamOperatorTestHarness<String, ChangeEvent, Void> h = harness()) {
            h.initializeState(state);
            h.open();
            h.processElement(dml(ChangeEvent.Op.UPDATE, new Object[]{1L, "a"}, new Object[]{1L, "b"}), 0);
            h.snapshot(2L, 2L);
            assertTrue(Long.parseLong(rows().get(0)[2]) > maxVersion);
        }
    }
}
