package com.github.izerui.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CdcConfigTest {

    private static Map<String, String> required() {
        Map<String, String> m = new HashMap<>();
        m.put("source.hostname", "127.0.0.1");
        m.put("source.username", "root");
        m.put("source.databases", "a, b");
        m.put("clickhouse.url", "http://127.0.0.1:8123/");
        return m;
    }

    @Test
    void loadsFileWithOverridesAndPlaceholders(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("app.properties");
        Files.writeString(file, "source.hostname=db\nsource.username=u\nsource.databases=a\nclickhouse.url=http://ch:8123\n"
                + "source.password=${CDC_TEST_NOT_EXIST_ENV:默认密码}\njob.parallelism=2\n");
        CdcConfig config = CdcConfig.load(file, Map.of("job.parallelism", "8"));
        assertEquals("默认密码", config.getSourcePassword());
        assertEquals(8, config.getJobParallelism());
        assertEquals("http://ch:8123", config.getClickHouseUrl());
        assertEquals(true, config.describe().contains("source.password = ******"));
    }

    @Test
    void defaults() {
        CdcConfig config = new CdcConfig(required());
        assertEquals("http://127.0.0.1:8123", config.getClickHouseUrl());
        assertEquals(CdcConfig.StartupMode.INITIAL, config.getStartupMode());
        assertEquals("exponential-delay", config.getJobRestartStrategy());
    }

    @Test
    void databaseMapping() {
        Map<String, String> m = required();
        m.put("clickhouse.database-mapping", "a:a_ch");
        CdcConfig config = new CdcConfig(m);
        assertEquals("a_ch", config.getClickHouseDatabase("a"));
        assertEquals("b", config.getClickHouseDatabase("b"), "未映射的库与 MySQL 同名");
        assertEquals(Map.of("a", "a_ch", "b", "b"), config.getTargetDatabases());

        Map<String, String> unknown = required();
        unknown.put("clickhouse.database-mapping", "x:y");
        assertThrows(IllegalArgumentException.class, () -> new CdcConfig(unknown));
        Map<String, String> badFormat = required();
        badFormat.put("clickhouse.database-mapping", "a");
        assertThrows(IllegalArgumentException.class, () -> new CdcConfig(badFormat));
        Map<String, String> sameTarget = required();
        sameTarget.put("clickhouse.database-mapping", "a:b");
        assertThrows(IllegalArgumentException.class, () -> new CdcConfig(sameTarget), "a 映射到 b 与源库 b 冲突");
    }

    @Test
    void validation() {
        Map<String, String> missing = required();
        missing.remove("source.databases");
        assertThrows(IllegalArgumentException.class, () -> new CdcConfig(missing));

        Map<String, String> timestamp = required();
        timestamp.put("startup.mode", "timestamp");
        assertThrows(IllegalArgumentException.class, () -> new CdcConfig(timestamp));

        Map<String, String> unknownDb = required();
        unknownDb.put("source.tables", "x.t_user");
        assertThrows(IllegalArgumentException.class, () -> new CdcConfig(unknownDb));

        Map<String, String> portConflict = required();
        portConflict.put("health.port", "8080");
        portConflict.put("job.web-ui.port", "8080");
        assertThrows(IllegalArgumentException.class, () -> new CdcConfig(portConflict));

        Map<String, String> badRestart = required();
        badRestart.put("job.restart.strategy", "none");
        assertThrows(IllegalArgumentException.class, () -> new CdcConfig(badRestart).getJobRestartStrategy());
    }

    @Test
    void sourceTablesList() {
        Map<String, String> m = required();
        m.put("source.tables", " t_user , a.t_order ,");
        assertEquals(java.util.List.of("t_user", "a.t_order"), new CdcConfig(m).getSourceTables());
        assertNotEquals(new CdcConfig(required()).describeSyncScope(), new CdcConfig(m).describeSyncScope(),
                "表清单变化属于同步范围变化");
    }

    @Test
    void syncScopeChangesWithTableRules() {
        CdcConfig a = new CdcConfig(required());
        Map<String, String> m = required();
        m.put("source.tables.exclude", "tmp_.*");
        assertNotEquals(a.describeSyncScope(), new CdcConfig(m).describeSyncScope());
        Map<String, String> unrelated = required();
        unrelated.put("sink.batch.size", "10");
        assertEquals(a.describeSyncScope(), new CdcConfig(unrelated).describeSyncScope());
    }
}
