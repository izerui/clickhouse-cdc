package com.github.izerui;

import com.github.izerui.clickhouse.TargetTableInitializer;
import com.github.izerui.config.CdcConfig;
import com.github.izerui.model.TableDef;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ScopeCheckTest {

    private static CdcConfig config(String exclude) {
        Map<String, String> m = new HashMap<>();
        m.put("source.hostname", "127.0.0.1");
        m.put("source.username", "root");
        m.put("source.databases", "demo");
        m.put("clickhouse.url", "http://127.0.0.1:8123");
        m.put("source.tables.exclude", exclude);
        return new CdcConfig(m);
    }

    private static TargetTableInitializer.Scan scan(String... tables) {
        TargetTableInitializer.Scan scan = new TargetTableInitializer.Scan();
        for (String t : tables) {
            scan.tables.add(new TableDef("demo", t));
        }
        return scan;
    }

    @Test
    void passesWhenUnchanged(@TempDir Path dir) throws Exception {
        Path scope = dir.resolve("sync-scope.txt");
        Path noPk = dir.resolve("no-primary-key-tables.txt");
        Files.writeString(scope, config("tmp_.*").describeSyncScope());
        Files.write(noPk, List.of("demo.log"));
        assertDoesNotThrow(() -> CdcApplication.checkScopeUnchanged(config("tmp_.*"), scan("t_user"), scope, noPk));
        // 首次续传（没有记录文件）不拦截
        assertDoesNotThrow(() -> CdcApplication.checkScopeUnchanged(config(""), scan("t_user"),
                dir.resolve("none-1"), dir.resolve("none-2")));
    }

    @Test
    void rejectsConfigChangeAndTablesGainingPrimaryKey(@TempDir Path dir) throws Exception {
        Path scope = dir.resolve("sync-scope.txt");
        Path noPk = dir.resolve("no-primary-key-tables.txt");
        Files.writeString(scope, config("tmp_.*").describeSyncScope());
        Files.write(noPk, List.of("demo.log"));

        IllegalStateException changed = assertThrows(IllegalStateException.class,
                () -> CdcApplication.checkScopeUnchanged(config(""), scan("t_user"), scope, noPk));
        assertTrue(changed.getMessage().contains("同步范围配置变化"));

        IllegalStateException gained = assertThrows(IllegalStateException.class,
                () -> CdcApplication.checkScopeUnchanged(config("tmp_.*"), scan("t_user", "log"), scope, noPk));
        assertTrue(gained.getMessage().contains("demo.log"), gained.getMessage());
    }

    private static CdcConfig mapped(String databases, String mapping) {
        Map<String, String> m = new HashMap<>();
        m.put("source.hostname", "127.0.0.1");
        m.put("source.username", "root");
        m.put("source.databases", databases);
        m.put("clickhouse.url", "http://127.0.0.1:8123");
        m.put("clickhouse.database-mapping", mapping);
        return new CdcConfig(m);
    }

    @Test
    void targetDatabaseCheck(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("target-databases.txt");
        Files.write(file, List.of("demo=demo"));
        // 未变化、首次续传（没有记录文件）、新增源库：不拦截
        assertDoesNotThrow(() -> CdcApplication.checkTargetDatabasesUnchanged(mapped("demo", ""), file));
        assertDoesNotThrow(() -> CdcApplication.checkTargetDatabasesUnchanged(mapped("demo", "demo:demo_ch"), dir.resolve("none")));
        assertDoesNotThrow(() -> CdcApplication.checkTargetDatabasesUnchanged(mapped("demo,other", "other:other_ch"), file));
        // 已同步的库改了目标库：拒绝
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> CdcApplication.checkTargetDatabasesUnchanged(mapped("demo", "demo:demo_ch"), file));
        assertTrue(e.getMessage().contains("demo：demo -> demo_ch"), e.getMessage());
        // 去掉映射、改回同名库同样是目标库变化
        Files.write(file, List.of("demo=demo_ch"));
        assertThrows(IllegalStateException.class, () -> CdcApplication.checkTargetDatabasesUnchanged(mapped("demo", ""), file));
    }

    @Test
    void forceResumeCannotSkipTargetDatabaseChange(@TempDir Path dir) throws Exception {
        // 已有 checkpoint，上次写入 demo 库
        Path checkpoints = dir.resolve("checkpoints");
        Path chk = checkpoints.resolve("0123456789abcdef0123456789abcdef").resolve("chk-5");
        Files.createDirectories(chk);
        Files.writeString(chk.resolve("_metadata"), "x");
        Files.write(checkpoints.resolve("target-databases.txt"), List.of("demo=demo"));
        // 本次把 demo 映射到 demo_ch，并加上 --force-resume
        Path conf = dir.resolve("app.properties");
        Files.writeString(conf, "source.hostname=127.0.0.1\nsource.username=root\nsource.databases=demo\n"
                + "clickhouse.url=http://127.0.0.1:8123\nclickhouse.database-mapping=demo:demo_ch\n"
                + "checkpoint.dir=" + checkpoints.toString().replace("\\", "/") + "\n");
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> CdcApplication.run(new String[]{"--config", conf.toString(), "--force-resume"}));
        assertTrue(e.getMessage().contains("--force-resume 也不能跳过"), e.getMessage());
        // 拒绝启动时不改动原有进度
        assertTrue(Files.exists(chk.resolve("_metadata")));
        assertEquals(List.of("demo=demo"), Files.readAllLines(checkpoints.resolve("target-databases.txt")));
    }
}
