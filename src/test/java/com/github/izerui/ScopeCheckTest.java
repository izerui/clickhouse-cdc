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
}
