package com.github.izerui.clickhouse;

import com.github.izerui.config.CdcConfig;
import com.github.izerui.model.TableDef;
import com.github.izerui.source.MySqlSchemaReader;
import com.github.izerui.source.TableFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * 启动前扫描 MySQL 源表，并在全新同步时预建 / 校正 ClickHouse 目标表。
 */
public class TargetTableInitializer {

    private static final Logger LOG = LoggerFactory.getLogger(TargetTableInitializer.class);

    /** 扫描结果 */
    public static class Scan {
        /** 需要同步的表（有主键、符合过滤规则） */
        public final List<TableDef> tables = new ArrayList<>();
        /** 没有主键、被排除的表（库名.表名） */
        public final Set<String> noPrimaryKeyTables = new LinkedHashSet<>();
    }

    private final CdcConfig config;
    private final MySqlSchemaReader schemaReader;
    private final SchemaSynchronizer synchronizer;

    public TargetTableInitializer(CdcConfig config, ClickHouseClient client) {
        this.config = config;
        this.schemaReader = new MySqlSchemaReader(config);
        this.synchronizer = new SchemaSynchronizer(client, config);
    }

    /** 检查 binlog 配置并扫描需要同步的表 */
    public Scan scan() throws Exception {
        TableFilter filter = TableFilter.fromConfig(config, null);
        Scan scan = new Scan();
        Set<String> existing = new HashSet<>();
        try (Connection conn = schemaReader.openConnection()) {
            schemaReader.validateBinlog(conn);
            for (String database : config.getSourceDatabases()) {
                int count = 0;
                List<TableDef> all = schemaReader.readTables(conn, database);
                all.forEach(t -> existing.add(t.fullName().toLowerCase()));
                for (TableDef t : all) {
                    if (!filter.accept(database, t.table)) {
                        continue;
                    }
                    if (!t.hasPrimaryKey()) {
                        scan.noPrimaryKeyTables.add(t.fullName());
                        continue;
                    }
                    scan.tables.add(t);
                    count++;
                }
                LOG.info("库 {}：待同步 {} 张表", database, count);
            }
        }
        // 指定的表必须存在，避免表名写错后悄悄漏同步：带库名的须在该库中存在，不带库名的须在任一库中存在
        List<String> missing = new ArrayList<>();
        for (String listed : config.getSourceTables()) {
            String name = listed.toLowerCase();
            boolean found = name.contains(".") ? existing.contains(name)
                    : existing.stream().anyMatch(t -> t.endsWith("." + name));
            if (!found) {
                missing.add(listed);
            }
        }
        if (!missing.isEmpty()) {
            throw new IllegalStateException("source.tables 中指定的表在 MySQL 中不存在: " + missing);
        }
        if (!scan.noPrimaryKeyTables.isEmpty()) {
            LOG.warn("以下 {} 张表没有主键，不参与同步: {}", scan.noPrimaryKeyTables.size(), scan.noPrimaryKeyTables);
        }
        return scan;
    }

    /**
     * 全新同步（不从 checkpoint 恢复）时，按源表当前结构创建 / 校正目标表。
     * <p>
     * 从 checkpoint 恢复时不调用：此时目标表结构应停留在 checkpoint 对应的位点，停机期间的表结构变更
     * 会随 binlog 中的 DDL 按顺序回放；若提前按当前结构校正，列改名会被当成“删除 + 新增”而丢失历史数据。
     *
     * @param truncate 是否清空目标表（全量同步前）
     */
    public void prepareTables(Scan scan, boolean truncate) {
        for (String database : config.getSourceDatabases()) {
            synchronizer.ensureDatabase(database);
        }
        for (TableDef t : scan.tables) {
            synchronizer.sync(t, null);
            if (truncate) {
                synchronizer.truncate(t.database, t.table);
            }
        }
        LOG.info("目标表初始化完成，共 {} 张表{}", scan.tables.size(), truncate ? "（已清空，准备全量同步）" : "");
    }
}
