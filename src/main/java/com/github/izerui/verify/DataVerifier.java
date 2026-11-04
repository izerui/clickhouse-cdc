package com.github.izerui.verify;

import com.github.izerui.clickhouse.ClickHouseClient;
import com.github.izerui.clickhouse.ClickHouseSql;
import com.github.izerui.config.CdcConfig;
import com.github.izerui.model.TableDef;
import com.github.izerui.source.MySqlSchemaReader;
import com.github.izerui.source.TableFilter;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * 逐表比对 MySQL 与 ClickHouse 的行数。
 * <p>
 * ClickHouse 端按 {@code FINAL WHERE _is_deleted = 0} 统计，即去重、排除删除后的最新数据。
 * 同步运行中比对时，频繁写入的表可能因同步延迟出现少量差异，可在低峰期或多次比对确认。
 */
public class DataVerifier {

    public static class Result {
        /** 库名.表名（两端一致） */
        public final String table;
        public final long sourceCount;
        /** 目标表不存在时为 -1 */
        public final long targetCount;

        Result(String table, long sourceCount, long targetCount) {
            this.table = table;
            this.sourceCount = sourceCount;
            this.targetCount = targetCount;
        }

        public boolean matched() {
            return sourceCount == targetCount;
        }
    }

    private final CdcConfig config;
    private final ClickHouseClient client;

    public DataVerifier(CdcConfig config, ClickHouseClient client) {
        this.config = config;
        this.client = client;
    }

    public List<Result> verify() throws Exception {
        TableFilter filter = TableFilter.fromConfig(config, null);
        MySqlSchemaReader reader = new MySqlSchemaReader(config);
        List<Result> results = new ArrayList<>();
        try (Connection conn = reader.openConnection()) {
            for (String database : config.getSourceDatabases()) {
                for (TableDef t : reader.readTables(conn, database)) {
                    if (!filter.accept(database, t.table) || !t.hasPrimaryKey()) {
                        continue;
                    }
                    long sourceCount;
                    try (Statement st = conn.createStatement();
                         ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM `" + database.replace("`", "``") + "`.`"
                                 + t.table.replace("`", "``") + "`")) {
                        rs.next();
                        sourceCount = rs.getLong(1);
                    }
                    results.add(new Result(database + "." + t.table, sourceCount, targetCount(database, t.table)));
                }
            }
        }
        return results;
    }

    private long targetCount(String sourceDatabase, String table) {
        String database = config.getClickHouseDatabase(sourceDatabase);
        List<String[]> exists = client.query("SELECT count() FROM system.tables WHERE database = "
                + ClickHouseSql.literal(database) + " AND name = " + ClickHouseSql.literal(table));
        if (exists.isEmpty() || "0".equals(exists.get(0)[0])) {
            return -1;
        }
        List<String[]> rows = client.query("SELECT count() FROM " + ClickHouseSql.table(database, table)
                + " FINAL WHERE " + ClickHouseSql.quote(ClickHouseSql.COL_IS_DELETED) + " = 0");
        return Long.parseLong(rows.get(0)[0]);
    }

    /** 格式化输出，返回不一致的表数量 */
    public static int print(List<Result> results, StringBuilder out) {
        int width = results.stream().mapToInt(r -> r.table.length()).max().orElse(10);
        String format = "%-" + width + "s  %14s  %14s  %s%n";
        out.append(String.format(format, "表", "MySQL 行数", "ClickHouse 行数", "结果"));
        int mismatched = 0;
        for (Result r : results) {
            String status = r.targetCount < 0 ? "目标表不存在" : r.matched() ? "一致" : "差异 " + (r.targetCount - r.sourceCount);
            if (!r.matched()) {
                mismatched++;
            }
            out.append(String.format(format, r.table, r.sourceCount, r.targetCount < 0 ? "-" : String.valueOf(r.targetCount), status));
        }
        out.append(String.format("共 %d 张表，一致 %d 张，不一致 %d 张%n", results.size(), results.size() - mismatched, mismatched));
        return mismatched;
    }
}
