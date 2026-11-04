package com.github.izerui.source;

import com.github.izerui.config.CdcConfig;
import com.github.izerui.model.ColumnDef;
import com.github.izerui.model.TableDef;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * 从 MySQL information_schema 读取表结构。
 */
public class MySqlSchemaReader {

    private final CdcConfig config;

    public MySqlSchemaReader(CdcConfig config) {
        this.config = config;
    }

    public Connection openConnection() throws SQLException {
        String url = "jdbc:mysql://" + config.getSourceHostname() + ":" + config.getSourcePort()
                + "/?useSSL=false&allowPublicKeyRetrieval=true&characterEncoding=UTF-8&connectTimeout="
                + config.getSourceConnectTimeoutSeconds() * 1000L;
        Properties props = new Properties();
        props.setProperty("user", config.getSourceUsername());
        props.setProperty("password", config.getSourcePassword());
        return DriverManager.getConnection(url, props);
    }

    /**
     * 启动前检查 binlog 配置，不满足 CDC 要求时直接报错（否则作业会在无限重启中反复失败）。
     */
    public void validateBinlog(Connection conn) throws SQLException {
        Map<String, String> vars = new LinkedHashMap<>();
        try (PreparedStatement ps = conn.prepareStatement(
                "SHOW VARIABLES WHERE Variable_name IN ('log_bin', 'binlog_format', 'binlog_row_image')");
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                vars.put(rs.getString(1).toLowerCase(), rs.getString(2));
            }
        }
        List<String> problems = new ArrayList<>();
        if (!"ON".equalsIgnoreCase(vars.get("log_bin"))) {
            problems.add("log_bin=" + vars.get("log_bin") + "（需要开启 binlog）");
        }
        if (!"ROW".equalsIgnoreCase(vars.get("binlog_format"))) {
            problems.add("binlog_format=" + vars.get("binlog_format") + "（需要 ROW）");
        }
        if (vars.containsKey("binlog_row_image") && !"FULL".equalsIgnoreCase(vars.get("binlog_row_image"))) {
            problems.add("binlog_row_image=" + vars.get("binlog_row_image") + "（需要 FULL）");
        }
        if (!problems.isEmpty()) {
            throw new IllegalStateException("MySQL binlog 配置不满足 CDC 要求: " + String.join("; ", problems));
        }
    }

    /**
     * 读取某个库下所有普通表（不含视图）的结构，按表名排序。
     */
    public List<TableDef> readTables(Connection conn, String database) throws SQLException {
        Map<String, TableDef> tables = new LinkedHashMap<>();
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT TABLE_NAME, TABLE_COMMENT, AVG_ROW_LENGTH, TABLE_ROWS FROM information_schema.TABLES "
                        + "WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME")) {
            ps.setString(1, database);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    TableDef t = new TableDef(database, rs.getString(1));
                    t.comment = rs.getString(2);
                    t.avgRowLength = rs.getLong(3);
                    t.estimatedRows = rs.getLong(4);
                    tables.put(t.table, t);
                }
            }
        }
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, IS_NULLABLE, NUMERIC_PRECISION, NUMERIC_SCALE, "
                        + "DATETIME_PRECISION, CHARACTER_MAXIMUM_LENGTH, COLUMN_COMMENT, COLUMN_DEFAULT, EXTRA "
                        + "FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? ORDER BY TABLE_NAME, ORDINAL_POSITION")) {
            ps.setString(1, database);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    TableDef t = tables.get(rs.getString("TABLE_NAME"));
                    if (t != null) {
                        t.columns.add(toColumn(rs));
                    }
                }
            }
        }
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT TABLE_NAME, COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE "
                        + "WHERE TABLE_SCHEMA = ? AND CONSTRAINT_NAME = 'PRIMARY' ORDER BY TABLE_NAME, ORDINAL_POSITION")) {
            ps.setString(1, database);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    TableDef t = tables.get(rs.getString(1));
                    if (t != null) {
                        t.primaryKeys.add(rs.getString(2));
                    }
                }
            }
        }
        return new ArrayList<>(tables.values());
    }

    private static ColumnDef toColumn(ResultSet rs) throws SQLException {
        String dataType = rs.getString("DATA_TYPE").toLowerCase();
        String columnType = rs.getString("COLUMN_TYPE").toLowerCase();
        Integer length;
        Integer scale = null;
        switch (dataType) {
            case "decimal":
            case "numeric":
                length = getInt(rs, "NUMERIC_PRECISION");
                scale = getInt(rs, "NUMERIC_SCALE");
                break;
            case "bit":
                length = getInt(rs, "NUMERIC_PRECISION");
                break;
            case "datetime":
            case "timestamp":
            case "time":
                length = getInt(rs, "DATETIME_PRECISION");
                break;
            case "char":
            case "varchar":
            case "binary":
            case "varbinary":
                long charLength = rs.getLong("CHARACTER_MAXIMUM_LENGTH");
                length = rs.wasNull() ? null : (int) Math.min(Integer.MAX_VALUE, charLength);
                break;
            default:
                length = null;
        }
        ColumnDef column = new ColumnDef(rs.getString("COLUMN_NAME"), dataType, columnType.contains("unsigned"), length, scale,
                "YES".equalsIgnoreCase(rs.getString("IS_NULLABLE")), rs.getString("COLUMN_COMMENT"));
        String extra = rs.getString("EXTRA");
        String defaultValue = rs.getString("COLUMN_DEFAULT");
        // MySQL 8.0.13+ 的表达式默认值（如 CURRENT_TIMESTAMP、(uuid())）在 EXTRA 中标记为 DEFAULT_GENERATED，不传播
        boolean expression = extra != null && extra.toUpperCase().contains("DEFAULT_GENERATED");
        if (defaultValue != null && !expression && !defaultValue.toUpperCase().startsWith("CURRENT_TIMESTAMP")) {
            column.defaultValue = defaultValue;
        }
        return column;
    }

    private static Integer getInt(ResultSet rs, String column) throws SQLException {
        long v = rs.getLong(column);
        return rs.wasNull() ? null : (int) v;
    }
}
