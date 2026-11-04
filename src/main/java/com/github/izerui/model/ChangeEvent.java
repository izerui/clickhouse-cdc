package com.github.izerui.model;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * 变更事件：一行数据变更（r/c/u/d）或一次表结构变更（DDL）。
 * <p>
 * 数据值在反序列化阶段已转换为与 ClickHouse 写入无关的普通 Java 值：
 * null、Boolean、Number（含 BigDecimal/BigInteger）、String、byte[]；日期时间已格式化为 ClickHouse 可解析的字符串。
 */
public class ChangeEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    public enum Op {
        /** 全量快照读取 */
        READ,
        CREATE,
        UPDATE,
        DELETE,
        /** 表结构变更 */
        DDL
    }

    public Op op;
    /** 库名（MySQL 与 ClickHouse 一致） */
    public String database;
    /** 表名（MySQL 与 ClickHouse 一致） */
    public String table;
    /** 源端事件时间（毫秒），快照读取时为 0 */
    public long sourceTsMs;

    // ---------------- DML ----------------
    /** 列名，与 before/after 下标一一对应 */
    public List<String> columns;
    /** 主键列名 */
    public List<String> primaryKeys;
    public Object[] before;
    public Object[] after;

    // ---------------- DDL ----------------
    /** 原始 DDL 语句 */
    public String ddl;
    /** Debezium 的表变更类型：CREATE / ALTER / DROP，TRUNCATE 时为 TRUNCATE */
    public String ddlType;
    /** 变更后的完整表结构（DROP / TRUNCATE 时为 null） */
    public TableDef tableDef;
    /** RENAME TABLE 时的原库名、原表名，其余情况为 null */
    public String previousDatabase;
    public String previousTable;

    /** 表的唯一标识：库名.表名 */
    public String key() {
        return database + "." + table;
    }

    /**
     * 写入算子的分区 key：按库分区，同一个库的 DDL 与 DML 进入同一个写入实例，严格按 binlog 顺序处理
     * （RENAME TABLE 等 DDL 涉及多张表，按表分区会失去先后顺序）。
     */
    public String partitionKey() {
        return database;
    }

    public boolean isDdl() {
        return op == Op.DDL;
    }

    @Override
    public String toString() {
        if (isDdl()) {
            return "ChangeEvent{DDL " + ddlType + " " + key() + ": " + ddl + "}";
        }
        return "ChangeEvent{" + op + " " + key() + ", columns=" + columns
                + ", before=" + Arrays.toString(before) + ", after=" + Arrays.toString(after) + "}";
    }
}
