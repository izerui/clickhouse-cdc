package com.github.izerui.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * MySQL 表定义（标准化后）。
 */
public class TableDef implements Serializable {

    private static final long serialVersionUID = 1L;

    public String database;
    public String table;
    public String comment;
    public List<ColumnDef> columns = new ArrayList<>();
    /** 主键列，按主键顺序 */
    public List<String> primaryKeys = new ArrayList<>();
    /** 源表平均行长度（字节，information_schema 统计值，用于估算全量阶段的内存占用） */
    public long avgRowLength;
    /** 源表估算行数（information_schema 统计值） */
    public long estimatedRows;

    public TableDef() {
    }

    public TableDef(String database, String table) {
        this.database = database;
        this.table = table;
    }

    public boolean hasPrimaryKey() {
        return primaryKeys != null && !primaryKeys.isEmpty();
    }

    public ColumnDef getColumn(String name) {
        for (ColumnDef c : columns) {
            if (c.name.equalsIgnoreCase(name)) {
                return c;
            }
        }
        return null;
    }

    public String fullName() {
        return database + "." + table;
    }

    @Override
    public String toString() {
        return fullName() + columns + " pk=" + primaryKeys;
    }
}
