package com.github.izerui.model;

import java.io.Serializable;
import java.util.Objects;

/**
 * MySQL 列定义（标准化后），来源可以是 information_schema 或 Debezium 的 DDL 事件。
 */
public class ColumnDef implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 列名 */
    public String name;
    /** 基础类型，小写，如 int、varchar、decimal、datetime */
    public String dataType;
    /** 是否 UNSIGNED */
    public boolean unsigned;
    /** 数值精度 / 字符长度 / 时间小数位，未知时为 null */
    public Integer length;
    /** 小数位数，未知时为 null */
    public Integer scale;
    /** 是否允许 NULL */
    public boolean nullable;
    /** 列注释 */
    public String comment;
    /** 默认值字面量（不含引号）；没有默认值、默认为 NULL 或为表达式（如 CURRENT_TIMESTAMP）时为 null */
    public String defaultValue;

    public ColumnDef() {
    }

    public ColumnDef(String name, String dataType, boolean unsigned, Integer length, Integer scale, boolean nullable, String comment) {
        this.name = name;
        this.dataType = dataType;
        this.unsigned = unsigned;
        this.length = length;
        this.scale = scale;
        this.nullable = nullable;
        this.comment = comment;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ColumnDef)) return false;
        ColumnDef that = (ColumnDef) o;
        return unsigned == that.unsigned && nullable == that.nullable && Objects.equals(name, that.name)
                && Objects.equals(dataType, that.dataType) && Objects.equals(length, that.length)
                && Objects.equals(scale, that.scale) && Objects.equals(comment, that.comment)
                && Objects.equals(defaultValue, that.defaultValue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, dataType, unsigned, length, scale, nullable, comment, defaultValue);
    }

    @Override
    public String toString() {
        return name + " " + dataType + (length != null ? "(" + length + (scale != null ? "," + scale : "") + ")" : "")
                + (unsigned ? " unsigned" : "") + (nullable ? " null" : " not null")
                + (defaultValue != null ? " default '" + defaultValue + "'" : "");
    }
}
