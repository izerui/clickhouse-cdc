package com.github.izerui.clickhouse;

import java.io.ByteArrayOutputStream;

/**
 * 可直接读取内部数组的字节缓冲，写出时不必再复制一份（批次可达几十 MB）。
 */
public class ExposedByteArrayOutputStream extends ByteArrayOutputStream {

    public ExposedByteArrayOutputStream(int initialSize) {
        super(initialSize);
    }

    /** 内部数组，有效数据为 [0, size()) */
    public byte[] array() {
        return buf;
    }

    /** 内部数组占用的内存（可能大于 size()） */
    public int capacity() {
        return buf.length;
    }
}
