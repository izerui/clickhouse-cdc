package com.github.izerui.source;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * 将标准 WKB（Well-Known Binary）转换为 WKT 文本，与 MySQL ST_AsText 的输出格式一致，
 * 如 POINT(1 2)、LINESTRING(0 0,1 1)、POLYGON((0 0,1 0,1 1,0 0))。
 * <p>
 * MySQL 空间类型均为二维；Debezium 输出的 wkb 已去掉 MySQL 内部的 4 字节 SRID 前缀。
 */
public final class WkbToWkt {

    private WkbToWkt() {
    }

    public static String convert(byte[] wkb) {
        StringBuilder sb = new StringBuilder();
        ByteBuffer buf = ByteBuffer.wrap(wkb);
        readGeometry(buf, sb);
        return sb.toString();
    }

    private static void readGeometry(ByteBuffer buf, StringBuilder sb) {
        buf.order(buf.get() == 0 ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN);
        int type = buf.getInt();
        switch (type) {
            case 1:
                sb.append("POINT");
                double x = buf.getDouble();
                double y = buf.getDouble();
                if (Double.isNaN(x) && Double.isNaN(y)) {
                    sb.append(" EMPTY");
                } else {
                    sb.append('(');
                    appendCoordinate(sb, x, y);
                    sb.append(')');
                }
                break;
            case 2:
                sb.append("LINESTRING");
                appendPoints(buf, sb);
                break;
            case 3:
                sb.append("POLYGON");
                appendRings(buf, sb);
                break;
            case 4:
            case 5:
            case 6:
            case 7: {
                sb.append(type == 4 ? "MULTIPOINT" : type == 5 ? "MULTILINESTRING" : type == 6 ? "MULTIPOLYGON" : "GEOMETRYCOLLECTION");
                int count = buf.getInt();
                if (count == 0) {
                    sb.append(" EMPTY");
                    break;
                }
                sb.append('(');
                for (int i = 0; i < count; i++) {
                    if (i > 0) {
                        sb.append(',');
                    }
                    if (type == 7) {
                        readGeometry(buf, sb);
                    } else {
                        // 子几何对象：去掉类型名，只保留坐标部分
                        StringBuilder child = new StringBuilder();
                        readGeometry(buf, child);
                        sb.append(child.substring(child.indexOf("(")));
                    }
                }
                sb.append(')');
                break;
            }
            default:
                throw new IllegalArgumentException("不支持的 WKB 几何类型: " + type);
        }
    }

    private static void appendPoints(ByteBuffer buf, StringBuilder sb) {
        int count = buf.getInt();
        if (count == 0) {
            sb.append(" EMPTY");
            return;
        }
        sb.append('(');
        for (int i = 0; i < count; i++) {
            if (i > 0) {
                sb.append(',');
            }
            appendCoordinate(sb, buf.getDouble(), buf.getDouble());
        }
        sb.append(')');
    }

    private static void appendRings(ByteBuffer buf, StringBuilder sb) {
        int rings = buf.getInt();
        if (rings == 0) {
            sb.append(" EMPTY");
            return;
        }
        sb.append('(');
        for (int i = 0; i < rings; i++) {
            if (i > 0) {
                sb.append(',');
            }
            appendPoints(buf, sb);
        }
        sb.append(')');
    }

    private static void appendCoordinate(StringBuilder sb, double x, double y) {
        sb.append(format(x)).append(' ').append(format(y));
    }

    /** 整数不带小数点，其余去掉末尾的 0，避免科学计数法 */
    private static String format(double d) {
        if (d == Math.rint(d) && Math.abs(d) < 1e15) {
            return Long.toString((long) d);
        }
        return BigDecimal.valueOf(d).stripTrailingZeros().toPlainString();
    }
}
