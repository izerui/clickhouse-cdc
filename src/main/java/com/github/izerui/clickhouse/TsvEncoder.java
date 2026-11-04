package com.github.izerui.clickhouse;

import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;

/**
 * ClickHouse TabSeparated 格式编码。
 * <p>
 * 转义规则见 https://clickhouse.com/docs/en/interfaces/formats#tabseparated ：
 * 反斜杠、制表符、换行等特殊字节用反斜杠转义，NULL 写作 \N，其余字节原样输出（因此二进制数据也能无损写入）。
 */
public final class TsvEncoder {

    private static final byte[] NULL = {'\\', 'N'};

    private TsvEncoder() {
    }

    public static void writeValue(ByteArrayOutputStream out, Object value) {
        if (value == null) {
            out.write(NULL, 0, NULL.length);
        } else if (value instanceof String) {
            writeEscaped(out, ((String) value).getBytes(StandardCharsets.UTF_8));
        } else if (value instanceof byte[]) {
            writeEscaped(out, (byte[]) value);
        } else if (value instanceof Boolean) {
            // 1/0 同时兼容 Bool 与整数列
            out.write((Boolean) value ? '1' : '0');
        } else if (value instanceof BigDecimal) {
            writeAscii(out, ((BigDecimal) value).toPlainString());
        } else if (value instanceof Float) {
            float f = (Float) value;
            writeAscii(out, Float.isNaN(f) || Float.isInfinite(f) ? formatFloat(f) : Float.toString(f));
        } else if (value instanceof Double) {
            writeAscii(out, formatFloat((Double) value));
        } else if (value instanceof Number) {
            writeAscii(out, value.toString());
        } else {
            writeEscaped(out, value.toString().getBytes(StandardCharsets.UTF_8));
        }
    }

    public static void writeTab(ByteArrayOutputStream out) {
        out.write('\t');
    }

    public static void writeNewLine(ByteArrayOutputStream out) {
        out.write('\n');
    }

    private static String formatFloat(double d) {
        if (Double.isNaN(d)) {
            return "nan";
        }
        if (Double.isInfinite(d)) {
            return d > 0 ? "inf" : "-inf";
        }
        return Double.toString(d);
    }

    private static void writeAscii(ByteArrayOutputStream out, String s) {
        for (int i = 0; i < s.length(); i++) {
            out.write(s.charAt(i));
        }
    }

    private static void writeEscaped(ByteArrayOutputStream out, byte[] bytes) {
        for (byte b : bytes) {
            switch (b) {
                case '\\':
                    out.write('\\');
                    out.write('\\');
                    break;
                case '\t':
                    out.write('\\');
                    out.write('t');
                    break;
                case '\n':
                    out.write('\\');
                    out.write('n');
                    break;
                case '\r':
                    out.write('\\');
                    out.write('r');
                    break;
                case 0:
                    out.write('\\');
                    out.write('0');
                    break;
                case '\b':
                    out.write('\\');
                    out.write('b');
                    break;
                case '\f':
                    out.write('\\');
                    out.write('f');
                    break;
                default:
                    out.write(b);
            }
        }
    }

    /** 反转义查询结果中的单元格 */
    public static String unescape(String cell) {
        if (cell.indexOf('\\') < 0) {
            return cell;
        }
        StringBuilder sb = new StringBuilder(cell.length());
        for (int i = 0; i < cell.length(); i++) {
            char c = cell.charAt(i);
            if (c != '\\' || i == cell.length() - 1) {
                sb.append(c);
                continue;
            }
            char n = cell.charAt(++i);
            switch (n) {
                case 't':
                    sb.append('\t');
                    break;
                case 'n':
                    sb.append('\n');
                    break;
                case 'r':
                    sb.append('\r');
                    break;
                case '0':
                    sb.append('\0');
                    break;
                case 'b':
                    sb.append('\b');
                    break;
                case 'f':
                    sb.append('\f');
                    break;
                default:
                    sb.append(n);
            }
        }
        return sb.toString();
    }
}
