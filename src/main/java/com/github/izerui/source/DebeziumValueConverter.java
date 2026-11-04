package com.github.izerui.source;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.stream.Collectors;

/**
 * 将 Debezium 输出的字段值转换为与写入端无关的普通 Java 值。
 * <p>
 * 时间语义（与 MySQL 保持一致）：
 * <ul>
 *     <li>DATETIME：Debezium 以“墙上时间当作 UTC”编码，还原为墙上时间；</li>
 *     <li>TIMESTAMP：Debezium 输出 UTC 的 ISO 字符串，转换为 source.server-time-zone 下的墙上时间；</li>
 * </ul>
 * 目标列统一使用 DateTime64(p, 'source.server-time-zone')，因此两者写入后表示的时刻都正确。
 * 超出 ClickHouse Date32/DateTime64 范围（1900-01-01 ~ 2299-12-31）的值会被截断到边界。
 */
public class DebeziumValueConverter implements Serializable {

    private static final long serialVersionUID = 1L;

    static final LocalDate MIN_DATE = LocalDate.of(1900, 1, 1);
    static final LocalDate MAX_DATE = LocalDate.of(2299, 12, 31);
    static final LocalDateTime MIN_DATETIME = MIN_DATE.atStartOfDay();
    static final LocalDateTime MAX_DATETIME = LocalDateTime.of(2299, 12, 31, 23, 59, 59, 999_999_000);

    private final ZoneId serverZone;

    public DebeziumValueConverter(ZoneId serverZone) {
        this.serverZone = serverZone;
    }

    public Object convert(Object value, Schema schema) {
        if (value == null) {
            return null;
        }
        String logical = schema.name();
        if (logical != null) {
            switch (logical) {
                case "io.debezium.time.Date":
                    return formatDate(LocalDate.ofEpochDay(((Number) value).longValue()));
                case "org.apache.kafka.connect.data.Date":
                    return formatDate(LocalDate.ofInstant(((java.util.Date) value).toInstant(), ZoneOffset.UTC));
                case "io.debezium.time.Timestamp":
                    return formatDateTime(epochNanosToUtc(((Number) value).longValue(), 1_000_000L));
                case "org.apache.kafka.connect.data.Timestamp":
                    return formatDateTime(LocalDateTime.ofInstant(((java.util.Date) value).toInstant(), ZoneOffset.UTC));
                case "io.debezium.time.MicroTimestamp":
                    return formatDateTime(epochNanosToUtc(((Number) value).longValue(), 1_000L));
                case "io.debezium.time.NanoTimestamp":
                    return formatDateTime(epochNanosToUtc(((Number) value).longValue(), 1L));
                case "io.debezium.time.ZonedTimestamp":
                    return formatDateTime(OffsetDateTime.parse(value.toString())
                            .atZoneSameInstant(serverZone).toLocalDateTime());
                case "io.debezium.time.Time":
                    return formatTime(((Number) value).longValue() * 1_000_000L);
                case "io.debezium.time.MicroTime":
                    return formatTime(((Number) value).longValue() * 1_000L);
                case "io.debezium.time.NanoTime":
                    return formatTime(((Number) value).longValue());
                case "org.apache.kafka.connect.data.Time":
                    return formatTime(((java.util.Date) value).getTime() * 1_000_000L);
                case "org.apache.kafka.connect.data.Decimal":
                    return toDecimal(value, schema);
                case "io.debezium.data.VariableScaleDecimal": {
                    Struct s = (Struct) value;
                    return new BigDecimal(new BigInteger(toBytes(s.get("value"))), s.getInt32("scale"));
                }
                case "io.debezium.data.Bits":
                    return bitsToNumber(toBytes(value));
                case "io.debezium.data.geometry.Geometry":
                case "io.debezium.data.geometry.Geography":
                case "io.debezium.data.geometry.Point": {
                    // 与 ClickHouse 官方 MySQL CDC（ClickPipes）一致，空间类型以 WKT 文本保存
                    Object wkb = ((Struct) value).get("wkb");
                    return wkb == null ? null : WkbToWkt.convert(toBytes(wkb));
                }
                case "io.debezium.data.Json":
                case "io.debezium.data.Enum":
                case "io.debezium.data.EnumSet":
                case "io.debezium.data.Xml":
                    return value.toString();
                default:
                    break;
            }
        }
        switch (schema.type()) {
            case BYTES:
                return toBytes(value);
            case ARRAY:
                return ((Collection<?>) value).stream().map(String::valueOf).collect(Collectors.joining(","));
            case STRUCT:
            case MAP:
                return value.toString();
            default:
                // BOOLEAN / INT8~INT64 / FLOAT32 / FLOAT64 / STRING
                return value;
        }
    }

    private static LocalDateTime epochNanosToUtc(long amount, long nanosPerUnit) {
        long unitsPerSecond = 1_000_000_000L / nanosPerUnit;
        long seconds = Math.floorDiv(amount, unitsPerSecond);
        long nanos = Math.floorMod(amount, unitsPerSecond) * nanosPerUnit;
        return LocalDateTime.ofInstant(Instant.ofEpochSecond(seconds, nanos), ZoneOffset.UTC);
    }

    private static BigDecimal toDecimal(Object value, Schema schema) {
        if (value instanceof BigDecimal) {
            return (BigDecimal) value;
        }
        int scale = Integer.parseInt(schema.parameters() == null ? "0" : schema.parameters().getOrDefault("scale", "0"));
        return new BigDecimal(new BigInteger(toBytes(value)), scale);
    }

    static byte[] toBytes(Object value) {
        if (value instanceof byte[]) {
            return (byte[]) value;
        }
        if (value instanceof ByteBuffer) {
            ByteBuffer buffer = ((ByteBuffer) value).duplicate();
            byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            return bytes;
        }
        throw new IllegalArgumentException("无法转换为字节数组: " + value.getClass());
    }

    /** BIT(n)：Debezium 以小端字节序输出 */
    private static Number bitsToNumber(byte[] bytes) {
        BigInteger result = BigInteger.ZERO;
        for (int i = bytes.length - 1; i >= 0; i--) {
            result = result.shiftLeft(8).or(BigInteger.valueOf(bytes[i] & 0xFF));
        }
        return result.bitLength() < 64 ? (Number) result.longValue() : result;
    }

    static String formatDate(LocalDate date) {
        if (date.isBefore(MIN_DATE)) {
            date = MIN_DATE;
        } else if (date.isAfter(MAX_DATE)) {
            date = MAX_DATE;
        }
        return date.toString();
    }

    /** 输出 yyyy-MM-dd HH:mm:ss[.SSS|.SSSSSS]，ClickHouse 默认解析格式 */
    static String formatDateTime(LocalDateTime dt) {
        if (dt.isBefore(MIN_DATETIME)) {
            dt = MIN_DATETIME;
        } else if (dt.isAfter(MAX_DATETIME)) {
            dt = MAX_DATETIME;
        }
        StringBuilder sb = new StringBuilder(26);
        sb.append(dt.toLocalDate()).append(' ');
        pad2(sb, dt.getHour()).append(':');
        pad2(sb, dt.getMinute()).append(':');
        pad2(sb, dt.getSecond());
        appendFraction(sb, dt.getNano());
        return sb.toString();
    }

    /** MySQL TIME 范围为 -838:59:59 ~ 838:59:59，ClickHouse 端以字符串保存 */
    static String formatTime(long nanos) {
        StringBuilder sb = new StringBuilder(16);
        if (nanos < 0) {
            sb.append('-');
            nanos = -nanos;
        }
        long totalSeconds = nanos / 1_000_000_000L;
        long hours = totalSeconds / 3600;
        if (hours < 10) {
            sb.append('0');
        }
        sb.append(hours).append(':');
        pad2(sb, (int) (totalSeconds / 60 % 60)).append(':');
        pad2(sb, (int) (totalSeconds % 60));
        appendFraction(sb, (int) (nanos % 1_000_000_000L));
        return sb.toString();
    }

    private static StringBuilder pad2(StringBuilder sb, int v) {
        if (v < 10) {
            sb.append('0');
        }
        return sb.append(v);
    }

    private static void appendFraction(StringBuilder sb, int nanos) {
        if (nanos == 0) {
            return;
        }
        if (nanos % 1_000_000 == 0) {
            sb.append('.').append(String.format("%03d", nanos / 1_000_000));
        } else {
            sb.append('.').append(String.format("%06d", nanos / 1_000));
        }
    }
}
