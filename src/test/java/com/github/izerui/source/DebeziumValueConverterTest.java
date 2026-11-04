package com.github.izerui.source;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class DebeziumValueConverterTest {

    private final DebeziumValueConverter converter = new DebeziumValueConverter(ZoneId.of("Asia/Shanghai"));

    private static Schema logical(Schema.Type type, String name) {
        return new SchemaBuilder(type).name(name).build();
    }

    @Test
    void date() {
        Schema s = logical(Schema.Type.INT32, "io.debezium.time.Date");
        assertEquals("2024-02-29", converter.convert((int) LocalDate.of(2024, 2, 29).toEpochDay(), s));
        assertEquals("1900-01-01", converter.convert((int) LocalDate.of(1000, 1, 1).toEpochDay(), s));
        assertEquals("2299-12-31", converter.convert((int) LocalDate.of(9999, 12, 31).toEpochDay(), s));
    }

    @Test
    void datetimeKeepsWallClock() {
        long millis = LocalDateTime.of(2024, 5, 1, 10, 20, 30, 123_000_000).toInstant(ZoneOffset.UTC).toEpochMilli();
        assertEquals("2024-05-01 10:20:30.123", converter.convert(millis, logical(Schema.Type.INT64, "io.debezium.time.Timestamp")));
        long micros = LocalDateTime.of(2024, 5, 1, 10, 20, 30, 123_456_000).toEpochSecond(ZoneOffset.UTC) * 1_000_000 + 123_456;
        assertEquals("2024-05-01 10:20:30.123456", converter.convert(micros, logical(Schema.Type.INT64, "io.debezium.time.MicroTimestamp")));
        long noFraction = LocalDateTime.of(2024, 5, 1, 0, 0).toInstant(ZoneOffset.UTC).toEpochMilli();
        assertEquals("2024-05-01 00:00:00", converter.convert(noFraction, logical(Schema.Type.INT64, "io.debezium.time.Timestamp")));
        long beforeEpoch = LocalDateTime.of(1960, 1, 1, 0, 0, 0, 500_000_000).toInstant(ZoneOffset.UTC).toEpochMilli();
        assertEquals("1960-01-01 00:00:00.500", converter.convert(beforeEpoch, logical(Schema.Type.INT64, "io.debezium.time.Timestamp")));
    }

    @Test
    void timestampConvertedToServerZone() {
        Schema s = logical(Schema.Type.STRING, "io.debezium.time.ZonedTimestamp");
        assertEquals("2024-05-01 18:00:00", converter.convert("2024-05-01T10:00:00Z", s));
        assertEquals("2024-05-01 18:00:00.120", converter.convert("2024-05-01T10:00:00.12Z", s));
    }

    @Test
    void time() {
        Schema s = logical(Schema.Type.INT64, "io.debezium.time.MicroTime");
        assertEquals("13:05:09", converter.convert((13 * 3600 + 5 * 60 + 9) * 1_000_000L, s));
        assertEquals("838:59:59", converter.convert((838 * 3600 + 59 * 60 + 59) * 1_000_000L, s));
        assertEquals("-01:00:00.000001", converter.convert(-3600 * 1_000_000L - 1, s));
    }

    @Test
    void decimals() {
        Schema decimal = Decimal.schema(2);
        assertEquals(new BigDecimal("12.34"), converter.convert(new BigDecimal("12.34"), decimal));
        assertEquals(new BigDecimal("12.34"), converter.convert(new BigInteger("1234").toByteArray(), decimal));

        Schema vsd = SchemaBuilder.struct().name("io.debezium.data.VariableScaleDecimal")
                .field("scale", Schema.INT32_SCHEMA).field("value", Schema.BYTES_SCHEMA).build();
        Struct v = new Struct(vsd).put("scale", 3).put("value", new BigInteger("-12345").toByteArray());
        assertEquals(new BigDecimal("-12.345"), converter.convert(v, vsd));
    }

    @Test
    void bitsAndBytes() {
        Schema bits = logical(Schema.Type.BYTES, "io.debezium.data.Bits");
        assertEquals(258L, converter.convert(new byte[]{0x02, 0x01}, bits));
        byte[] data = {0, 1, (byte) 0xFF, '\t'};
        assertArrayEquals(data, (byte[]) converter.convert(ByteBuffer.wrap(data), Schema.BYTES_SCHEMA));
    }

    @Test
    void geometryAsWkt() {
        Schema geometry = SchemaBuilder.struct().name("io.debezium.data.geometry.Point")
                .field("wkb", Schema.BYTES_SCHEMA).field("srid", Schema.OPTIONAL_INT32_SCHEMA).build();
        java.nio.ByteBuffer wkb = java.nio.ByteBuffer.allocate(21).order(java.nio.ByteOrder.LITTLE_ENDIAN);
        wkb.put((byte) 1).putInt(1).putDouble(1.5).putDouble(2);
        assertEquals("POINT(1.5 2)", converter.convert(new Struct(geometry).put("wkb", wkb.array()), geometry));
    }

    @Test
    void plainValuesPassThrough() {
        assertEquals(5, converter.convert(5, Schema.INT32_SCHEMA));
        assertEquals("x", converter.convert("x", Schema.STRING_SCHEMA));
        assertEquals("[\"a\"]", converter.convert("[\"a\"]", logical(Schema.Type.STRING, "io.debezium.data.Json")));
        assertNull(converter.convert(null, Schema.STRING_SCHEMA));
    }
}
