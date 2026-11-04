package com.github.izerui.clickhouse;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class TsvEncoderTest {

    private static String encode(Object value) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        TsvEncoder.writeValue(out, value);
        return out.toString(StandardCharsets.UTF_8);
    }

    @Test
    void escapesSpecialCharacters() {
        assertEquals("a\\tb\\nc\\\\d\\re'", encode("a\tb\nc\\d\re'"));
        assertEquals("中文", encode("中文"));
        assertEquals("\\N", encode(null));
    }

    @Test
    void numbersAndBooleans() {
        assertEquals("1", encode(true));
        assertEquals("0", encode(false));
        assertEquals("0.00000001", encode(new BigDecimal("1E-8")));
        assertEquals("nan", encode(Double.NaN));
        assertEquals("-inf", encode(Float.NEGATIVE_INFINITY));
        assertEquals("123", encode(123L));
        assertEquals("1.1", encode(1.1f));
        assertEquals("1.1", encode(1.1d));
    }

    @Test
    void binaryIsWrittenRaw() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        TsvEncoder.writeValue(out, new byte[]{(byte) 0xFF, 0, '\n'});
        assertArrayEquals(new byte[]{(byte) 0xFF, '\\', '0', '\\', 'n'}, out.toByteArray());
    }

    @Test
    void unescape() {
        assertEquals("a\tb\nc\\d", TsvEncoder.unescape("a\\tb\\nc\\\\d"));
        assertEquals("it's", TsvEncoder.unescape("it\\'s"));
    }
}
