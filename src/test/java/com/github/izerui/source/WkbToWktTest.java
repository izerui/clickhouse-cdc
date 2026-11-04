package com.github.izerui.source;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.junit.jupiter.api.Assertions.assertEquals;

class WkbToWktTest {

    /** 构造 WKB：order + type + 坐标/计数 */
    private static ByteBuffer header(ByteBuffer b, int type) {
        b.put((byte) (b.order() == ByteOrder.LITTLE_ENDIAN ? 1 : 0));
        b.putInt(type);
        return b;
    }

    private static byte[] point(double x, double y, ByteOrder order) {
        ByteBuffer b = ByteBuffer.allocate(21).order(order);
        header(b, 1).putDouble(x).putDouble(y);
        return b.array();
    }

    @Test
    void point() {
        assertEquals("POINT(1 2)", WkbToWkt.convert(point(1, 2, ByteOrder.LITTLE_ENDIAN)));
        assertEquals("POINT(116.397128 39.916527)", WkbToWkt.convert(point(116.397128, 39.916527, ByteOrder.BIG_ENDIAN)));
        assertEquals("POINT(-0.5 1000000)", WkbToWkt.convert(point(-0.5, 1e6, ByteOrder.LITTLE_ENDIAN)));
        assertEquals("POINT EMPTY", WkbToWkt.convert(point(Double.NaN, Double.NaN, ByteOrder.LITTLE_ENDIAN)));
    }

    @Test
    void lineStringAndPolygon() {
        ByteBuffer line = ByteBuffer.allocate(9 + 4 + 32).order(ByteOrder.LITTLE_ENDIAN);
        header(line, 2).putInt(2).putDouble(0).putDouble(0).putDouble(1).putDouble(1);
        assertEquals("LINESTRING(0 0,1 1)", WkbToWkt.convert(line.array()));

        ByteBuffer polygon = ByteBuffer.allocate(9 + 4 + 4 + 64).order(ByteOrder.LITTLE_ENDIAN);
        header(polygon, 3).putInt(1).putInt(4)
                .putDouble(0).putDouble(0).putDouble(1).putDouble(0).putDouble(1).putDouble(1).putDouble(0).putDouble(0);
        assertEquals("POLYGON((0 0,1 0,1 1,0 0))", WkbToWkt.convert(polygon.array()));
    }

    @Test
    void multiAndCollection() {
        byte[] p1 = point(1, 1, ByteOrder.LITTLE_ENDIAN);
        byte[] p2 = point(2, 2, ByteOrder.BIG_ENDIAN);
        ByteBuffer multi = ByteBuffer.allocate(9 + p1.length + p2.length).order(ByteOrder.LITTLE_ENDIAN);
        header(multi, 4).putInt(2).put(p1).put(p2);
        assertEquals("MULTIPOINT((1 1),(2 2))", WkbToWkt.convert(multi.array()));

        ByteBuffer collection = ByteBuffer.allocate(9 + p1.length).order(ByteOrder.LITTLE_ENDIAN);
        header(collection, 7).putInt(1).put(p1);
        assertEquals("GEOMETRYCOLLECTION(POINT(1 1))", WkbToWkt.convert(collection.array()));

        ByteBuffer empty = ByteBuffer.allocate(9).order(ByteOrder.LITTLE_ENDIAN);
        header(empty, 7).putInt(0);
        assertEquals("GEOMETRYCOLLECTION EMPTY", WkbToWkt.convert(empty.array()));
    }
}
