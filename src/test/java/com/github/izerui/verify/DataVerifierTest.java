package com.github.izerui.verify;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DataVerifierTest {

    private static DataVerifier.Result result(String source, long sourceCount, long targetCount) throws Exception {
        Constructor<DataVerifier.Result> c = DataVerifier.Result.class.getDeclaredConstructor(String.class, long.class, long.class);
        return c.newInstance(source, sourceCount, targetCount);
    }

    @Test
    void printCountsMismatches() throws Exception {
        StringBuilder out = new StringBuilder();
        int mismatched = DataVerifier.print(List.of(
                result("demo.a", 10, 10),
                result("demo.b", 10, 8),
                result("demo.c", 5, -1)), out);
        assertEquals(2, mismatched);
        String report = out.toString();
        assertTrue(report.contains("差异 -2"), report);
        assertTrue(report.contains("目标表不存在"), report);
        assertTrue(report.contains("共 3 张表，一致 1 张，不一致 2 张"), report);
    }
}
