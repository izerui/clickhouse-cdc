package com.github.izerui.source;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TableFilterTest {

    @Test
    void acceptsByIncludeAndExclude() {
        TableFilter filter = new TableFilter("order_.*|user", "order_tmp.*", List.of("shop.order_nopk"));
        assertTrue(filter.accept("shop", "order_item"));
        assertTrue(filter.accept("shop", "user"));
        assertFalse(filter.accept("shop", "user_role"));
        assertFalse(filter.accept("shop", "order_tmp_1"));
        assertFalse(filter.accept("shop", "order_nopk"));
        assertTrue(filter.accept("other", "order_nopk"));
    }

    @Test
    void cdcRegexMatchesSameTablesAsFilter() {
        TableFilter filter = new TableFilter("order_.*|user", "order_tmp.*", List.of("shop.order_nopk", "other.x"));
        String regex = filter.toCdcTableRegex("shop");
        Pattern p = Pattern.compile(regex);
        for (String table : List.of("order_item", "user", "user_role", "order_tmp_1", "order_nopk", "order_nopk2")) {
            assertEquals(filter.accept("shop", table), p.matcher("shop." + table).matches(), table);
        }
        assertFalse(p.matcher("shopx.user").matches());
    }

    @Test
    void caseInsensitiveLikeDebezium() {
        TableFilter filter = new TableFilter("Order_.*", "", List.of("shop.NoPk"));
        assertTrue(filter.accept("shop", "order_item"));
        assertFalse(filter.accept("shop", "nopk"));
    }

    @Test
    void tableListOnly() {
        TableFilter filter = new TableFilter(List.of("t_user", "shop.t_order", "T_Tmp"), "ignored_by_list", "t_tmp", List.of());
        assertTrue(filter.accept("demo", "t_user"));
        assertTrue(filter.accept("shop", "t_user"));
        assertTrue(filter.accept("shop", "t_order"));
        assertFalse(filter.accept("demo", "t_order"), "带库名的只匹配该库");
        assertFalse(filter.accept("demo", "t_other"), "未列出的表不同步");
        assertFalse(filter.accept("demo", "t_tmp"), "exclude 仍然生效");
        assertEquals(List.of("t_user", "T_Tmp"), filter.listedTables("demo"));
        assertEquals(List.of("t_user", "t_order", "T_Tmp"), filter.listedTables("shop"));
    }

    @Test
    void tableListCdcRegexMatchesSameTables() {
        TableFilter filter = new TableFilter(List.of("t_user", "shop.t_order", "a.b"), ".*", "", List.of("demo.t_user"));
        for (String db : List.of("demo", "shop", "other")) {
            Pattern p = Pattern.compile(filter.toCdcTableRegex(db), Pattern.CASE_INSENSITIVE);
            for (String table : List.of("t_user", "t_order", "t_other", "b")) {
                assertEquals(filter.accept(db, table), p.matcher(db + "." + table).matches(), db + "." + table);
            }
        }
        // 某个库没有指定任何表时不匹配任何表
        assertFalse(Pattern.compile(new TableFilter(List.of("shop.t_order"), ".*", "", null).toCdcTableRegex("demo"))
                .matcher("demo.t_order").matches());
    }

    @Test
    void defaultIncludesEverything() {
        TableFilter filter = new TableFilter("", "", null);
        assertEquals("\\Qdemo\\E\\.(?:.*)", filter.toCdcTableRegex("demo"));
        assertTrue(filter.accept("demo", "anything"));
    }

    @Test
    void rejectsCommaInRegex() {
        assertThrows(IllegalArgumentException.class, () -> new TableFilter("a{1,2}", "", null));
    }
}
