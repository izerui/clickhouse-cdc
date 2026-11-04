package com.github.izerui.config;

import com.github.izerui.model.TableDef;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.github.izerui.config.ResourcePlanner.MB;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ResourcePlannerTest {

    private static CdcConfig config(String... keyValues) {
        Map<String, String> m = new HashMap<>();
        m.put("source.hostname", "127.0.0.1");
        m.put("source.username", "root");
        m.put("source.databases", "demo");
        m.put("clickhouse.url", "http://127.0.0.1:8123");
        for (int i = 0; i < keyValues.length; i += 2) {
            m.put(keyValues[i], keyValues[i + 1]);
        }
        return new CdcConfig(m);
    }

    private static TableDef table(long rows, long avgRowLength) {
        TableDef t = new TableDef("demo", "t" + rows + "_" + avgRowLength);
        t.estimatedRows = rows;
        t.avgRowLength = avgRowLength;
        return t;
    }

    /** 一张 1000 万行、平均行长为 avgRow 的大表 */
    private static CdcConfig planned(CdcConfig config, long heapMb, int cpus, long avgRow) {
        return config.withOverrides(ResourcePlanner.plan(config, heapMb * MB, cpus, List.of(table(10_000_000, avgRow))));
    }

    @Test
    void largeMachineUsesUpperBounds() {
        CdcConfig c = planned(config(), 4096, 16, 200);
        assertEquals(4, c.getJobParallelism());
        assertEquals(8096, c.getSourceSplitSize());
        assertEquals(1024, c.getSourceFetchSize());
        assertEquals(512 * MB, c.getSinkBufferMaxTotalBytes());
        assertEquals(64 * MB, c.getSinkBatchMaxBytes());
    }

    @Test
    void smallContainerShrinksEverything() {
        // 1Gi 容器：堆 512MB
        CdcConfig c = planned(config(), 512, 8, 200);
        assertEquals(2, c.getJobParallelism());
        assertTrue(c.getSinkBufferMaxTotalBytes() <= 120 * MB);
        assertTrue(c.getSinkBatchMaxBytes() <= 30 * MB);
        // 并行度 × 分片行数 × 单行内存 不超过可用堆的 30%
        long rowMemory = ResourcePlanner.rowMemory(200);
        assertTrue(c.getJobParallelism() * (long) c.getSourceSplitSize() * rowMemory <= 512 * MB * 3 / 10);
    }

    @Test
    void wideRowsGetSmallerSplits() {
        CdcConfig narrow = planned(config(), 1200, 4, 100);
        CdcConfig wide = planned(config(), 1200, 4, 200_000);
        assertTrue(wide.getSourceSplitSize() < narrow.getSourceSplitSize());
        assertTrue(wide.getSourceSplitSize() >= ResourcePlanner.MIN_SPLIT_SIZE);
        assertTrue(wide.getSourceFetchSize() <= wide.getSourceSplitSize());
    }

    @Test
    void smallTablesDoNotShrinkSplits() {
        // 3 行的小表平均行长统计为 5461（16KB 页 / 3 行），整表放得下，不应影响分片大小
        Map<String, String> resolved = ResourcePlanner.plan(config(), 600 * MB, 1,
                List.of(table(3, 5461), table(1, 16384), table(100_000, 60)));
        assertEquals("8096", resolved.get("source.split-size"));
        // 大的宽表会限制分片大小
        resolved = ResourcePlanner.plan(config(), 600 * MB, 1, List.of(table(3, 5461), table(1_000_000, 50_000)));
        assertTrue(Integer.parseInt(resolved.get("source.split-size")) < 8096);
    }

    @Test
    void parallelismLimitedByCpuAndServerIdRange() {
        assertEquals(2, planned(config(), 4096, 2, 0).getJobParallelism());
        assertEquals(2, planned(config("source.server-id", "5400-5401"), 4096, 16, 0).getJobParallelism());
        assertEquals(1, planned(config("source.server-id", "5400"), 4096, 16, 0).getJobParallelism());
    }

    @Test
    void explicitValuesAreKept() {
        CdcConfig c = config("job.parallelism", "8", "source.split-size", "20000", "sink.batch.max-bytes", "1048576",
                "sink.buffer.max-total-bytes", "auto");
        Map<String, String> resolved = ResourcePlanner.plan(c, 600 * MB, 1, List.of(table(10_000_000, 100_000)));
        assertFalse(resolved.containsKey("job.parallelism"));
        assertFalse(resolved.containsKey("source.split-size"));
        assertFalse(resolved.containsKey("sink.batch.max-bytes"));
        assertTrue(resolved.containsKey("sink.buffer.max-total-bytes"));
        CdcConfig p = c.withOverrides(resolved);
        assertEquals(8, p.getJobParallelism());
        assertEquals(20000, p.getSourceSplitSize());
        assertEquals(1024, p.getSourceFetchSize());
    }

    @Test
    void debeziumQueueShrinksForWideRows() {
        Map<String, String> narrow = ResourcePlanner.plan(config(), 600 * MB, 1, List.of(table(1_000_000, 100)));
        assertEquals("8192", narrow.get(ResourcePlanner.QUEUE_SIZE));
        assertEquals("2048", narrow.get(ResourcePlanner.BATCH_SIZE));
        Map<String, String> wide = ResourcePlanner.plan(config(), 600 * MB, 1, List.of(table(1_000_000, 1_000_000)));
        int queue = Integer.parseInt(wide.get(ResourcePlanner.QUEUE_SIZE));
        int batch = Integer.parseInt(wide.get(ResourcePlanner.BATCH_SIZE));
        assertTrue(queue < 8192 && batch < queue);
        // 显式配置了 Debezium 队列参数时不覆盖
        CdcConfig explicit = config("source.debezium.max.queue.size", "1000");
        assertFalse(ResourcePlanner.plan(explicit, 600 * MB, 1, List.of()).containsKey(ResourcePlanner.QUEUE_SIZE));
    }

    @Test
    void splitSizeAccountsForUnevenChunks() {
        // 分布系数上限越大，主键分布不均的分片可能越大，分片行数相应调小
        CdcConfig loose = config("source.chunk-key.even-distribution.factor.upper-bound", "100");
        int strict = Integer.parseInt(ResourcePlanner.plan(config(), 600 * MB, 1, List.of(table(1_000_000, 1000))).get("source.split-size"));
        int relaxed = Integer.parseInt(ResourcePlanner.plan(loose, 600 * MB, 1, List.of(table(1_000_000, 1000))).get("source.split-size"));
        assertTrue(relaxed < strict);
    }

    @Test
    void serverIdCount() {
        assertEquals(0, ResourcePlanner.serverIdCount(""));
        assertEquals(1, ResourcePlanner.serverIdCount("5400"));
        assertEquals(4, ResourcePlanner.serverIdCount("5400-5403"));
    }
}
