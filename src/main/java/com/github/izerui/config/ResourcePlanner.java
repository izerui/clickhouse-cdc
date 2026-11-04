package com.github.izerui.config;

import com.github.izerui.model.TableDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 按运行环境（JVM 堆上限、CPU 核数）和源表规模，自动计算与内存相关的参数，让同一个程序在大小不同的
 * 机器 / 容器、大小不同的库上都不容易内存溢出。
 * <p>
 * 只计算配置为 auto（或未配置）的项，显式配置的值保持不变。内存主要花在三处，分别按堆的比例限额：
 * <ul>
 *     <li>全量快照：Flink CDC 读一个分片时整片放在内存中，占用约为 并行度 × 分片行数 × 单行内存；</li>
 *     <li>增量读取：Debezium 事件队列（默认最多 8192 条），宽表、大字段表按行宽缩短；</li>
 *     <li>写入缓冲：所有表的缓冲合计不超过 sink.buffer.max-total-bytes，单表不超过 sink.batch.max-bytes；</li>
 *     <li>其余（Flink 运行时、表结构、网络缓冲、写出时的压缩副本）留作余量。</li>
 * </ul>
 */
public final class ResourcePlanner {

    private static final Logger LOG = LoggerFactory.getLogger(ResourcePlanner.class);

    static final long MB = 1024L * 1024;
    /** Flink 本地集群、Debezium、连接池等的固定开销 */
    static final long BASE_OVERHEAD = 192 * MB;
    /** 每个并行读取实例至少需要的内存（一个 8096 行的普通分片约二三十 MB，留足余量） */
    static final long MEMORY_PER_READER = 128 * MB;
    static final int MAX_AUTO_PARALLELISM = 4;
    static final int MAX_SPLIT_SIZE = 8096;
    static final int MIN_SPLIT_SIZE = 256;
    static final int MAX_QUEUE_SIZE = 8192;
    static final int MIN_QUEUE_SIZE = 256;
    /** 统计信息可信的最小行数：行数很少的表平均行长统计值偏大（一个 16KB 页除以行数） */
    static final long MIN_ROWS_FOR_STATS = 100;
    static final String QUEUE_SIZE = "source.debezium.max.queue.size";
    static final String BATCH_SIZE = "source.debezium.max.batch.size";

    private ResourcePlanner() {
    }

    /**
     * 返回补齐了自动参数的配置，并打印计算结果。
     */
    public static CdcConfig apply(CdcConfig config, List<TableDef> tables) {
        Map<String, String> resolved = plan(config, Runtime.getRuntime().maxMemory(),
                Runtime.getRuntime().availableProcessors(), tables);
        CdcConfig planned = resolved.isEmpty() ? config : config.withOverrides(resolved);
        LOG.info("资源参数（堆上限 {}MB，CPU {} 核，{} 张表）：job.parallelism={}{}，source.split-size={}{}，"
                        + "source.fetch-size={}{}，sink.batch.max-bytes={}MB{}，sink.buffer.max-total-bytes={}MB{}，"
                        + "Debezium 队列 {} 条{}",
                Runtime.getRuntime().maxMemory() / MB, Runtime.getRuntime().availableProcessors(), tables.size(),
                planned.getJobParallelism(), mark(resolved, "job.parallelism"),
                planned.getSourceSplitSize(), mark(resolved, "source.split-size"),
                planned.getSourceFetchSize(), mark(resolved, "source.fetch-size"),
                planned.getSinkBatchMaxBytes() / MB, mark(resolved, "sink.batch.max-bytes"),
                planned.getSinkBufferMaxTotalBytes() / MB, mark(resolved, "sink.buffer.max-total-bytes"),
                planned.getSourceDebeziumProperties().getProperty("max.queue.size", String.valueOf(MAX_QUEUE_SIZE)),
                mark(resolved, QUEUE_SIZE));
        if (Runtime.getRuntime().maxMemory() < 384 * MB) {
            LOG.warn("JVM 堆上限只有 {}MB，建议至少 512MB（容器内存 1Gi 以上）", Runtime.getRuntime().maxMemory() / MB);
        }
        return planned;
    }

    private static String mark(Map<String, String> resolved, String key) {
        return resolved.containsKey(key) ? "(auto)" : "";
    }

    /**
     * 计算配置为 auto 的参数。
     *
     * @param maxHeap   JVM 堆上限
     * @param cpus      可用 CPU 核数（容器中为 CPU limit）
     * @param tables    同步范围内的源表（含行数、平均行长度的统计值）
     */
    static Map<String, String> plan(CdcConfig config, long maxHeap, int cpus, List<TableDef> tables) {
        Map<String, String> resolved = new LinkedHashMap<>();
        long usable = Math.max(maxHeap - BASE_OVERHEAD, maxHeap / 2);

        int parallelism = config.getJobParallelism();
        if (config.isAuto("job.parallelism")) {
            parallelism = (int) Math.min(Math.min(MAX_AUTO_PARALLELISM, Math.max(1, cpus)),
                    Math.max(1, usable / MEMORY_PER_READER));
            // 每个读取实例需要一个独立的 server-id，配置了区间时不能超过区间大小
            int serverIds = serverIdCount(config.getSourceServerId());
            if (serverIds > 0) {
                parallelism = Math.min(parallelism, serverIds);
            }
            resolved.put("job.parallelism", String.valueOf(parallelism));
        }

        long bufferTotal = config.getSinkBufferMaxTotalBytes();
        if (config.isAuto("sink.buffer.max-total-bytes")) {
            bufferTotal = clamp(usable / 5, 8 * MB, 512 * MB);
            resolved.put("sink.buffer.max-total-bytes", String.valueOf(bufferTotal));
        }
        if (config.isAuto("sink.batch.max-bytes")) {
            resolved.put("sink.batch.max-bytes", String.valueOf(clamp(bufferTotal / 4, MB, 64 * MB)));
        }

        int splitSize = config.getSourceSplitSize();
        if (config.isAuto("source.split-size")) {
            // 每个读取实例可用于缓存分片的内存。按主键范围均匀切分时，主键分布不均的分片最多有
            // 分片行数 × 分布系数上限 行；整表都放得下的表不限制分片大小
            long perReader = usable * 3 / 10 / Math.max(1, parallelism);
            double factor = Math.max(1.0, config.getSourceEvenDistributionFactorUpper());
            long limit = MAX_SPLIT_SIZE;
            for (TableDef t : tables) {
                long rowMemory = rowMemory(t.avgRowLength);
                if (Math.max(0, t.estimatedRows) * rowMemory > perReader) {
                    limit = Math.min(limit, (long) (perReader / factor / rowMemory));
                }
            }
            splitSize = (int) clamp(limit, MIN_SPLIT_SIZE, MAX_SPLIT_SIZE);
            resolved.put("source.split-size", String.valueOf(splitSize));
        }
        if (config.isAuto("source.fetch-size")) {
            resolved.put("source.fetch-size", String.valueOf(Math.min(1024, splitSize)));
        }

        // 增量阶段 Debezium 事件队列：可用堆的 10%，按最宽的表估算条数（按字节限制需要逐条计算对象大小，开销大）
        if (config.isAuto(QUEUE_SIZE) && config.isAuto(BATCH_SIZE)) {
            long widest = tables.stream().filter(t -> t.estimatedRows >= MIN_ROWS_FOR_STATS)
                    .mapToLong(t -> t.avgRowLength).max().orElse(0);
            int queueSize = (int) clamp(usable / 10 / rowMemory(widest), MIN_QUEUE_SIZE, MAX_QUEUE_SIZE);
            resolved.put(QUEUE_SIZE, String.valueOf(queueSize));
            // Debezium 要求 max.queue.size > max.batch.size
            resolved.put(BATCH_SIZE, String.valueOf(Math.min(2048, queueSize / 4)));
        }
        return resolved;
    }

    /** 一行在内存中的估算占用：Debezium 记录与 Java 对象的开销远大于行数据本身 */
    static long rowMemory(long avgRowLength) {
        return 2048 + 4 * Math.max(0, avgRowLength);
    }

    /** server-id 配置包含的 id 个数：5400-5403 为 4，单个 id 为 1，未配置为 0 */
    static int serverIdCount(String serverId) {
        if (serverId == null || serverId.isBlank()) {
            return 0;
        }
        String[] range = serverId.trim().split("-");
        if (range.length != 2) {
            return 1;
        }
        return Math.max(1, Integer.parseInt(range[1].trim()) - Integer.parseInt(range[0].trim()) + 1);
    }

    private static long clamp(long value, long min, long max) {
        return Math.max(min, Math.min(max, value));
    }
}
