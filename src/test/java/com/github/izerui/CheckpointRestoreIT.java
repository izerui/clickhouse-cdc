package com.github.izerui;

import com.github.izerui.config.CdcConfig;
import com.github.izerui.config.CheckpointLocator;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证 java -jar 内嵌 Flink 的续传链路：本地环境写 checkpoint → 进程“重启” → 定位最新 checkpoint → 从该位置继续。
 * 用 NumberSequenceSource 代替 MySQL CDC 源（同为 FLIP-27 Source，读取进度同样保存在 checkpoint 中）。
 */
class CheckpointRestoreIT {

    static final ConcurrentLinkedQueue<Long> OUTPUT = new ConcurrentLinkedQueue<>();

    private static CdcConfig config(Path checkpointDir) {
        return new CdcConfig(Map.of(
                "source.hostname", "127.0.0.1",
                "source.username", "root",
                "source.databases", "demo",
                "clickhouse.url", "http://127.0.0.1:8123",
                "job.parallelism", "1",
                "checkpoint.dir", checkpointDir.toString(),
                "checkpoint.interval-ms", "200"));
    }

    private static JobClient start(CdcConfig config, Path checkpointDir, Path restorePath) throws Exception {
        StreamExecutionEnvironment env = CdcApplication.createEnvironment(config, checkpointDir, restorePath);
        env.fromSource(new NumberSequenceSource(0, Long.MAX_VALUE), WatermarkStrategy.noWatermarks(), "numbers")
                .uid("mysql-cdc-source")
                .map(v -> {
                    Thread.sleep(2);
                    return v;
                })
                .addSink(new SinkFunction<Long>() {
                    @Override
                    public void invoke(Long value, Context context) {
                        OUTPUT.add(value);
                    }
                });
        return env.executeAsync("restore-test");
    }

    @Test
    void resumesFromLatestCheckpoint(@TempDir Path root) throws Exception {
        Path checkpointDir = root.resolve("checkpoints");
        CdcConfig config = config(checkpointDir);

        // 第一次运行：等到至少产生一个完整 checkpoint 后取消（模拟进程被杀）
        JobClient first = start(config, checkpointDir, null);
        Optional<Path> latest = Optional.empty();
        for (int i = 0; i < 100 && latest.isEmpty(); i++) {
            Thread.sleep(200);
            latest = CheckpointLocator.findLatest(checkpointDir);
        }
        Thread.sleep(500);
        first.cancel().get(30, TimeUnit.SECONDS);
        latest = CheckpointLocator.findLatest(checkpointDir);
        assertTrue(latest.isPresent(), "应保留 checkpoint（RETAIN_ON_CANCELLATION）");
        long maxBeforeRestart = OUTPUT.stream().mapToLong(Long::longValue).max().orElse(-1);
        assertTrue(maxBeforeRestart > 0);

        // 第二次运行：从最新 checkpoint 恢复
        OUTPUT.clear();
        JobClient second = start(config, checkpointDir, latest.get());
        for (int i = 0; i < 100 && OUTPUT.isEmpty(); i++) {
            Thread.sleep(100);
        }
        second.cancel().get(30, TimeUnit.SECONDS);
        long firstAfterRestore = OUTPUT.stream().mapToLong(Long::longValue).min().orElse(-1);

        // 从 checkpoint 位置继续：不会从 0 开始，且不晚于上次处理到的位置（至少一次，可能有少量重放）
        assertTrue(firstAfterRestore > 0, "恢复后应从 checkpoint 位置继续，而不是从头开始，实际从 " + firstAfterRestore + " 开始");
        assertTrue(firstAfterRestore <= maxBeforeRestart + 1, "不应跳过数据: 恢复起点 " + firstAfterRestore + "，上次处理到 " + maxBeforeRestart);
        System.out.println("上次处理到 " + maxBeforeRestart + "，恢复后从 " + firstAfterRestore + " 继续");
    }
}
