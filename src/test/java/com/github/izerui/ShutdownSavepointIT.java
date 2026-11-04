package com.github.izerui;

import com.github.izerui.config.CdcConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证进程收到 SIGTERM 时，关闭钩子能在 Flink 自身关闭前完成 stop-with-savepoint。
 */
class ShutdownSavepointIT {

    /** 子进程入口：与 CdcApplication 相同的环境与关闭钩子 */
    public static void main(String[] args) throws Exception {
        Path checkpointDir = Paths.get(args[0]);
        CdcConfig config = new CdcConfig(Map.of(
                "source.hostname", "127.0.0.1",
                "source.username", "root",
                "source.databases", "demo",
                "clickhouse.url", "http://127.0.0.1:8123",
                "job.parallelism", "1",
                "checkpoint.interval-ms", "500"));
        StreamExecutionEnvironment env = CdcApplication.createEnvironment(config, checkpointDir, null);
        env.fromSource(new NumberSequenceSource(0, Long.MAX_VALUE), WatermarkStrategy.noWatermarks(), "numbers")
                .map(v -> {
                    Thread.sleep(1);
                    return v;
                })
                .addSink(new DiscardingSink<>());
        JobClient client = env.executeAsync("shutdown-test");
        Runtime.getRuntime().addShutdownHook(new Thread(
                () -> CdcApplication.stopWithSavepoint(client, checkpointDir.resolve("savepoints"))));
        System.out.println("STARTED");
        client.getJobExecutionResult().get();
    }

    @Test
    void savepointOnSigterm(@TempDir Path root) throws Exception {
        Path checkpointDir = root.resolve("checkpoints");
        Process process = new ProcessBuilder(
                Paths.get(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", System.getProperty("java.class.path"),
                ShutdownSavepointIT.class.getName(), checkpointDir.toString())
                .redirectErrorStream(true)
                .redirectOutput(root.resolve("child.log").toFile())
                .start();
        try {
            // 等作业稳定运行（已产生 checkpoint）后发送 SIGTERM
            for (int i = 0; i < 150 && !hasMetadata(checkpointDir, "chk-"); i++) {
                Thread.sleep(200);
            }
            assertTrue(hasMetadata(checkpointDir, "chk-"), "作业未正常运行，日志: " + root.resolve("child.log"));
            process.destroy();
            assertTrue(process.waitFor(120, TimeUnit.SECONDS), "进程未在 120 秒内退出");
        } finally {
            process.destroyForcibly();
        }
        String log = Files.readString(root.resolve("child.log"));
        assertTrue(hasMetadata(checkpointDir.resolve("savepoints"), "savepoint-"),
                "收到 SIGTERM 后应生成 savepoint，子进程日志:\n" + tail(log));
        System.out.println("子进程日志尾部:\n" + tail(log));
    }

    private static boolean hasMetadata(Path dir, String prefix) throws Exception {
        if (!Files.isDirectory(dir)) {
            return false;
        }
        try (Stream<Path> files = Files.walk(dir, 3)) {
            return files.anyMatch(p -> p.getFileName().toString().equals("_metadata")
                    && p.getParent().getFileName().toString().startsWith(prefix));
        }
    }

    private static String tail(String log) {
        String[] lines = log.split("\r?\n");
        StringBuilder sb = new StringBuilder();
        for (int i = Math.max(0, lines.length - 15); i < lines.length; i++) {
            sb.append(lines[i]).append('\n');
        }
        return sb.toString();
    }
}
