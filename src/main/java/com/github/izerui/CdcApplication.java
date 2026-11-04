package com.github.izerui;

import com.github.izerui.clickhouse.ClickHouseClient;
import com.github.izerui.clickhouse.TargetTableInitializer;
import com.github.izerui.config.CdcConfig;
import com.github.izerui.config.CheckpointLocator;
import com.github.izerui.config.ResourcePlanner;
import com.github.izerui.health.HealthServer;
import com.github.izerui.model.ChangeEvent;
import com.github.izerui.model.TableDef;
import com.github.izerui.sink.ClickHouseWriter;
import com.github.izerui.source.MySqlSourceFactory;
import com.github.izerui.source.TableFilter;
import com.github.izerui.verify.DataVerifier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * MySQL → ClickHouse CDC 同步入口，内嵌 Flink 本地集群运行：
 * <pre>
 *   java -jar clickhouse-cdc.jar [--config conf/application.properties] [--fresh] [--key=value ...]
 * </pre>
 * <ul>
 *     <li>--config：配置文件路径，默认 conf/application.properties</li>
 *     <li>--fresh：忽略已有 checkpoint，按 startup.mode 重新开始</li>
 *     <li>--restore &lt;path&gt;：从指定的 checkpoint / savepoint 目录恢复</li>
 *     <li>--verify：逐表比对 MySQL 与 ClickHouse 行数后退出（不启动同步）</li>
 *     <li>--force-resume：续传时跳过同步范围变化检查</li>
 *     <li>--key=value：覆盖配置文件中的同名配置</li>
 * </ul>
 */
public class CdcApplication {

    private static final Logger LOG = LoggerFactory.getLogger(CdcApplication.class);

    public static void main(String[] args) {
        try {
            run(args);
        } catch (Throwable e) {
            // 健康检查等非守护线程会阻止 JVM 退出，启动失败时必须显式退出，K8s 才会感知并重建 Pod
            LOG.error("启动失败", e);
            System.exit(1);
        }
    }

    static void run(String[] args) throws Exception {
        Path configFile = Paths.get("conf/application.properties");
        boolean fresh = false;
        boolean verify = false;
        boolean forceResume = false;
        Path explicitRestore = null;
        Map<String, String> overrides = new LinkedHashMap<>();
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (arg.equals("--fresh")) {
                fresh = true;
            } else if (arg.equals("--verify")) {
                verify = true;
            } else if (arg.equals("--force-resume")) {
                forceResume = true;
            } else if (arg.equals("--restore") && i + 1 < args.length) {
                explicitRestore = Paths.get(args[++i]).toAbsolutePath().normalize();
            } else if (arg.equals("--config") && i + 1 < args.length) {
                configFile = Paths.get(args[++i]);
            } else if (arg.startsWith("--config=")) {
                configFile = Paths.get(arg.substring("--config=".length()));
            } else if (arg.startsWith("--") && arg.contains("=")) {
                overrides.put(arg.substring(2, arg.indexOf('=')), arg.substring(arg.indexOf('=') + 1));
            } else if (arg.equals("--help") || arg.equals("-h")) {
                printUsage();
                return;
            } else {
                throw new IllegalArgumentException("无法识别的参数: " + arg + "（使用 --help 查看用法）");
            }
        }
        if (!Files.exists(configFile)) {
            throw new IllegalArgumentException("配置文件不存在: " + configFile.toAbsolutePath());
        }
        CdcConfig config = CdcConfig.load(configFile, overrides);
        LOG.info("加载配置 {}:\n{}", configFile.toAbsolutePath(), config.describe());

        if (verify) {
            DataVerifier verifier = new DataVerifier(config, new ClickHouseClient(config));
            StringBuilder report = new StringBuilder();
            int mismatched = DataVerifier.print(verifier.verify(), report);
            System.out.print(report);
            System.exit(mismatched == 0 ? 0 : 3);
        }
        if (fresh && explicitRestore != null) {
            throw new IllegalArgumentException("--fresh 与 --restore 不能同时使用");
        }
        if (explicitRestore != null && !Files.exists(explicitRestore.resolve("_metadata"))) {
            throw new IllegalArgumentException("--restore 指定的目录中没有 _metadata，不是有效的 checkpoint / savepoint: " + explicitRestore);
        }

        // 尽早启动健康检查，预建表耗时较长时存活探针也能通过
        HealthServer health = null;
        if (config.getHealthPort() > 0) {
            health = new HealthServer(config.getHealthPort());
            health.start();
        }

        Path checkpointDir = Paths.get(config.getCheckpointDir()).toAbsolutePath().normalize();
        if (fresh) {
            // 重新开始：删除全部旧进度，避免新作业在产生第一个 checkpoint 前退出时，下次启动又退回旧位点
            for (Path deleted : CheckpointLocator.cleanupExcept(checkpointDir, null)) {
                LOG.info("--fresh：删除旧 checkpoint {}", deleted);
            }
        }
        Optional<Path> restorePath = explicitRestore != null ? Optional.of(explicitRestore)
                : fresh ? Optional.empty() : CheckpointLocator.findLatest(checkpointDir);
        restorePath.ifPresentOrElse(
                p -> LOG.info("从 checkpoint 恢复: {}", p),
                () -> LOG.info("未使用 checkpoint，按 startup.mode={} 启动", config.getStartupMode()));
        // 目标库变化不受 --force-resume 影响：旧位点之前的数据都在原来的库里，续写到新库必然缺少历史数据
        Path targetDatabasesFile = checkpointDir.resolve("target-databases.txt");
        if (restorePath.isPresent()) {
            checkTargetDatabasesUnchanged(config, targetDatabasesFile);
        }

        // 1. 扫描源表；全新同步时预建 / 校正目标表（全量同步前按配置清空），续传时目标表结构交由回放的 DDL 推进
        if (health != null) {
            health.setPhase("INITIALIZING_TABLES");
        }
        TargetTableInitializer initializer = new TargetTableInitializer(config, new ClickHouseClient(config));
        TargetTableInitializer.Scan scan = initializer.scan();
        Path scopeFile = checkpointDir.resolve("sync-scope.txt");
        Path noPrimaryKeyFile = checkpointDir.resolve("no-primary-key-tables.txt");
        if (restorePath.isPresent() && !forceResume) {
            checkScopeUnchanged(config, scan, scopeFile, noPrimaryKeyFile);
        }
        if (restorePath.isEmpty()) {
            boolean freshSnapshot = config.getStartupMode() == CdcConfig.StartupMode.INITIAL;
            initializer.prepareTables(scan, freshSnapshot && config.isSinkTruncateBeforeSnapshot());
        }
        Files.createDirectories(checkpointDir);
        Files.writeString(scopeFile, config.describeSyncScope());
        Files.write(targetDatabasesFile, config.getTargetDatabases().entrySet().stream()
                .map(e -> e.getKey() + "=" + e.getValue()).collect(Collectors.toList()));
        Files.write(noPrimaryKeyFile, scan.noPrimaryKeyTables);
        Set<String> noPrimaryKeyTables = scan.noPrimaryKeyTables;

        // 2. 按堆内存、CPU 与源表规模计算未显式配置的并行度、分片大小、写入缓冲上限
        CdcConfig jobConfig = ResourcePlanner.apply(config, scan.tables);

        // 3. 构建作业
        StreamExecutionEnvironment env = createEnvironment(jobConfig, checkpointDir, restorePath.orElse(null));
        TableFilter tableFilter = TableFilter.fromConfig(jobConfig, noPrimaryKeyTables);
        env.fromSource(MySqlSourceFactory.create(jobConfig, tableFilter), WatermarkStrategy.noWatermarks(), "MySQL CDC Source")
                .uid("mysql-cdc-source")
                .keyBy(ChangeEvent::partitionKey)
                .process(new ClickHouseWriter(jobConfig))
                .uid("clickhouse-writer")
                .name("ClickHouse Writer");

        // 4. 运行，并在进程退出时尝试 stop-with-savepoint
        JobClient jobClient = env.executeAsync(jobConfig.getJobName());
        LOG.info("作业已启动: {} (jobId={})", jobConfig.getJobName(), jobClient.getJobID());
        if (health != null) {
            health.setJobClient(jobClient);
        }
        startCheckpointCleaner(checkpointDir, jobClient);
        Path savepointDir = checkpointDir.resolve("savepoints");
        Thread shutdownHook = new Thread(() -> stopWithSavepoint(jobClient, savepointDir), "cdc-shutdown");
        Runtime.getRuntime().addShutdownHook(shutdownHook);
        try {
            jobClient.getJobExecutionResult().get();
            LOG.info("作业已结束");
            if (health != null) {
                health.close();
            }
        } catch (Exception e) {
            LOG.error("作业运行失败", e);
            try {
                Runtime.getRuntime().removeShutdownHook(shutdownHook);
            } catch (IllegalStateException ignored) {
                // 已在关闭流程中
            }
            System.exit(1);
        }
    }

    /**
     * 续传前检查已同步的源库写入的 ClickHouse 库是否变化（clickhouse.database-mapping）。
     * 新增的源库不在此检查，由同步范围检查处理。
     */
    static void checkTargetDatabasesUnchanged(CdcConfig config, Path targetDatabasesFile) throws Exception {
        if (!Files.exists(targetDatabasesFile)) {
            return;
        }
        Map<String, String> current = config.getTargetDatabases();
        List<String> changed = new ArrayList<>();
        for (String line : Files.readAllLines(targetDatabasesFile)) {
            int i = line.indexOf('=');
            if (i <= 0) {
                continue;
            }
            String source = line.substring(0, i);
            String previous = line.substring(i + 1);
            String now = current.get(source);
            if (now != null && !now.equals(previous)) {
                changed.add(source + "：" + previous + " -> " + now);
            }
        }
        if (!changed.isEmpty()) {
            throw new IllegalStateException("写入的 ClickHouse 库发生变化（clickhouse.database-mapping）：" + changed
                    + "\n续传只会把之后的变更写入新库，新库缺少全部历史数据，--force-resume 也不能跳过此检查。"
                    + "请恢复原来的映射；或使用 --fresh 重新全量同步到新库（会删除旧进度、清空新库中的同名表，原来的库保持不动）。");
        }
    }

    /**
     * 续传前检查同步范围是否变化：范围扩大会让新纳入的表只有增量、缺少全量数据。
     */
    static void checkScopeUnchanged(CdcConfig config, TargetTableInitializer.Scan scan,
                                            Path scopeFile, Path noPrimaryKeyFile) throws Exception {
        List<String> problems = new ArrayList<>();
        if (Files.exists(scopeFile)) {
            String previousScope = Files.readString(scopeFile);
            if (!previousScope.equals(config.describeSyncScope())) {
                problems.add("同步范围配置变化。\n上次:\n" + previousScope + "本次:\n" + config.describeSyncScope());
            }
        }
        if (Files.exists(noPrimaryKeyFile)) {
            Set<String> previouslyExcluded = new HashSet<>(Files.readAllLines(noPrimaryKeyFile));
            List<String> gainedPrimaryKey = new ArrayList<>();
            for (TableDef t : scan.tables) {
                if (previouslyExcluded.contains(t.fullName())) {
                    gainedPrimaryKey.add(t.fullName());
                }
            }
            if (!gainedPrimaryKey.isEmpty()) {
                problems.add("以下表上次因无主键未同步、现已有主键：" + gainedPrimaryKey);
            }
        }
        if (!problems.isEmpty()) {
            throw new IllegalStateException(String.join("\n", problems)
                    + "\n续传会导致新纳入的表缺少全量数据。请恢复原配置，或使用 --fresh 重新全量同步；"
                    + "确认只需增量时可加 --force-resume 跳过检查。");
        }
    }

    /**
     * 新作业产生第一个 checkpoint 后，清理旧作业留下的 checkpoint / savepoint（之前清理的话，恢复失败将无路可退）。
     */
    private static void startCheckpointCleaner(Path checkpointDir, JobClient jobClient) {
        Path jobDir = checkpointDir.resolve(jobClient.getJobID().toString());
        Thread cleaner = new Thread(() -> {
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    Optional<Path> latest = CheckpointLocator.findLatest(checkpointDir);
                    if (latest.isPresent() && latest.get().startsWith(jobDir)) {
                        for (Path deleted : CheckpointLocator.cleanupExcept(checkpointDir, latest.get())) {
                            LOG.info("清理历史 checkpoint: {}", deleted);
                        }
                        return;
                    }
                    Thread.sleep(10_000);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                LOG.warn("清理历史 checkpoint 失败: {}", e.toString());
            }
        }, "checkpoint-cleaner");
        cleaner.setDaemon(true);
        cleaner.start();
    }

    static StreamExecutionEnvironment createEnvironment(CdcConfig config, Path checkpointDir, Path restorePath) {
        Configuration conf = new Configuration();
        conf.set(CoreOptions.DEFAULT_PARALLELISM, config.getJobParallelism());
        // 内嵌集群的 IO 线程池默认为 4 × CPU 核数，容器未设 CPU 上限时按节点核数创建大量线程，这里只需少量
        conf.set(ClusterOptions.CLUSTER_IO_EXECUTOR_POOL_SIZE, Math.min(4, Runtime.getRuntime().availableProcessors()));
        // checkpoint：保存在本地目录，作业取消后保留，用于进程重启后续传
        conf.set(StateBackendOptions.STATE_BACKEND, "hashmap");
        conf.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
        conf.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir.toUri().toString());
        conf.set(CheckpointingOptions.MAX_RETAINED_CHECKPOINTS, config.getCheckpointRetained());
        conf.set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofMillis(config.getCheckpointIntervalMs()));
        conf.set(ExecutionCheckpointingOptions.CHECKPOINTING_TIMEOUT, Duration.ofMillis(config.getCheckpointTimeoutMs()));
        conf.set(ExecutionCheckpointingOptions.CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE);
        conf.set(ExecutionCheckpointingOptions.TOLERABLE_FAILURE_NUMBER, 3);
        conf.set(ExecutionCheckpointingOptions.EXTERNALIZED_CHECKPOINT,
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 失败后在进程内自动重启，从最近的 checkpoint 恢复
        String restartStrategy = config.getJobRestartStrategy();
        conf.set(RestartStrategyOptions.RESTART_STRATEGY, restartStrategy);
        if (restartStrategy.equals("fixed-delay")) {
            conf.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, config.getJobRestartAttempts());
            conf.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofMillis(config.getJobRestartDelayMs()));
        } else {
            // 下游短暂不可用（如 ClickHouse 维护）时持续重试，恢复后自动继续
            conf.set(RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_INITIAL_BACKOFF, Duration.ofMillis(config.getJobRestartDelayMs()));
            conf.set(RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_MAX_BACKOFF, Duration.ofMillis(config.getJobRestartMaxDelayMs()));
            conf.set(RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_BACKOFF_MULTIPLIER, 2.0);
            conf.set(RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_RESET_BACKOFF_THRESHOLD, Duration.ofHours(1));
        }
        if (restorePath != null) {
            conf.set(SavepointConfigOptions.SAVEPOINT_PATH, restorePath.toUri().toString());
        }
        int webUiPort = config.getJobWebUiPort();
        if (webUiPort > 0) {
            conf.set(RestOptions.PORT, webUiPort);
            // 禁止通过 Web UI / REST 上传、运行 jar（可执行任意代码）；隐藏取消按钮。
            // 注意 CANCEL_ENABLE 只影响页面，REST 接口仍可取消作业，因此 Web UI 不能无鉴权对外开放
            conf.set(WebOptions.SUBMIT_ENABLE, false);
            conf.set(WebOptions.CANCEL_ENABLE, false);
            return StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        }
        return StreamExecutionEnvironment.createLocalEnvironment(config.getJobParallelism(), conf);
    }

    static void stopWithSavepoint(JobClient jobClient, Path savepointDir) {
        try {
            LOG.info("收到退出信号，正在 stop-with-savepoint ...");
            String path = jobClient.stopWithSavepoint(false, savepointDir.toUri().toString(), SavepointFormatType.CANONICAL)
                    .get(2, TimeUnit.MINUTES);
            LOG.info("savepoint 已保存: {}", path);
        } catch (Exception e) {
            LOG.warn("stop-with-savepoint 失败，下次启动将从最近的 checkpoint 恢复: {}", e.toString());
        }
    }

    private static void printUsage() {
        System.out.println("用法: java -jar clickhouse-cdc.jar [--config conf/application.properties] [--fresh | --restore <checkpoint目录>] [--force-resume] [--verify] [--key=value ...]");
    }
}
