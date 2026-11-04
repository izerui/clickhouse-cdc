package com.github.izerui.config;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * 同步任务配置。
 * <p>
 * 加载顺序（后者覆盖前者）：配置文件 → JVM 系统属性(-Dkey=value) → 命令行参数(--key=value)。
 * 配置值支持 ${ENV_NAME} 或 ${ENV_NAME:默认值} 形式引用环境变量。
 */
public class CdcConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Pattern PLACEHOLDER = Pattern.compile("\\$\\{([A-Za-z0-9_.]+)(?::([^}]*))?}");

    public enum StartupMode {INITIAL, LATEST, EARLIEST, TIMESTAMP}

    private final Map<String, String> props;

    public CdcConfig(Map<String, String> props) {
        this(props, true);
    }

    private CdcConfig(Map<String, String> props, boolean resolvePlaceholders) {
        Map<String, String> resolved = new TreeMap<>();
        props.forEach((k, v) -> {
            String value = v == null ? "" : v.trim();
            resolved.put(k.trim(), resolvePlaceholders ? resolvePlaceholders(value) : value);
        });
        this.props = Collections.unmodifiableMap(resolved);
        validate();
    }

    /**
     * 从配置文件加载，并用系统属性、命令行覆盖项覆盖。
     */
    public static CdcConfig load(Path configFile, Map<String, String> overrides) throws IOException {
        Properties properties = new Properties();
        if (configFile != null) {
            try (InputStream in = Files.newInputStream(configFile);
                 Reader reader = new InputStreamReader(in, StandardCharsets.UTF_8)) {
                properties.load(reader);
            }
        }
        Map<String, String> map = new LinkedHashMap<>();
        properties.stringPropertyNames().forEach(k -> map.put(k, properties.getProperty(k)));
        System.getProperties().stringPropertyNames().stream()
                .filter(k -> k.startsWith("source.") || k.startsWith("startup.") || k.startsWith("clickhouse.")
                        || k.startsWith("sink.") || k.startsWith("job.") || k.startsWith("checkpoint.") || k.startsWith("health."))
                .forEach(k -> map.put(k, System.getProperty(k)));
        map.putAll(overrides);
        return new CdcConfig(map);
    }

    private static String resolvePlaceholders(String value) {
        Matcher m = PLACEHOLDER.matcher(value);
        StringBuilder sb = new StringBuilder();
        while (m.find()) {
            String env = System.getenv(m.group(1));
            String replacement = env != null ? env : (m.group(2) != null ? m.group(2) : "");
            m.appendReplacement(sb, Matcher.quoteReplacement(replacement));
        }
        m.appendTail(sb);
        return sb.toString();
    }

    private void validate() {
        require("source.hostname");
        require("source.username");
        require("source.databases");
        require("clickhouse.url");
        if (getStartupMode() == StartupMode.TIMESTAMP && getStartupTimestampMillis() == null) {
            throw new IllegalArgumentException("startup.mode=timestamp 时必须配置 startup.timestamp-millis");
        }
        for (String table : getSourceTables()) {
            if (table.contains(".") && !getSourceDatabases().contains(table.substring(0, table.indexOf('.')))) {
                throw new IllegalArgumentException("source.tables 中的 " + table + " 所属库不在 source.databases 中");
            }
        }
        if (getHealthPort() > 0 && getHealthPort() == getJobWebUiPort()) {
            throw new IllegalArgumentException("health.port 与 job.web-ui.port 不能相同: " + getHealthPort());
        }
        if (getSinkBatchSize() <= 0 || getSinkBatchIntervalMs() <= 0) {
            throw new IllegalArgumentException("sink.batch.size 和 sink.batch.interval-ms 必须大于 0");
        }
    }

    private void require(String key) {
        if (get(key, "").isEmpty()) {
            throw new IllegalArgumentException("缺少必填配置项: " + key);
        }
    }

    /**
     * 返回覆盖了部分配置项的新配置（用于写入启动时自动计算出的资源参数）。
     */
    public CdcConfig withOverrides(Map<String, String> overrides) {
        Map<String, String> merged = new LinkedHashMap<>(props);
        merged.putAll(overrides);
        // 已有的值在加载时已替换过占位符，不再替换（例如密码中恰好含有 ${...}）
        return new CdcConfig(merged, false);
    }

    // ============================== 通用读取 ==============================

    /** 未配置、为空或为 auto：由程序按运行环境自动计算 */
    public boolean isAuto(String key) {
        String v = props.get(key);
        return v == null || v.isEmpty() || v.equalsIgnoreCase("auto");
    }

    public String get(String key, String defaultValue) {
        String v = props.get(key);
        return v == null || v.isEmpty() ? defaultValue : v;
    }

    public int getInt(String key, int defaultValue) {
        return isAuto(key) ? defaultValue : Integer.parseInt(props.get(key));
    }

    public long getLong(String key, long defaultValue) {
        return isAuto(key) ? defaultValue : Long.parseLong(props.get(key));
    }

    public boolean getBoolean(String key, boolean defaultValue) {
        String v = props.get(key);
        return v == null || v.isEmpty() ? defaultValue : Boolean.parseBoolean(v);
    }

    private static List<String> splitList(String value) {
        if (value == null || value.isBlank()) {
            return new ArrayList<>();
        }
        return Arrays.stream(value.split(",")).map(String::trim).filter(s -> !s.isEmpty()).collect(Collectors.toList());
    }

    /**
     * 以 prefix 开头的配置项（去掉前缀），用于透传 Debezium 参数。
     */
    public Properties getWithPrefix(String prefix) {
        Properties p = new Properties();
        props.forEach((k, v) -> {
            if (k.startsWith(prefix) && k.length() > prefix.length()) {
                p.setProperty(k.substring(prefix.length()), v);
            }
        });
        return p;
    }

    // ============================== MySQL 源 ==============================

    public String getSourceHostname() {
        return get("source.hostname", null);
    }

    public int getSourcePort() {
        return getInt("source.port", 3306);
    }

    public String getSourceUsername() {
        return get("source.username", null);
    }

    public String getSourcePassword() {
        return get("source.password", "");
    }

    public List<String> getSourceDatabases() {
        return splitList(get("source.databases", ""));
    }

    /**
     * 指定要同步的表（逗号分隔）：表名，或 库名.表名（同步多个库时精确指定）。
     * 配置后只同步这些表，source.tables.include 不再生效；source.tables.exclude 仍然生效。
     */
    public List<String> getSourceTables() {
        return splitList(get("source.tables", ""));
    }

    /** 表名包含规则（正则，只匹配表名，不含库名），默认全部 */
    public String getSourceTablesInclude() {
        return get("source.tables.include", ".*");
    }

    /** 表名排除规则（正则，只匹配表名），默认不排除 */
    public String getSourceTablesExclude() {
        return get("source.tables.exclude", "");
    }

    /** server-id 或范围，如 5400-5420；未配置时按并行度随机生成 */
    public String getSourceServerId() {
        return get("source.server-id", "");
    }

    public ZoneId getSourceServerTimeZone() {
        return ZoneId.of(get("source.server-time-zone", ZoneId.systemDefault().getId()));
    }

    public int getSourceFetchSize() {
        return getInt("source.fetch-size", 1024);
    }

    public int getSourceSplitSize() {
        return getInt("source.split-size", 8096);
    }

    /**
     * 主键分布系数 (MAX - MIN + 1) / 行数 不超过该值时按主键范围均匀切分分片，否则逐个分片查询边界。
     * 均匀切分时主键分布不均的区间可能包含 分片行数 × 系数 行，因此默认值比 Flink CDC 的 1000 小得多，
     * 保证单个分片的内存可控。
     */
    public double getSourceEvenDistributionFactorUpper() {
        return Double.parseDouble(get("source.chunk-key.even-distribution.factor.upper-bound", "2.0"));
    }

    public int getSourceConnectionPoolSize() {
        return getInt("source.connection-pool-size", 20);
    }

    public int getSourceConnectTimeoutSeconds() {
        return getInt("source.connect-timeout-seconds", 30);
    }

    /** 透传给 Debezium 的参数：source.debezium.xxx=yyy */
    public Properties getSourceDebeziumProperties() {
        return getWithPrefix("source.debezium.");
    }

    // ============================== 启动模式 ==============================

    public StartupMode getStartupMode() {
        return StartupMode.valueOf(get("startup.mode", "initial").toUpperCase());
    }

    public Long getStartupTimestampMillis() {
        String v = get("startup.timestamp-millis", "");
        return v.isEmpty() ? null : Long.parseLong(v);
    }

    // ============================== ClickHouse ==============================

    public String getClickHouseUrl() {
        String url = get("clickhouse.url", null);
        return url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
    }

    public String getClickHouseUsername() {
        return get("clickhouse.username", "default");
    }

    public String getClickHousePassword() {
        return get("clickhouse.password", "");
    }

    public int getClickHouseSocketTimeoutMs() {
        return getInt("clickhouse.socket-timeout-ms", 300_000);
    }

    public boolean isClickHouseCompress() {
        return getBoolean("clickhouse.compress", true);
    }

    /** 建表时的分区表达式，默认不分区 */
    public String getClickHousePartitionBy() {
        return get("clickhouse.partition-by", "");
    }

    /** 建表时追加的 SETTINGS，如 index_granularity=8192 */
    public String getClickHouseTableSettings() {
        return get("clickhouse.table-settings", "");
    }

    // ============================== 写入 ==============================

    public int getSinkBatchSize() {
        return getInt("sink.batch.size", 50_000);
    }

    public long getSinkBatchIntervalMs() {
        return getLong("sink.batch.interval-ms", 2_000);
    }

    /** 单表缓冲的最大字节数，超过即写出，避免宽表占用过多内存 */
    public long getSinkBatchMaxBytes() {
        return getLong("sink.batch.max-bytes", 64L * 1024 * 1024);
    }

    /** 所有表写入缓冲合计的最大字节数（各写入实例平分），超过即全部写出 */
    public long getSinkBufferMaxTotalBytes() {
        return getLong("sink.buffer.max-total-bytes", 256L * 1024 * 1024);
    }

    public int getSinkMaxRetries() {
        return getInt("sink.max-retries", 5);
    }

    /** 全新全量同步（无 checkpoint 的 initial 模式）前是否清空目标表 */
    public boolean isSinkTruncateBeforeSnapshot() {
        return getBoolean("sink.truncate-before-snapshot", true);
    }

    /** 源端 DROP TABLE 是否同步删除目标表 */
    public boolean isSinkDropTableEnabled() {
        return getBoolean("sink.drop-table-enabled", false);
    }

    /** 源端删除列时是否同步删除目标列 */
    public boolean isSinkDropColumnEnabled() {
        return getBoolean("sink.drop-column-enabled", true);
    }

    /** 源端 TRUNCATE TABLE 是否同步清空目标表 */
    public boolean isSinkTruncateTableEnabled() {
        return getBoolean("sink.truncate-table-enabled", true);
    }

    /** 目标端执行 DDL 失败时是否忽略（默认不忽略，作业失败重启以暴露问题） */
    public boolean isSinkIgnoreDdlErrors() {
        return getBoolean("sink.ignore-ddl-errors", false);
    }

    // ============================== 作业 ==============================

    public String getJobName() {
        return get("job.name", "mysql-to-clickhouse-cdc");
    }

    public int getJobParallelism() {
        return getInt("job.parallelism", 4);
    }

    /** Flink Web UI 端口，0 表示不启用 */
    public int getJobWebUiPort() {
        return getInt("job.web-ui.port", 0);
    }

    /**
     * 重启策略：exponential-delay（默认，无限次重试，适合常驻同步）或 fixed-delay（重试 job.restart.attempts 次后进程退出）
     */
    public String getJobRestartStrategy() {
        String strategy = get("job.restart.strategy", "exponential-delay");
        if (!strategy.equals("exponential-delay") && !strategy.equals("fixed-delay")) {
            throw new IllegalArgumentException("job.restart.strategy 只支持 exponential-delay 或 fixed-delay: " + strategy);
        }
        return strategy;
    }

    /** fixed-delay 策略的最大重启次数 */
    public int getJobRestartAttempts() {
        return getInt("job.restart.attempts", 10);
    }

    /** 重启间隔（exponential-delay 为初始间隔） */
    public long getJobRestartDelayMs() {
        return getLong("job.restart.delay-ms", 10_000);
    }

    /** exponential-delay 策略的最大重启间隔 */
    public long getJobRestartMaxDelayMs() {
        return getLong("job.restart.max-delay-ms", 300_000);
    }

    /** 健康检查端点端口（/healthz、/readyz、/status），0 表示不启用；K8s 部署时开启 */
    public int getHealthPort() {
        return getInt("health.port", 0);
    }

    public String getCheckpointDir() {
        return get("checkpoint.dir", "./data/checkpoints");
    }

    public long getCheckpointIntervalMs() {
        return getLong("checkpoint.interval-ms", 60_000);
    }

    public long getCheckpointTimeoutMs() {
        return getLong("checkpoint.timeout-ms", 600_000);
    }

    public int getCheckpointRetained() {
        return getInt("checkpoint.retained", 3);
    }

    /**
     * 同步范围：源库、表过滤规则。续传时范围变化会导致新增的表缺少全量数据，需要据此拒绝续传。
     */
    public String describeSyncScope() {
        return "source.databases=" + String.join(",", getSourceDatabases()) + "\n"
                + "source.tables=" + String.join(",", getSourceTables()) + "\n"
                + "source.tables.include=" + getSourceTablesInclude() + "\n"
                + "source.tables.exclude=" + getSourceTablesExclude() + "\n";
    }

    /** 打印用：隐藏密码 */
    public String describe() {
        return props.entrySet().stream()
                .map(e -> "  " + e.getKey() + " = " + (e.getKey().contains("password") ? "******" : e.getValue()))
                .collect(Collectors.joining("\n"));
    }
}
