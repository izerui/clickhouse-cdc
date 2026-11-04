package com.github.izerui.clickhouse;

import com.github.izerui.config.CdcConfig;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPOutputStream;

/**
 * 基于 ClickHouse 官方 HTTP 接口的轻量客户端。
 * <p>
 * 写入使用 {@code INSERT ... FORMAT TabSeparated}，查询结果同样以 TabSeparated 解析。
 */
public class ClickHouseClient implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String url;
    private final String username;
    private final String password;
    private final int socketTimeoutMs;
    private final boolean compress;

    private transient HttpClient httpClient;

    public ClickHouseClient(CdcConfig config) {
        this(config.getClickHouseUrl(), config.getClickHouseUsername(), config.getClickHousePassword(),
                config.getClickHouseSocketTimeoutMs(), config.isClickHouseCompress());
    }

    public ClickHouseClient(String url, String username, String password, int socketTimeoutMs, boolean compress) {
        this.url = url;
        this.username = username;
        this.password = password;
        this.socketTimeoutMs = socketTimeoutMs;
        this.compress = compress;
    }

    private HttpClient http() {
        if (httpClient == null) {
            httpClient = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(30))
                    .version(HttpClient.Version.HTTP_1_1)
                    .build();
        }
        return httpClient;
    }

    /** 执行 DDL 或其他无返回值的语句 */
    public void execute(String sql) {
        send(URI.create(url + "/"), sql.getBytes(StandardCharsets.UTF_8), false);
    }

    /** 执行查询，返回 TabSeparated 解析后的行 */
    public List<String[]> query(String sql) {
        String body = send(URI.create(url + "/"), (sql + " FORMAT TabSeparated").getBytes(StandardCharsets.UTF_8), false);
        List<String[]> rows = new ArrayList<>();
        for (String line : body.split("\n")) {
            if (line.isEmpty()) {
                continue;
            }
            String[] cells = line.split("\t", -1);
            for (int i = 0; i < cells.length; i++) {
                cells[i] = "\\N".equals(cells[i]) ? null : TsvEncoder.unescape(cells[i]);
            }
            rows.add(cells);
        }
        return rows;
    }

    /**
     * 以 TabSeparated 格式批量写入。
     *
     * @param columns 列名（已转义），与 tsv 中的列一一对应
     */
    public void insert(String database, String table, List<String> columns, byte[] tsv) {
        String sql = "INSERT INTO " + ClickHouseSql.quote(database) + "." + ClickHouseSql.quote(table)
                + " (" + String.join(", ", columns) + ") FORMAT TabSeparated";
        URI uri = URI.create(url + "/?input_format_null_as_default=1&query="
                + URLEncoder.encode(sql, StandardCharsets.UTF_8));
        send(uri, tsv, compress);
    }

    private String send(URI uri, byte[] body, boolean gzip) {
        HttpRequest.Builder builder = HttpRequest.newBuilder(uri)
                .timeout(Duration.ofMillis(socketTimeoutMs))
                .header("X-ClickHouse-User", username)
                .header("X-ClickHouse-Key", password == null ? "" : password);
        byte[] payload = body;
        if (gzip) {
            payload = gzip(body);
            builder.header("Content-Encoding", "gzip");
        }
        HttpRequest request = builder.POST(HttpRequest.BodyPublishers.ofByteArray(payload)).build();
        HttpResponse<String> response;
        try {
            response = http().send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new ClickHouseException("请求 ClickHouse 失败: " + e.getMessage(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ClickHouseException("请求 ClickHouse 被中断", e);
        }
        if (response.statusCode() != 200) {
            throw new ClickHouseException(response.statusCode(), response.body().trim());
        }
        return response.body();
    }

    private static byte[] gzip(byte[] data) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(Math.max(64, data.length / 4));
        try (GZIPOutputStream gz = new GZIPOutputStream(bos, 64 * 1024)) {
            gz.write(data);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        return bos.toByteArray();
    }

    @Override
    public String toString() {
        return "ClickHouseClient{" + url + ", user=" + username + "}";
    }
}
