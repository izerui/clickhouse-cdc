package com.github.izerui.clickhouse;

/**
 * ClickHouse 请求失败。
 */
public class ClickHouseException extends RuntimeException {

    private final int statusCode;

    public ClickHouseException(int statusCode, String message) {
        super(message);
        this.statusCode = statusCode;
    }

    public ClickHouseException(String message, Throwable cause) {
        super(message, cause);
        this.statusCode = -1;
    }

    /** HTTP 状态码，网络异常时为 -1 */
    public int getStatusCode() {
        return statusCode;
    }
}
