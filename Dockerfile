# 构建：mvn clean package -DskipTests && docker build -t clickhouse-cdc:latest .
FROM eclipse-temurin:11-jre

# 以非 root 的数字 UID 运行（不新建用户：基础镜像 Ubuntu 24.04 已自带 uid 1000 的 ubuntu 用户，useradd 会冲突）
# /app/data 存放 checkpoint（同步进度），需挂载持久卷
RUN mkdir -p /app/conf /app/data && chown -R 1000:1000 /app

WORKDIR /app
COPY --chown=1000:1000 target/clickhouse-cdc.jar /app/clickhouse-cdc.jar
COPY --chown=1000:1000 conf/application.properties /app/conf/application.properties

USER 1000:1000

# JVM 按容器内存比例分配堆；日志只输出到标准输出
ENV JAVA_OPTS="-XX:MaxRAMPercentage=60 -XX:+ExitOnOutOfMemoryError -Dlogback.configurationFile=logback-console.xml" \
    TZ=Asia/Shanghai

# 健康检查端口（配置项 health.port）
EXPOSE 8080

# 使用 exec 让 java 成为 1 号进程，直接收到 SIGTERM 以执行 stop-with-savepoint
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/clickhouse-cdc.jar --config /app/conf/application.properties \"$@\"", "--"]
