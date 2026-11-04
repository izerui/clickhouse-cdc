# 构建：mvn clean package -DskipTests && docker build -t clickhouse-cdc:latest .
FROM eclipse-temurin:11-jre

# 以非 root 的数字 UID 运行（不新建用户：基础镜像 Ubuntu 24.04 已自带 uid 1000 的 ubuntu 用户，useradd 会冲突）
# /app/data 存放 checkpoint（同步进度），需挂载持久卷
RUN mkdir -p /app/conf /app/data && chown -R 1000:1000 /app

WORKDIR /app
COPY --chown=1000:1000 target/clickhouse-cdc.jar /app/clickhouse-cdc.jar
COPY --chown=1000:1000 conf/application.properties /app/conf/application.properties
COPY --chmod=755 docker-entrypoint.sh /app/docker-entrypoint.sh

USER 1000:1000

# 堆大小由 docker-entrypoint.sh 按容器内存上限计算；日志只输出到标准输出
# MALLOC_ARENA_MAX：限制 glibc 为每个线程创建的内存池，避免堆外内存随线程数膨胀
ENV JAVA_OPTS="-XX:+ExitOnOutOfMemoryError -Dlogback.configurationFile=logback-console.xml" \
    MALLOC_ARENA_MAX=2 \
    TZ=Asia/Shanghai

# 健康检查端口（配置项 health.port）
EXPOSE 8080

# 脚本最后 exec java，使 java 成为 1 号进程，直接收到 SIGTERM 以执行 stop-with-savepoint
ENTRYPOINT ["/app/docker-entrypoint.sh"]
