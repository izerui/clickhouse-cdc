#!/bin/sh
# 容器启动脚本：按容器资源上限设置 JVM 参数后启动同步程序（exec 使 java 成为 1 号进程，直接收到 SIGTERM）。
#
# 堆大小：堆外内存（类元数据、线程栈、GC、Flink 网络缓冲、压缩缓冲）实测约 300MB 以上，按固定比例分配堆时，
# 小容器（如 1Gi × 60%）余量不足，容易超出容器内存上限被 OOMKilled。这里按 容器上限 - 640MB 计算，
# 并限制在容器上限的 50% ~ 70% 之间。JAVA_OPTS 中已指定 -Xmx / MaxRAMPercentage 时不做调整。
set -e

opts="$JAVA_OPTS"
# CGROUP_ROOT 仅用于测试脚本
cg="${CGROUP_ROOT:-/sys/fs/cgroup}"

case "$opts" in
  *-Xmx*|*MaxRAMPercentage*|*-XX:MaxRAM=*) ;;
  *)
    limit=""
    if [ -r $cg/memory.max ]; then
      limit=$(cat $cg/memory.max)
    elif [ -r $cg/memory/memory.limit_in_bytes ]; then
      limit=$(cat $cg/memory/memory.limit_in_bytes)
    fi
    case "$limit" in
      ''|max) ;;
      *)
        mb=$((limit / 1048576))
        # cgroup v1 未设上限时为一个接近 2^63 的值
        if [ "$mb" -lt 1048576 ]; then
          heap=$((mb - 640))
          min=$((mb / 2))
          max=$((mb * 7 / 10))
          [ "$heap" -lt "$min" ] && heap=$min
          [ "$heap" -gt "$max" ] && heap=$max
          opts="-Xmx${heap}m $opts"
          echo "容器内存上限 ${mb}MB，JVM 堆上限 ${heap}MB"
        fi
        ;;
    esac
    ;;
esac

exec java $opts -jar /app/clickhouse-cdc.jar --config /app/conf/application.properties "$@"
