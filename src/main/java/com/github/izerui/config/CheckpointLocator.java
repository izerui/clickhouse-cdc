package com.github.izerui.config;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * 在本地 checkpoint 目录中查找最近一次完整的 checkpoint / savepoint，用于重启后断点续传。
 * <p>
 * 目录结构：{dir}/{jobId}/chk-N/_metadata 或 {dir}/savepoints/savepoint-xxx/_metadata
 */
public final class CheckpointLocator {

    private static final Pattern JOB_ID_DIR = Pattern.compile("[0-9a-f]{32}");

    private CheckpointLocator() {
    }

    public static Optional<Path> findLatest(Path checkpointDir) throws IOException {
        if (!Files.isDirectory(checkpointDir)) {
            return Optional.empty();
        }
        try (Stream<Path> files = Files.find(checkpointDir, 3,
                (path, attrs) -> attrs.isRegularFile() && path.getFileName().toString().equals("_metadata"))) {
            return files.max(Comparator.comparingLong(CheckpointLocator::lastModified))
                    .map(Path::getParent);
        }
    }

    private static long lastModified(Path path) {
        try {
            return Files.getLastModifiedTime(path).toMillis();
        } catch (IOException e) {
            return 0L;
        }
    }

    /**
     * 删除除 keep 以外的历史 checkpoint / savepoint。
     * <p>
     * 每次启动都是一个新的 Flink 作业（新的 jobId 目录），旧作业的 checkpoint 不会被 Flink 自动清理。
     * 恢复所用的 keep 一定是最新的，其余的都已无用。只删除 jobId 目录与 savepoints 下的目录，不碰其他文件。
     *
     * @param keep 要保留的 checkpoint / savepoint 目录；为 null 时全部删除（--fresh）
     * @return 被删除的目录
     */
    public static List<Path> cleanupExcept(Path checkpointDir, Path keep) throws IOException {
        List<Path> deleted = new ArrayList<>();
        if (!Files.isDirectory(checkpointDir)) {
            return deleted;
        }
        Path keepNormalized = keep == null ? null : keep.toAbsolutePath().normalize();
        List<Path> candidates = new ArrayList<>();
        try (Stream<Path> children = Files.list(checkpointDir)) {
            children.filter(Files::isDirectory).forEach(child -> {
                String name = child.getFileName().toString();
                if (JOB_ID_DIR.matcher(name).matches()) {
                    candidates.add(child);
                } else if (name.equals("savepoints")) {
                    try (Stream<Path> savepoints = Files.list(child)) {
                        savepoints.filter(p -> Files.isDirectory(p) && p.getFileName().toString().startsWith("savepoint-"))
                                .forEach(candidates::add);
                    } catch (IOException e) {
                        throw new IllegalStateException(e);
                    }
                }
            });
        }
        for (Path candidate : candidates) {
            if (keepNormalized != null && keepNormalized.startsWith(candidate.toAbsolutePath().normalize())) {
                continue;
            }
            deleteRecursively(candidate);
            deleted.add(candidate);
        }
        return deleted;
    }

    private static void deleteRecursively(Path dir) throws IOException {
        try (Stream<Path> walk = Files.walk(dir)) {
            List<Path> paths = new ArrayList<>();
            walk.forEach(paths::add);
            paths.sort(Comparator.reverseOrder());
            for (Path p : paths) {
                Files.deleteIfExists(p);
            }
        }
    }
}
