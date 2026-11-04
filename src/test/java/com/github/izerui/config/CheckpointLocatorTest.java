package com.github.izerui.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CheckpointLocatorTest {

    private static final String JOB_A = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    private static final String JOB_B = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    private static Path metadata(Path dir, long mtime) throws Exception {
        Files.createDirectories(dir);
        Path meta = dir.resolve("_metadata");
        Files.writeString(meta, "x");
        Files.setLastModifiedTime(meta, FileTime.fromMillis(mtime));
        return dir;
    }

    @Test
    void findsLatestCompleteCheckpoint(@TempDir Path root) throws Exception {
        assertEquals(Optional.empty(), CheckpointLocator.findLatest(root.resolve("missing")));
        metadata(root.resolve(JOB_A).resolve("chk-3"), 1000);
        Path latest = metadata(root.resolve(JOB_B).resolve("chk-1"), 3000);
        metadata(root.resolve("savepoints").resolve("savepoint-1"), 2000);
        // 未完成的 checkpoint 没有 _metadata
        Files.createDirectories(root.resolve(JOB_B).resolve("chk-2"));
        assertEquals(Optional.of(latest), CheckpointLocator.findLatest(root));
    }

    @Test
    void cleanupAllForFresh(@TempDir Path root) throws Exception {
        metadata(root.resolve(JOB_A).resolve("chk-3"), 1000);
        metadata(root.resolve("savepoints").resolve("savepoint-1"), 2000);
        assertEquals(2, CheckpointLocator.cleanupExcept(root, null).size());
        assertEquals(Optional.empty(), CheckpointLocator.findLatest(root));
    }

    @Test
    void cleanupKeepsRestorePathOnly(@TempDir Path root) throws Exception {
        metadata(root.resolve(JOB_A).resolve("chk-3"), 1000);
        Path keep = metadata(root.resolve(JOB_B).resolve("chk-1"), 3000);
        metadata(root.resolve("savepoints").resolve("savepoint-1"), 2000);
        Files.createDirectories(root.resolve("other"));

        List<Path> deleted = CheckpointLocator.cleanupExcept(root, keep);
        assertEquals(2, deleted.size());
        assertFalse(Files.exists(root.resolve(JOB_A)));
        assertFalse(Files.exists(root.resolve("savepoints").resolve("savepoint-1")));
        assertTrue(Files.exists(keep.resolve("_metadata")));
        assertTrue(Files.exists(root.resolve("other")));
    }
}
