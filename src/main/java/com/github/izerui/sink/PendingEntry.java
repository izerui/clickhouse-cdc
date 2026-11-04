package com.github.izerui.sink;

import com.github.izerui.model.ChangeEvent;

import java.io.Serializable;

/**
 * DDL 屏障中暂存的事件（按库保存在 Flink keyed state 中）。
 */
public class PendingEntry implements Serializable {

    private static final long serialVersionUID = 1L;

    public ChangeEvent event;
    /**
     * 仅对 DDL 有效：该 DDL 到达时最近一次 checkpoint 快照的编号，
     * 须等编号大于它的 checkpoint 完成（即该 DDL 之前的数据与位点都已持久化）后才能执行；DML 为 -1。
     */
    public long requiredAfter;
    /**
     * 写入该条目的作业运行编号。与当前运行不同，说明它是从 checkpoint 恢复出来的：
     * 能被恢复说明它已包含在一个完成的 checkpoint 中，可以立即执行。
     */
    public long attemptId;

    public PendingEntry() {
    }

    public PendingEntry(ChangeEvent event, long requiredAfter, long attemptId) {
        this.event = event;
        this.requiredAfter = requiredAfter;
        this.attemptId = attemptId;
    }

    /** DDL 是否已可执行 */
    public boolean isReady(long currentAttemptId, long lastCompletedCheckpointId) {
        return attemptId != currentAttemptId || lastCompletedCheckpointId > requiredAfter;
    }
}
