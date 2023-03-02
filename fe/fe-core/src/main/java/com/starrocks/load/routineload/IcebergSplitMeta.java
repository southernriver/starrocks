// Licensed to the Apache Software Foundation (ASF) under one

package com.starrocks.load.routineload;

import com.starrocks.common.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class IcebergSplitMeta implements Writable {
    private final long startSnapshotId;
    private final long endSnapshotId;
    private final long endSnapshotTimestamp;
    private final int totalSplits;
    private boolean allDone;

    public IcebergSplitMeta(long startSnapshotId, long endSnapshotId, long endSnapshotTimestamp, int totalSplits) {
        this.startSnapshotId = startSnapshotId;
        this.endSnapshotId = endSnapshotId;
        this.endSnapshotTimestamp = endSnapshotTimestamp;
        this.totalSplits = totalSplits;
    }

    public long getStartSnapshotId() {
        return startSnapshotId;
    }

    public long getEndSnapshotId() {
        return endSnapshotId;
    }

    public long getEndSnapshotTimestamp() {
        return endSnapshotTimestamp;
    }

    public int getTotalSplits() {
        return totalSplits;
    }

    public boolean isAllDone() {
        return allDone;
    }

    public void markAllDone() {
        this.allDone = true;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(startSnapshotId);
        out.writeLong(endSnapshotId);
        out.writeLong(endSnapshotTimestamp);
        out.writeInt(totalSplits);
    }

    public static IcebergSplitMeta fromDataInput(DataInput in) throws IOException {
        long startSnapshotId = in.readLong();
        long endSnapshotId = in.readLong();
        long endSnapshotTimestamp = in.readLong();
        int totalSplits = in.readInt();
        return new IcebergSplitMeta(startSnapshotId, endSnapshotId, endSnapshotTimestamp, totalSplits);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IcebergSplitMeta that = (IcebergSplitMeta) o;
        return startSnapshotId == that.startSnapshotId && endSnapshotId == that.endSnapshotId
                && endSnapshotTimestamp == that.endSnapshotTimestamp && totalSplits == that.totalSplits;
    }

    @Override
    public int hashCode() {
        return Objects.hash(startSnapshotId, endSnapshotId, endSnapshotTimestamp, totalSplits);
    }

    @Override
    public String toString() {
        return "IcebergSplitMeta{" +
                "startSnapshotId=" + startSnapshotId +
                ", endSnapshotId=" + endSnapshotId +
                ", endSnapshotTimestamp=" + endSnapshotTimestamp +
                ", totalSplits=" + totalSplits +
                '}';
    }
}
