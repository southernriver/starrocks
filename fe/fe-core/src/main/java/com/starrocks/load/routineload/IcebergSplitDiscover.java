// Licensed to the Apache Software Foundation (ASF) under one

package com.starrocks.load.routineload;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.connector.iceberg.IcebergUtil;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.TableScanUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class IcebergSplitDiscover {
    private static final Logger LOG = LogManager.getLogger(IcebergSplitDiscover.class);
    private static final ScheduledExecutorService SCHEDULED_EXECUTOR_SERVICE = Executors
            .newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setDaemon(true)
                    .setNameFormat("iceberg-routine-load-split-scheduler").build());
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

    private final String jobName;
    private final AtomicBoolean stopped = new AtomicBoolean(false);
    private final AtomicLong scheduleSeqId = new AtomicLong();
    private final org.apache.iceberg.Table iceTbl; // actual iceberg table
    private final IcebergProgress icebergProgress;
    private final String icebergConsumePosition;
    private final Long readIcebergSnapshotsAfterTimestamp;
    private final Queue<Pair<IcebergSplitMeta, CombinedScanTask>> splitsQueue;
    private Expression whereExpr;
    private int currentConcurrentTaskNum;
    private long splitSize;

    public IcebergSplitDiscover(String jobName, Table iceTbl,
                                IcebergProgress icebergProgress, String icebergConsumePosition,
                                Long readIcebergSnapshotsAfterTimestamp,
                                Queue<Pair<IcebergSplitMeta, CombinedScanTask>> splitsQueue) {
        this.jobName = jobName;
        this.iceTbl = iceTbl;
        this.icebergProgress = icebergProgress;
        this.icebergConsumePosition = icebergConsumePosition;
        this.readIcebergSnapshotsAfterTimestamp = readIcebergSnapshotsAfterTimestamp;
        this.splitsQueue = splitsQueue;
    }

    public void setWhereExpr(Expression whereExpr) {
        this.whereExpr = whereExpr;
    }

    public void setCurrentConcurrentTaskNum(int currentConcurrentTaskNum) {
        this.currentConcurrentTaskNum = currentConcurrentTaskNum;
    }

    public void setSplitSize(long splitSize) {
        this.splitSize = splitSize;
    }

    private void scheduleCheckAndAddSplits(long expectedScheduleSeqId) {
        lock.writeLock().lock();
        try {
            if (stopped.get() || expectedScheduleSeqId != scheduleSeqId.get()) {
                LOG.info("job {} abort checking splits", jobName);
                return;
            }
            checkAndAddSplits();
            if (stopped.get() || expectedScheduleSeqId != scheduleSeqId.get()) {
                LOG.info("job {} abort checking splits", jobName);
                return;
            }
            SCHEDULED_EXECUTOR_SERVICE.schedule(() -> scheduleCheckAndAddSplits(expectedScheduleSeqId),
                    Config.routine_load_iceberg_split_check_interval_second, TimeUnit.SECONDS);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private CloseableIterable<CombinedScanTask> planTasks(TableScan scan) {
        // the difference between this planTasks() and scan.planTasks() is the sorting of fileScanTasks,
        // so that the result of the planTasks() is idempotent
        CloseableIterable<FileScanTask> fileScanTasks = scan.planFiles();

        List<FileScanTask> sortedFileScanTask = Lists.newArrayList(fileScanTasks);
        sortedFileScanTask.sort(Comparator.comparing(o -> o.file().path().toString()));
        CloseableIterable<FileScanTask> sortedFileScanTasks =
                CloseableIterable.combine(sortedFileScanTask, fileScanTasks);

        CloseableIterable<FileScanTask> splitFiles =
                TableScanUtil.splitFiles(sortedFileScanTasks, scan.targetSplitSize());
        return TableScanUtil.planTasks(
                splitFiles, scan.targetSplitSize(), scan.splitLookback(), scan.splitOpenFileCost());
    }

    private IcebergSplitMeta addToQueue(TableScan scan, long startSnapshotId, long endSnapshotId,
                                        long endSnapshotTimestamp) {
        try (CloseableIterable<CombinedScanTask> tasksIterable = planTasks(scan)) {
            int totalSplits = 0;
            for (CombinedScanTask combinedScanTask : tasksIterable) {
                totalSplits += combinedScanTask.files().size();
            }
            IcebergSplitMeta splitMeta =
                    new IcebergSplitMeta(startSnapshotId, endSnapshotId, endSnapshotTimestamp, totalSplits);
            tasksIterable.forEach(combinedScanTask -> splitsQueue.offer(Pair.create(splitMeta, combinedScanTask)));
            return splitMeta;
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to close table scan: " + scan, e);
        }
    }

    private void addToQueue(TableScan scan, IcebergSplitMeta splitMeta) {
        try (CloseableIterable<CombinedScanTask> tasksIterable = planTasks(scan)) {
            tasksIterable.forEach(combinedScanTask -> splitsQueue.offer(Pair.create(splitMeta, combinedScanTask)));
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to close table scan: " + scan, e);
        }
    }

    private void firstScan(TableScan scan, Snapshot snapshot) {
        if (IcebergCreateRoutineLoadStmtConfig.isFromEarliest(icebergConsumePosition)) {
            long snapshotId = snapshot.snapshotId();
            if (readIcebergSnapshotsAfterTimestamp == null) {
                scan = scan.useSnapshot(snapshotId);
                icebergProgress.setLastSplitMeta(addToQueue(scan, -1, snapshotId, snapshot.timestampMillis()));
            } else {
                scanBetween(scan, snapshot, readIcebergSnapshotsAfterTimestamp);
            }
        } else {
            scanBetween(scan, snapshot, null);
        }
    }

    private void scanBetween(TableScan scan, Snapshot endSnapshot, Long readIcebergSnapshotsAfterTimestamp) {
        long endSnapshotId = endSnapshot.snapshotId();
        Snapshot startSnapshot = findStartSnapshot(readIcebergSnapshotsAfterTimestamp);
        if (startSnapshot == null) {
            return;
        }
        scan = scan.appendsBetween(startSnapshot.snapshotId(), endSnapshotId);
        icebergProgress.setLastSplitMeta(
                addToQueue(scan, startSnapshot.snapshotId(), endSnapshotId, endSnapshot.timestampMillis()));
    }

    private Snapshot findStartSnapshot(Long readIcebergSnapshotsAfterTimestamp) {
        // sorted from most recently to earliest, current snapshot is included
        Iterator<Snapshot> snapshotsAncestors = SnapshotUtil.currentAncestors(iceTbl).iterator();
        // current snapshot
        snapshotsAncestors.next();
        Snapshot startSnapshot = null;
        while (snapshotsAncestors.hasNext()) {
            Snapshot s = snapshotsAncestors.next();
            if (readIcebergSnapshotsAfterTimestamp == null) {
                return s;
            }
            startSnapshot = s;
            if (s.timestampMillis() <= readIcebergSnapshotsAfterTimestamp) {
                break;
            }
        }
        return startSnapshot;
    }

    private TableScan newTableScan() {
        TableScan scan = iceTbl.newScan()
                .includeColumnStats()
                .option(TableProperties.SPLIT_SIZE, "" + splitSize);
        if (whereExpr != null) {
            scan = scan.filter(whereExpr);
        }
        return scan;
    }

    private void checkAndAddSplits() {
        if (splitsQueue.size() > currentConcurrentTaskNum * 50) {
            LOG.warn("iceberg routine load job [job name {}] too many tasks[{} > {}] are running, delay check", jobName,
                    splitsQueue.size(), currentConcurrentTaskNum * 50);
            return;
        }
        IcebergUtil.refreshTable(iceTbl);
        icebergProgress.cleanExpiredSplitRecords();
        Snapshot snapshot = iceTbl.currentSnapshot();
        if (snapshot == null) {
            // no data
            return;
        }
        TableScan scan = newTableScan();
        IcebergSplitMeta lastSplitMeta = icebergProgress.getLastSplitMeta();
        // first time
        if (lastSplitMeta == null) {
            firstScan(scan, snapshot);
            return;
        }
        long snapshotId = snapshot.snapshotId();
        if (lastSplitMeta.getEndSnapshotId() == snapshotId) {
            // no new data
            return;
        }
        scan = scan.appendsBetween(lastSplitMeta.getEndSnapshotId(), snapshotId);
        icebergProgress.setLastSplitMeta(addToQueue(scan, lastSplitMeta.getEndSnapshotId(), snapshotId,
                snapshot.timestampMillis()));
    }

    private void addSplitsFromRecovery(List<IcebergSplitMeta> splitMetas, Snapshot snapshot) {
        if (snapshot == null) {
            // no data
            return;
        }
        TableScan scan = newTableScan();

        // first time
        if (splitMetas == null) {
            firstScan(scan, snapshot);
            return;
        }
        IcebergSplitMeta lastSplitMeta = null;
        for (IcebergSplitMeta splitMeta : splitMetas) {
            if (lastSplitMeta != null && splitMeta.getStartSnapshotId() != lastSplitMeta.getEndSnapshotId()) {
                // usually, the splitMetas are continuous, like[{-1,3},{3,6}]. if not, the gap should be filled
                try {
                    scan = scan.appendsBetween(lastSplitMeta.getEndSnapshotId(), splitMeta.getStartSnapshotId());
                    addToQueue(scan, splitMeta);
                } catch (IllegalArgumentException e) {
                    // range [lastSplitMeta.getEndSnapshotId(), splitMeta.getStartSnapshotId()] is illegal
                    LOG.warn("ignore this range gap: " + e.getMessage(), e);
                }
            }
            lastSplitMeta = splitMeta;
            if (splitMeta.isAllDone()) {
                continue;
            }
            if (splitMeta.getStartSnapshotId() == -1) {
                scan = scan.useSnapshot(splitMeta.getEndSnapshotId());
                addToQueue(scan, splitMeta);
            } else {
                scan = scan.appendsBetween(splitMeta.getStartSnapshotId(), splitMeta.getEndSnapshotId());
                addToQueue(scan, splitMeta);
            }
        }
    }

    public void start() {
        Snapshot snapshot = iceTbl.currentSnapshot();
        stopped.set(false);
        // use new id, so that the old scheduled check can aborted automatically to avoid duplicate check
        long nextId = scheduleSeqId.incrementAndGet();
        lock.writeLock().lock();
        try {
            List<IcebergSplitMeta> splitMetas = icebergProgress.recoverLastSnapshots();
            splitsQueue.clear();
            addSplitsFromRecovery(splitMetas, snapshot);
        } finally {
            lock.writeLock().unlock();
        }
        scheduleCheckAndAddSplits(nextId);
    }

    public void stop() {
        stopped.set(true);
    }
}
