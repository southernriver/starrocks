// Licensed to the Apache Software Foundation (ASF) under one

package com.starrocks.load.routineload;

import com.google.common.base.Predicate;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.starrocks.common.Config;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.connector.iceberg.IcebergUtil;
import com.starrocks.thrift.TIcebergRLTaskProgress;
import com.starrocks.thrift.TIcebergRLTaskProgressSplit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class IcebergProgress extends RoutineLoadProgress {
    private static final Logger LOG = LogManager.getLogger(IcebergProgress.class);
    private static final ScheduledExecutorService SCHEDULED_EXECUTOR_SERVICE = Executors
            .newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setDaemon(true)
                    .setNameFormat("iceberg-routine-load-expire-records-scheduler").build());
    private IcebergSplitMeta lastSplitMeta;
    private IcebergSplitMeta lastCheckpointSplitMeta;
    private IcebergRoutineLoadJob job;

    private Map<IcebergSplit, Boolean> splitDoneRecords = Maps.newConcurrentMap();

    public IcebergProgress() {
        super(LoadDataSourceType.ICEBERG);
    }

    public IcebergProgress(RoutineLoadJob job) {
        super(LoadDataSourceType.ICEBERG);
        this.job = (IcebergRoutineLoadJob) job;
        SCHEDULED_EXECUTOR_SERVICE.schedule(this::scheduleCheckAndExpireRecords,
                Config.routine_load_iceberg_split_check_interval_second, TimeUnit.SECONDS);
    }

    public IcebergProgress(TIcebergRLTaskProgress tIcebergRLTaskProgress) {
        super(LoadDataSourceType.ICEBERG);
        for (TIcebergRLTaskProgressSplit split : tIcebergRLTaskProgress.splits) {
            IcebergSplit icebergSplit = new IcebergSplit(split);
            splitDoneRecords.put(icebergSplit, Boolean.TRUE);
        }
    }

    private void scheduleCheckAndExpireRecords() {
        try {
            if (job.getState().isFinalState()) {
                LOG.info("job {} abort progress expiring records", job.getName());
                clear();
                return;
            }
            if (job.getState() == RoutineLoadJob.JobState.PAUSED) {
                SCHEDULED_EXECUTOR_SERVICE.schedule(this::scheduleCheckAndExpireRecords,
                        Config.routine_load_iceberg_split_check_interval_second, TimeUnit.SECONDS);
                return;
            }
            // usually, this fe is follower now
            if (job.getState() == RoutineLoadJob.JobState.NEED_SCHEDULE) {
                LOG.info("job {} has {} records", job.getName(), splitDoneRecords.size());
            }
            cleanExpiredSplitRecords();
            SCHEDULED_EXECUTOR_SERVICE.schedule(this::scheduleCheckAndExpireRecords,
                    Config.routine_load_iceberg_split_check_interval_second, TimeUnit.SECONDS);
        } catch (Exception e) {
            LOG.error("job " + job.getName() + " failed to scheduleCheckAndExpireRecords", e);
            SCHEDULED_EXECUTOR_SERVICE.schedule(this::scheduleCheckAndExpireRecords,
                    Config.routine_load_iceberg_split_check_interval_second, TimeUnit.SECONDS);
        }
    }

    public IcebergSplitMeta getLastSplitMeta() {
        return lastSplitMeta;
    }

    public void setLastSplitMeta(IcebergSplitMeta lastSplitMeta) {
        this.lastSplitMeta = lastSplitMeta;
    }

    private Map<IcebergSplitMeta, List<IcebergSplit>> markSplitMetaDone() {
        Map<IcebergSplitMeta, List<IcebergSplit>> splitsGroupedByMeta = splitDoneRecords.keySet().stream()
                .collect(Collectors.groupingBy(IcebergSplit::getSplitMeta));
        for (Map.Entry<IcebergSplitMeta, List<IcebergSplit>> entry : splitsGroupedByMeta.entrySet()) {
            IcebergSplitMeta splitMeta = entry.getKey();
            if (splitMeta.isAllDone() || splitMeta.getTotalSplits() > entry.getValue().size()) {
                continue;
            }
            boolean done = true;
            for (IcebergSplit split : entry.getValue()) {
                if (!splitDoneRecords.get(split)) {
                    done = false;
                    break;
                }
            }
            if (done) {
                splitMeta.markAllDone();
            }
        }
        return splitsGroupedByMeta;
    }

    public List<IcebergSplitMeta> recoverLastSnapshots() {
        if (splitDoneRecords.isEmpty()) {
            return null;
        }
        List<IcebergSplitMeta> splitMetas = cleanExpiredSplitRecords();
        lastSplitMeta = splitMetas.get(splitMetas.size() - 1);
        return splitMetas;
    }

    public void removeSplitMeta(IcebergSplitMeta splitMeta) {
        Iterator<IcebergSplit> it = splitDoneRecords.keySet().iterator();
        while (it.hasNext()) {
            IcebergSplit split = it.next();
            if (split.getSplitMeta().equals(splitMeta)) {
                splitDoneRecords.remove(split);
            }
        }
    }

    public boolean allDone() {
        if (splitDoneRecords.isEmpty()) {
            return true;
        }
        List<IcebergSplitMeta> splitMetas = cleanExpiredSplitRecords();
        for (IcebergSplitMeta splitMeta : splitMetas) {
            if (!splitMeta.isAllDone()) {
                return false;
            }
        }
        return true;
    }

    private void setLastCheckpointSplitMeta(Iterable<IcebergSplitMeta> splitMetas) {
        // `last` is either last done meta or last running meta
        IcebergSplitMeta last = null;
        for (IcebergSplitMeta splitMeta : splitMetas) {
            last = splitMeta;
            if (splitMeta.isAllDone()) {
                continue;
            }
            // this is the first running splitMeta
            // if verification result is success, then lastCheckpointSplitMeta = this splitMeta

            // no need to do the following verification
            if (splitMeta.getStartSnapshotId() == -1) {
                break;
            }
            org.apache.iceberg.Table table;
            try {
                table = job.getIceTbl();
                IcebergUtil.refreshTable(table);
            } catch (Exception e) {
                LOG.warn(e.getMessage(), e);
                // this method will be called again later, so just break here
                break;
            }
            try {
                // verify it is still a valid range
                table.newScan().appendsBetween(splitMeta.getStartSnapshotId(), splitMeta.getEndSnapshotId());
                break;
            } catch (IllegalArgumentException e) {
                // range [splitMeta.getStartSnapshotId(), splitMeta.getEndSnapshotId()] is illegal
                LOG.warn("ignore this range " + splitMeta + ": " + e.getMessage(), e);
                removeSplitMeta(splitMeta);
            }
        }
        lastCheckpointSplitMeta = last;
    }

    /**
     * return sorted splitMetas
     */
    public synchronized List<IcebergSplitMeta> cleanExpiredSplitRecords() {
        Map<IcebergSplitMeta, List<IcebergSplit>> unsortedSplits = markSplitMetaDone();
        // the order is important to make recovery correct
        Map<IcebergSplitMeta, List<IcebergSplit>> sortedSplits = new TreeMap<>(
                Comparator.comparingLong(IcebergSplitMeta::getEndSnapshotTimestamp));
        sortedSplits.putAll(unsortedSplits);
        List<IcebergSplitMeta> splitMetas = new ArrayList<>(sortedSplits.keySet());
        setLastCheckpointSplitMeta(sortedSplits.keySet());
        for (Map.Entry<IcebergSplitMeta, List<IcebergSplit>> entry : sortedSplits.entrySet()) {
            // keep at least on split meta in memory so that it can recover from records correctly when allDone
            if (entry.getKey().equals(lastCheckpointSplitMeta)) {
                return splitMetas;
            }
            if (!entry.getKey().isAllDone()) {
                return splitMetas;
            }
            for (IcebergSplit split : entry.getValue()) {
                splitDoneRecords.remove(split);
            }
        }
        return splitMetas;
    }

    @Override
    public String toString() {
        return "IcebergProgress [current planned size=" + (lastSplitMeta == null ? 0 : lastSplitMeta.getTotalSplits()) +
                ", splitDoneRecords size=" + splitDoneRecords.size() + ", running size=" +
                splitDoneRecords.values().stream().filter((Predicate<Boolean>) done -> !done).count() + "]";
    }

    @Override
    public String toJsonString() {
        cleanExpiredSplitRecords();
        return toString();
    }

    @Override
    public void update(RLTaskTxnCommitAttachment attachment) {
        IcebergProgress newProgress = (IcebergProgress) attachment.getProgress();
        splitDoneRecords.putAll(newProgress.splitDoneRecords);
        if (LOG.isDebugEnabled()) {
            LOG.debug("update iceberg progress: {}, task: {}, job: {}",
                    newProgress.toString(), DebugUtil.printId(attachment.getTaskId()), attachment.getJobId());
        }
    }

    public Boolean isDone(IcebergSplit split) {
        Boolean done = splitDoneRecords.get(split);
        if (done == null) {
            if (!split.getSplitMeta().equals(lastCheckpointSplitMeta)) {
                // this split meta is cleaned, which means all splits of this split meta are all done
                return true;
            }
            return false;
        }
        return done;
    }

    public Boolean add(IcebergSplit split) {
        return splitDoneRecords.putIfAbsent(split, Boolean.FALSE);
    }

    public void clear() {
        splitDoneRecords.clear();
        lastSplitMeta = null;
        lastCheckpointSplitMeta = null;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        Map<IcebergSplit, Boolean> records = new HashMap<>(splitDoneRecords);
        out.writeInt(records.size());
        for (Map.Entry<IcebergSplit, Boolean> entry : records.entrySet()) {
            entry.getKey().write(out);
            out.writeBoolean(entry.getValue());
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        int size = in.readInt();
        splitDoneRecords = Maps.newConcurrentMap();
        for (int i = 0; i < size; i++) {
            splitDoneRecords.put(IcebergSplit.fromDataInput(in), in.readBoolean());
        }
    }
}
