// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.load.routineload;

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.starrocks.common.FeMetaVersion;
import com.starrocks.common.Pair;
import com.starrocks.common.UserException;
import com.starrocks.common.io.Text;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TPulsarRLTaskProgress;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.pulsar.client.api.MessageId;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * this is description of pulsar routine load progress
 * the data before position was already loaded in StarRocks
 */
public class PulsarProgress extends RoutineLoadProgress {
    private static final Logger LOG = LogManager.getLogger(PulsarProgress.class);

    // (partition, backlog num)
    private Map<String, Long> partitionToBacklogNum = Maps.newConcurrentMap();
    private Map<String, MessageId> partitionToInitialPosition = Maps.newConcurrentMap();

    public PulsarProgress() {
        super(LoadDataSourceType.PULSAR);
    }

    public PulsarProgress(TPulsarRLTaskProgress tPulsarRLTaskProgress) throws UserException {
        super(LoadDataSourceType.PULSAR);
        this.partitionToBacklogNum = tPulsarRLTaskProgress.getPartitionBacklogNum();
        for (Map.Entry<String, ByteBuffer> initialPosition : tPulsarRLTaskProgress.getPartitionInitialPositions()
                .entrySet()) {
            try {
                partitionToInitialPosition.put(initialPosition.getKey(),
                        MessageId.fromByteArray(initialPosition.getValue().array()));
            } catch (IOException e) {
                throw new UserException(
                        "Failed to deserialize messageId for partition: " + initialPosition.getKey(), e);
            }
        }
    }

    public Map<String, MessageId> getPartitionToInitialPosition(List<String> partitions) {
        // TODO: tmp code for compatibility
        for (String partition : partitions) {
            if (!partitionToInitialPosition.containsKey(partition)) {
                partitionToInitialPosition.put(partition, MessageId.latest);
            }
        }

        Map<String, MessageId> result = Maps.newHashMap();
        for (Map.Entry<String, MessageId> entry : partitionToInitialPosition.entrySet()) {
            for (String partition : partitions) {
                if (entry.getKey().equals(partition)) {
                    result.put(partition, entry.getValue());
                }
            }
        }
        return result;
    }

    public List<Long> getBacklogNums() {
        return new ArrayList<Long>(partitionToBacklogNum.values());
    }

    public MessageId getInitialPositionByPartition(String partition) {
        // TODO: tmp code for compatibility
        if (!partitionToInitialPosition.containsKey(partition)) {
            partitionToInitialPosition.put(partition, MessageId.latest);
        }
        return partitionToInitialPosition.get(partition);
    }

    public boolean containsPartition(String pulsarPartition) {
        return this.partitionToInitialPosition.containsKey(pulsarPartition);
    }

    public void addPartitionToInitialPosition(Pair<String, MessageId> partitionToInitialPosition) {
        this.partitionToInitialPosition.put(partitionToInitialPosition.first, partitionToInitialPosition.second);
    }

    public void modifyInitialPositions(List<Pair<String, MessageId>> partitionInitialPositions) {
        for (Pair<String, MessageId> pair : partitionInitialPositions) {
            this.partitionToInitialPosition.put(pair.first, pair.second);
        }
    }

    private void getReadableProgress(Map<String, String> showPartitionIdToPosition) {
        for (Map.Entry<String, Long> entry : partitionToBacklogNum.entrySet()) {
            showPartitionIdToPosition.put(entry.getKey() + "(BacklogNum)", String.valueOf(entry.getValue()));
        }
    }

    @Override
    public String toString() {
        Map<String, String> showPartitionToBacklogNum = Maps.newHashMap();
        getReadableProgress(showPartitionToBacklogNum);
        return "PulsarProgress [partitionToBacklogNum="
                + Joiner.on("|").withKeyValueSeparator("_").join(showPartitionToBacklogNum) + "]";
    }

    @Override
    public String toJsonString() {
        Map<String, String> showPartitionToBacklogNum = Maps.newHashMap();
        getReadableProgress(showPartitionToBacklogNum);
        Gson gson = new Gson();
        return gson.toJson(showPartitionToBacklogNum);
    }

    @Override
    public void update(RLTaskTxnCommitAttachment attachment) {
        PulsarProgress newProgress = (PulsarProgress) attachment.getProgress();
        this.partitionToBacklogNum.putAll(newProgress.partitionToBacklogNum);
        this.partitionToInitialPosition.putAll(newProgress.partitionToInitialPosition);
        LOG.debug("update pulsar progress: {}, task: {}, job: {}",
                newProgress.toJsonString(), DebugUtil.printId(attachment.getTaskId()), attachment.getJobId());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeInt(partitionToInitialPosition.size());
        for (Map.Entry<String, MessageId> entry : partitionToInitialPosition.entrySet()) {
            Text.writeString(out, entry.getKey());
            byte[] messageId = entry.getValue().toByteArray();
            out.writeInt(messageId.length);
            out.write(messageId, 0, messageId.length);
        }
    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        if (GlobalStateMgr.getCurrentStateJournalVersion() < FeMetaVersion.VERSION_95) {
            int backlogSize = in.readInt();
            partitionToBacklogNum = new HashMap<>();
            for (int i = 0; i < backlogSize; i++) {
                partitionToBacklogNum.put(Text.readString(in), in.readLong());
            }
        }

        if (GlobalStateMgr.getCurrentStateJournalVersion() >= FeMetaVersion.VERSION_95) {
            int positionSize = in.readInt();
            partitionToInitialPosition = new HashMap<>();
            for (int i = 0; i < positionSize; i++) {
                String partition = Text.readString(in);
                int length = in.readInt();
                byte[] messageId = new byte[length];
                in.readFully(messageId, 0, length);
                partitionToInitialPosition.put(partition, MessageId.fromByteArray(messageId));
            }
        }
    }
}
