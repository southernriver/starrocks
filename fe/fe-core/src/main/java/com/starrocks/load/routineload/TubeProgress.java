// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/load/routineload/TubeProgress.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.load.routineload;

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.starrocks.common.io.Text;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.thrift.TTubeRLTaskProgress;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * this is description of tube routine load progress
 * the data before position was already loaded in StarRocks
 */
// {"partitionToOffset": {}}
public class TubeProgress extends RoutineLoadProgress {
    private static final Logger LOG = LogManager.getLogger(TubeProgress.class);

    // (partition, offset)
    private Map<String, Long> partitionToOffset = Maps.newConcurrentMap();

    public TubeProgress() {
        super(LoadDataSourceType.TUBE);
    }

    public TubeProgress(TTubeRLTaskProgress tTubeRLTaskProgress) {
        super(LoadDataSourceType.TUBE);
        this.partitionToOffset = tTubeRLTaskProgress.getPartitionCmtOffset();
    }

    private void getReadableProgress(Map<String, String> showPartitionIdToPosition) {
        for (Map.Entry<String, Long> entry : partitionToOffset.entrySet()) {
            showPartitionIdToPosition.put(entry.getKey(), String.valueOf(entry.getValue()));
        }
    }

    @Override
    public String toString() {
        Map<String, String> showPartitionIdToPosition = Maps.newHashMap();
        getReadableProgress(showPartitionIdToPosition);
        return "TubeProgress [partitionToOffset="
                + Joiner.on("|").withKeyValueSeparator("_").join(showPartitionIdToPosition) + "]";
    }

    @Override
    public String toJsonString() {
        Map<String, String> showPartitionIdToPosition = Maps.newHashMap();
        getReadableProgress(showPartitionIdToPosition);
        Gson gson = new Gson();
        return gson.toJson(showPartitionIdToPosition);
    }

    @Override
    public void update(RLTaskTxnCommitAttachment attachment) {
        TubeProgress newProgress = (TubeProgress) attachment.getProgress();
        for (Map.Entry<String, Long> entry : newProgress.partitionToOffset.entrySet()) {
            String partition = entry.getKey();
            Long offset = entry.getValue();
            // Update progress
            this.partitionToOffset.put(partition, offset);
        }
        LOG.debug("update tube progress: {}, task: {}, job: {}",
                newProgress.toJsonString(), DebugUtil.printId(attachment.getTaskId()), attachment.getJobId());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeInt(partitionToOffset.size());
        for (Map.Entry<String, Long> entry : partitionToOffset.entrySet()) {
            Text.writeString(out, entry.getKey());
            out.writeLong((Long) entry.getValue());
        }
    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        int size = in.readInt();
        partitionToOffset = new HashMap<>();
        for (int i = 0; i < size; i++) {
            partitionToOffset.put(Text.readString(in), in.readLong());
        }
    }
}
