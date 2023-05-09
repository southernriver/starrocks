// This file is made available under Elastic License 2.0.

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

package com.starrocks.load;

import com.google.gson.Gson;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class PartitionColddownInfo implements Writable {
    private long dbId;
    private long tableId;
    private long partitionId;
    private String jobName;
    private boolean success;
    private boolean triggeredByTtl;
    private long syncedTimeMs;
    private long exportedRowCount;

    public PartitionColddownInfo() {
    }

    public PartitionColddownInfo(long dbId, long tableId, long partitionId, String jobName, boolean success,
                                 boolean triggeredByTtl, long syncedTimeMs, long exportedRowCount) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.partitionId = partitionId;
        this.jobName = jobName;
        this.success = success;
        this.triggeredByTtl = triggeredByTtl;
        this.syncedTimeMs = syncedTimeMs;
        this.exportedRowCount = exportedRowCount;
    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        return tableId;
    }

    public long getPartitionId() {
        return partitionId;
    }

    public String getJobName() {
        return jobName;
    }

    public boolean isSuccess() {
        return success;
    }

    public boolean isTriggeredByTtl() {
        return triggeredByTtl;
    }

    public long getSyncedTimeMs() {
        return syncedTimeMs;
    }

    public long getExportedRowCount() {
        return exportedRowCount;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(dbId);
        out.writeLong(tableId);
        out.writeLong(partitionId);
        Text.writeString(out, jobName);
        out.writeBoolean(success);
        out.writeBoolean(triggeredByTtl);
        out.writeLong(syncedTimeMs);
        out.writeLong(exportedRowCount);
    }

    public void readFields(DataInput in) throws IOException {
        dbId = in.readLong();
        tableId = in.readLong();
        partitionId = in.readLong();
        jobName = Text.readString(in);
        success = in.readBoolean();
        triggeredByTtl = in.readBoolean();
        syncedTimeMs = in.readLong();
        exportedRowCount = in.readLong();
    }

    public String toSimpleString() {
        Map<String, Object> result = new HashMap<>();
        result.put("jobName", jobName);
        result.put("success", success);
        result.put("triggeredByTtl", triggeredByTtl);
        result.put("syncedTimeMs", syncedTimeMs);
        result.put("exportedRowCount", exportedRowCount);
        return new Gson().toJson(result);
    }
}
