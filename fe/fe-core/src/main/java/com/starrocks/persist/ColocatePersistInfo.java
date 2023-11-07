// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/persist/ColocatePersistInfo.java

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

package com.starrocks.persist;

import com.clearspring.analytics.util.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.ColocateTableIndex.GroupId;
import com.starrocks.catalog.ResourceGroup;
import com.starrocks.common.FeMetaVersion;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * PersistInfo for ColocateTableIndex
 */
public class ColocatePersistInfo implements Writable {
    @SerializedName(value = "groupId")
    private GroupId groupId;
    @SerializedName(value = "tableId")
    private long tableId;
    @SerializedName(value = "backendsPerBucketSeq")
    private Map<String, List<List<Long>>> backendsPerBucketSeq = Maps.newHashMap();

    public ColocatePersistInfo() {

    }

    public static ColocatePersistInfo createForAddTable(GroupId groupId, long tableId,
                                                        Map<String, List<List<Long>>> backendsPerBucketSeq) {
        return new ColocatePersistInfo(groupId, tableId, backendsPerBucketSeq);
    }

    public static ColocatePersistInfo createForBackendsPerBucketSeq(GroupId groupId,
                                                                    Map<String, List<List<Long>>> backendsPerBucketSeq) {
        return new ColocatePersistInfo(groupId, -1L, backendsPerBucketSeq);
    }

    public static ColocatePersistInfo createForMarkUnstable(GroupId groupId) {
        return new ColocatePersistInfo(groupId, -1L, Maps.newHashMap());
    }

    public static ColocatePersistInfo createForMarkStable(GroupId groupId) {
        return new ColocatePersistInfo(groupId, -1L, Maps.newHashMap());
    }

    public static ColocatePersistInfo createForRemoveTable(long tableId) {
        return new ColocatePersistInfo(new GroupId(-1, -1), tableId, Maps.newHashMap());
    }

    private ColocatePersistInfo(GroupId groupId, long tableId, Map<String, List<List<Long>>> backendsPerBucketSeq) {
        this.groupId = groupId;
        this.tableId = tableId;
        this.backendsPerBucketSeq = backendsPerBucketSeq;
    }

    public long getTableId() {
        return tableId;
    }

    public GroupId getGroupId() {
        return groupId;
    }

    public Map<String, List<List<Long>>> getBackendsPerBucketSeq() {
        return backendsPerBucketSeq;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public void readFields(DataInput in) throws IOException {
        if (GlobalStateMgr.getCurrentStateJournalVersion() < FeMetaVersion.VERSION_97) {
            tableId = in.readLong();
            if (GlobalStateMgr.getCurrentStateJournalVersion() < FeMetaVersion.VERSION_55) {
                long grpId = in.readLong();
                long dbId = in.readLong();
                groupId = new GroupId(dbId, grpId);
            } else {
                groupId = GroupId.read(in);
            }

            int size = in.readInt();
            backendsPerBucketSeq = Maps.newHashMap();
            List<List<Long>> backendsPerBucketSeqList = Lists.newArrayList();
            backendsPerBucketSeq.put(ResourceGroup.DEFAULT_RESOURCE_GROUP_NAME, backendsPerBucketSeqList);
            for (int i = 0; i < size; i++) {
                int beListSize = in.readInt();
                List<Long> beLists = new ArrayList<>();
                for (int j = 0; j < beListSize; j++) {
                    beLists.add(in.readLong());
                }
                backendsPerBucketSeqList.add(beLists);
            }
        } else {
            String json = Text.readString(in);
            ColocatePersistInfo colocatePersistInfo = GsonUtils.GSON.fromJson(json, ColocatePersistInfo.class);
            tableId = colocatePersistInfo.getTableId();
            groupId = colocatePersistInfo.groupId;
            backendsPerBucketSeq = colocatePersistInfo.getBackendsPerBucketSeq();
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        if (!(obj instanceof ColocatePersistInfo)) {
            return false;
        }

        ColocatePersistInfo info = (ColocatePersistInfo) obj;

        return tableId == info.tableId
                && groupId.equals(info.groupId)
                && backendsPerBucketSeq.equals(info.backendsPerBucketSeq);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("table id: ").append(tableId);
        sb.append(" group id: ").append(groupId);
        sb.append(" backendsPerBucketSeq: ").append(backendsPerBucketSeq);
        return sb.toString();
    }
}
