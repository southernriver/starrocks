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

package com.starrocks.catalog;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class ReplicaAssignment implements Writable {
    public static final ReplicaAssignment DEFAULT_ALLOCATION;
    // represent that replica allocation is not set.
    public static final ReplicaAssignment NOT_SET;

    static {
        DEFAULT_ALLOCATION = new ReplicaAssignment((short) 3);
        NOT_SET = new ReplicaAssignment();
    }

    @SerializedName(value = "assignMap")
    private Map<String, Short> assignMap = Maps.newHashMap();

    public ReplicaAssignment() {

    }

    // For convert the old replica number to replica allocation
    public ReplicaAssignment(short replicaNum) {
        assignMap.put(ResourceGroup.DEFAULT_RESOURCE_GROUP_NAME, replicaNum);
    }

    public ReplicaAssignment(Map<String, Short> assignMap) {
        this.assignMap = assignMap;
    }

    public void put(String tag, Short num) {
        this.assignMap.put(tag, num);
    }

    public Map<String, Short> getAssignMap() {
        return assignMap;
    }

    public short getTotalReplicaNum() {
        short num = 0;
        for (Short s : assignMap.values()) {
            num += s;
        }
        return num;
    }

    public boolean isEmpty() {
        return assignMap.isEmpty();
    }

    public boolean isNotSet() {
        return this.equals(NOT_SET);
    }

    public Short getReplicaNumByString(String tag) {
        return assignMap.getOrDefault(tag, (short) 0);
    }

    public static ReplicaAssignment read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, ReplicaAssignment.class);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ReplicaAssignment that = (ReplicaAssignment) o;
        return that.assignMap.equals(this.assignMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(assignMap);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    @Override
    public String toString() {
        return toCreateStmt();
    }

    // For show create table stmt. like:
    // "tag.location.zone1: 2, tag.location.zone2: 1"
    public String toCreateStmt() {
        List<String> tags = Lists.newArrayList();
        for (Map.Entry<String, Short> entry : assignMap.entrySet()) {
            tags.add(entry.getKey() + ": " + entry.getValue());
        }
        return Joiner.on(", ").join(tags);
    }
}
