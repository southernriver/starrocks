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

import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.starrocks.thrift.TRLTaskStatistics;

import java.util.Map;

public class RoutineLoadTaskStatistics {
    private long consumeTime;
    private long blockingGetTime;
    private long blockingPutTime;
    private long receivedRows;
    private long receivedBytes;
    private Map<String, Long> consumeLags = null;

    public RoutineLoadTaskStatistics(TRLTaskStatistics taskStatistics) {
        this.consumeTime = taskStatistics.consumeTime;
        this.blockingGetTime = taskStatistics.blockingGetTime;
        this.blockingPutTime = taskStatistics.blockingPutTime;
        this.receivedRows = taskStatistics.receivedRows;
        this.receivedBytes = taskStatistics.receivedBytes;
        if (taskStatistics.isSetConsumeLags()) {
            this.consumeLags = taskStatistics.consumeLags;
        }
    }

    public Map<String, Long> getConsumeLags() {
        return consumeLags;
    }

    @Override
    public String toString() {
        Map<String, String> result = Maps.newHashMap();
        result.put("consumeTime(ms)", String.valueOf(consumeTime));
        result.put("blockingGetTime(us)", String.valueOf(blockingGetTime));
        result.put("blockingPutTime(us)", String.valueOf(blockingPutTime));
        result.put("receivedRows", String.valueOf(receivedRows));
        result.put("receivedBytes", String.valueOf(receivedBytes));
        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        return gson.toJson(result);
    }
}
