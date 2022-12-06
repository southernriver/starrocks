// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/load/routineload/TubeTaskInfo.java

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
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.UserException;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.thrift.TExecPlanFragmentParams;
import com.starrocks.thrift.TFileFormatType;
import com.starrocks.thrift.TLoadSourceType;
import com.starrocks.thrift.TPlanFragment;
import com.starrocks.thrift.TRoutineLoadTask;
import com.starrocks.thrift.TTubeLoadInfo;
import com.starrocks.thrift.TUniqueId;

import java.util.UUID;

public class TubeTaskInfo extends RoutineLoadTaskInfo {
    private RoutineLoadManager routineLoadManager = Catalog.getCurrentCatalog().getRoutineLoadManager();

    private String filters = null;
    private Integer consumePosition = null;

    public TubeTaskInfo(UUID id, long jobId, String clusterName, long taskScheduleIntervalMs, long timeToExecuteMs,
                        String filters, Integer consumePosition) {
        super(id, jobId, clusterName, taskScheduleIntervalMs, timeToExecuteMs);
        this.filters = filters;
        this.consumePosition = consumePosition;
    }

    public TubeTaskInfo(long timeToExecuteMs, TubeTaskInfo tubeTaskInfo) {
        super(UUID.randomUUID(), tubeTaskInfo.getJobId(), tubeTaskInfo.getClusterName(),
                tubeTaskInfo.getTaskScheduleIntervalMs(), timeToExecuteMs, tubeTaskInfo.getBeId(),
                tubeTaskInfo.getStatistics());
        this.filters = tubeTaskInfo.getFilters();
    }

    public String getFilters() {
        return filters;
    }

    public int getConsumePosition() {
        return consumePosition;
    }

    // There no proper way to find out if tube got messages to consume.
    @Override
    public boolean readyToExecute() {
        return true;
    }

    @Override
    public boolean isProgressKeepUp(RoutineLoadProgress progress) {
        return true;
    }

    @Override
    public TRoutineLoadTask createRoutineLoadTask() throws UserException {
        TubeRoutineLoadJob routineLoadJob = (TubeRoutineLoadJob) routineLoadManager.getJob(jobId);

        // init tRoutineLoadTask and create plan fragment
        TRoutineLoadTask tRoutineLoadTask = new TRoutineLoadTask();
        TUniqueId queryId = new TUniqueId(id.getMostSignificantBits(), id.getLeastSignificantBits());
        tRoutineLoadTask.setId(queryId);
        tRoutineLoadTask.setJob_id(jobId);
        tRoutineLoadTask.setTxn_id(txnId);
        Database database = Catalog.getCurrentCatalog().getDb(routineLoadJob.getDbId());
        if (database == null) {
            throw new MetaNotFoundException("database " + routineLoadJob.getDbId() + " does not exist");
        }
        tRoutineLoadTask.setDb(database.getFullName());
        Table tbl = database.getTable(routineLoadJob.getTableId());
        if (tbl == null) {
            throw new MetaNotFoundException("table " + routineLoadJob.getTableId() + " does not exist");
        }
        tRoutineLoadTask.setTbl(tbl.getName());
        // label = job_name+job_id+task_id+txn_id
        String label =
                Joiner.on("-").join(routineLoadJob.getName(), routineLoadJob.getId(), DebugUtil.printId(id), txnId);
        tRoutineLoadTask.setLabel(label);
        tRoutineLoadTask.setAuth_code(routineLoadJob.getAuthCode());
        TTubeLoadInfo tTubeLoadInfo = new TTubeLoadInfo();
        tTubeLoadInfo.setMaster_addr((routineLoadJob).getMasterAddr());
        tTubeLoadInfo.setTopic((routineLoadJob).getTopic());
        tTubeLoadInfo.setGroup_name((routineLoadJob).getGroupName());
        if (filters != null) {
            tTubeLoadInfo.setFilters(getFilters());
        }
        if (consumePosition != null) {
            tTubeLoadInfo.setConsume_position(getConsumePosition());
        }
        tRoutineLoadTask.setTube_load_info(tTubeLoadInfo);
        tRoutineLoadTask.setType(TLoadSourceType.TUBE);
        tRoutineLoadTask.setParams(plan(routineLoadJob));
        tRoutineLoadTask.setMax_interval_s(Config.routine_load_task_consume_second);
        tRoutineLoadTask.setMax_batch_rows(routineLoadJob.getMaxBatchRows());
        tRoutineLoadTask.setMax_batch_size(Config.max_routine_load_batch_size);
        if (!routineLoadJob.getFormat().isEmpty() && routineLoadJob.getFormat().equalsIgnoreCase("json")) {
            tRoutineLoadTask.setFormat(TFileFormatType.FORMAT_JSON);
        } else {
            tRoutineLoadTask.setFormat(TFileFormatType.FORMAT_CSV_PLAIN);
        }
        return tRoutineLoadTask;
    }

    @Override
    protected String getTaskDataSourceProperties() {
        if (consumePosition != null) {
            return "ConsumePositison: " + consumePosition;
        } else {
            return new String();
        }
    }

    @Override
    public String toString() {
        return "Task id: " + getId() + ", " + getTaskDataSourceProperties();
    }

    private TExecPlanFragmentParams plan(RoutineLoadJob routineLoadJob) throws UserException {
        TUniqueId loadId = new TUniqueId(id.getMostSignificantBits(), id.getLeastSignificantBits());
        // plan for each task, in case table has change(rollup or schema change)
        TExecPlanFragmentParams tExecPlanFragmentParams = routineLoadJob.plan(loadId, txnId);
        TPlanFragment tPlanFragment = tExecPlanFragmentParams.getFragment();
        tPlanFragment.getOutput_sink().getOlap_table_sink().setTxn_id(txnId);
        return tExecPlanFragmentParams;
    }
}
