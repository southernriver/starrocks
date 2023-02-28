// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/load/routineload/TubeRoutineLoadJob.java

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

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.starrocks.analysis.RoutineLoadDataSourceProperties;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.UserException;
import com.starrocks.common.io.Text;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreateRoutineLoadStmt;
import com.starrocks.system.SystemInfoService;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TransactionStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * TubeRoutineLoadJob is a kind of RoutineLoadJob which fetch data from tube.
 */
public class TubeRoutineLoadJob extends RoutineLoadJob {
    private static final Logger LOG = LogManager.getLogger(TubeRoutineLoadJob.class);

    private String masterAddr;
    private String topic;
    private String groupName;
    // optional, user want to specify filters.
    private String filters = null;
    // optional, user want to specify consume position.
    private Integer consumePosition = null;

    public static final String EMPTY_FILTER = "EMPTY_FILTER";

    public TubeRoutineLoadJob() {
        // for serialization, id is dummy
        super(-1, LoadDataSourceType.TUBE);
    }

    public TubeRoutineLoadJob(Long id, String name, long dbId, long tableId, String masterAddr, String topic,
                              String groupName) {
        super(id, name, dbId, tableId, LoadDataSourceType.TUBE);
        this.masterAddr = masterAddr;
        this.topic = topic;
        this.groupName = groupName;
        this.progress = new TubeProgress();
    }

    public String getTopic() {
        return topic;
    }

    public String getMasterAddr() {
        return masterAddr;
    }

    public String getGroupName() {
        return groupName;
    }

    @Override
    public void divideRoutineLoadJob(int currentConcurrentTaskNum) throws UserException {
        List<RoutineLoadTaskInfo> result = new ArrayList<>();
        writeLock();
        try {
            if (state == JobState.NEED_SCHEDULE) {
                for (int i = 0; i < currentConcurrentTaskNum; i++) {
                    long timeToExecuteMs = System.currentTimeMillis() + taskSchedIntervalS * 1000;
                    TubeTaskInfo tubeTaskInfo =
                            new TubeTaskInfo(UUID.randomUUID(), id, taskSchedIntervalS * 1000, timeToExecuteMs, filters,
                                    consumePosition);
                    LOG.debug("tube routine load task created: " + tubeTaskInfo);
                    routineLoadTaskInfoList.add(tubeTaskInfo);
                    result.add(tubeTaskInfo);
                }
                if (result.size() != 0) {
                    // change job state to running
                    unprotectUpdateState(JobState.RUNNING, null, false);
                    // reset consume position
                    consumePosition = null;
                }
            } else {
                LOG.debug("Ignore to divide routine load job while job state {}", state);
            }
            // save task into queue of needScheduleTasks
            GlobalStateMgr.getCurrentState().getRoutineLoadTaskScheduler().addTasksInQueue(result);
        } finally {
            writeUnlock();
        }
    }

    @Override
    public int calculateCurrentConcurrentTaskNum() throws MetaNotFoundException {
        SystemInfoService systemInfoService = GlobalStateMgr.getCurrentSystemInfo();
        int aliveBeNum = systemInfoService.getAliveBackendNumber();
        if (desireTaskConcurrentNum == 0) {
            desireTaskConcurrentNum = Config.max_routine_load_task_concurrent_num;
        }

        LOG.debug("current concurrent task number is min" +
                        "(desire task concurrent num: {}, alive be num: {}, config: {})", desireTaskConcurrentNum, aliveBeNum,
                Config.max_routine_load_task_concurrent_num);
        currentTaskConcurrentNum =
                Math.min(Math.min(desireTaskConcurrentNum, aliveBeNum), Config.max_routine_load_task_concurrent_num);
        return currentTaskConcurrentNum;
    }

    // Through the transaction status and attachment information, to determine whether the progress needs to be updated.
    @Override
    protected boolean checkCommitInfo(RLTaskTxnCommitAttachment rlTaskTxnCommitAttachment, TransactionState txnState,
                                      TransactionState.TxnStatusChangeReason txnStatusChangeReason) {
        if (txnState.getTransactionStatus() == TransactionStatus.COMMITTED) {
            // For committed txn, update the progress.
            return true;
        }

        // For compatible reason, the default behavior of empty load is still returning "all partitions have no load data" and abort transaction.
        // In this situation, we also need update commit info.
        if (txnStatusChangeReason != null &&
                txnStatusChangeReason == TransactionState.TxnStatusChangeReason.NO_PARTITIONS) {
            // Because the max_filter_ratio of routine load task is always 1.
            // Therefore, under normal circumstances, routine load task will not return the error "too many filtered rows".
            // If no data is imported, the error "all partitions have no load data" may only be returned.
            // In this case, the status of the transaction is ABORTED,
            // but we still need to update the position to skip these error lines.
            Preconditions.checkState(txnState.getTransactionStatus() == TransactionStatus.ABORTED,
                    txnState.getTransactionStatus());
            return true;
        }

        // Running here, the status of the transaction should be ABORTED,
        // and it is caused by other errors. In this case, we should not update the position.
        LOG.debug("no need to update the progress of tube routine load. txn status: {}, " +
                        "txnStatusChangeReason: {}, task: {}, job: {}", txnState.getTransactionStatus(), txnStatusChangeReason,
                DebugUtil.printId(rlTaskTxnCommitAttachment.getTaskId()), id);
        return false;
    }

    @Override
    protected void updateProgress(RLTaskTxnCommitAttachment attachment) throws UserException {
        super.updateProgress(attachment);
        this.progress.update(attachment);
    }

    @Override
    protected void replayUpdateProgress(RLTaskTxnCommitAttachment attachment) {
        super.replayUpdateProgress(attachment);
        this.progress.update(attachment);
        // reset consumePostion anyway, this will only be called when tasks been committed
        consumePosition = null;
    }

    @Override
    protected RoutineLoadTaskInfo unprotectRenewTask(long timeToExecuteMs, RoutineLoadTaskInfo routineLoadTaskInfo) {
        TubeTaskInfo oldTubeTaskInfo = (TubeTaskInfo) routineLoadTaskInfo;
        // add new task
        TubeTaskInfo tubeTaskInfo = new TubeTaskInfo(timeToExecuteMs, oldTubeTaskInfo);
        // remove old task
        routineLoadTaskInfoList.remove(routineLoadTaskInfo);
        // add new task
        routineLoadTaskInfoList.add(tubeTaskInfo);
        LOG.debug("tube routine load task renewed: " + tubeTaskInfo);
        return tubeTaskInfo;
    }

    @Override
    protected String getStatistic() {
        Map<String, Object> summary = Maps.newHashMap();
        summary.put("totalRows", Long.valueOf(totalRows));
        summary.put("loadedRows", Long.valueOf(totalRows - errorRows - unselectedRows));
        summary.put("errorRows", Long.valueOf(errorRows));
        summary.put("unselectedRows", Long.valueOf(unselectedRows));
        summary.put("receivedBytes", Long.valueOf(receivedBytes));
        summary.put("taskExecuteTimeMs", Long.valueOf(totalTaskExcutionTimeMs));
        summary.put("receivedBytesRate", Long.valueOf(receivedBytes / totalTaskExcutionTimeMs * 1000));
        summary.put("loadRowsRate",
                Long.valueOf((totalRows - errorRows - unselectedRows) / totalTaskExcutionTimeMs * 1000));
        summary.put("committedTaskNum", Long.valueOf(committedTaskNum));
        summary.put("abortedTaskNum", Long.valueOf(abortedTaskNum));
        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        return gson.toJson(summary);
    }

    public static TubeRoutineLoadJob fromCreateStmt(CreateRoutineLoadStmt stmt) throws UserException {
        // check db and table
        Database db = GlobalStateMgr.getCurrentState().getDb(stmt.getDBName());
        if (db == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, stmt.getDBName());
        }

        long tableId = -1L;
        db.readLock();
        try {
            unprotectedCheckMeta(db, stmt.getTableName(), stmt.getRoutineLoadDesc());
            Table table = db.getTable(stmt.getTableName());
            tableId = table.getId();
        } finally {
            db.readUnlock();
        }

        // init tube routine load job
        long id = GlobalStateMgr.getCurrentState().getNextId();
        TubeRoutineLoadJob tubeRoutineLoadJob =
                new TubeRoutineLoadJob(id, stmt.getName(), db.getId(), tableId, stmt.getTubeMasterAddr(),
                        stmt.getTubeTopic(), stmt.getTubeGroupName());
        tubeRoutineLoadJob.setOptional(stmt);
        return tubeRoutineLoadJob;
    }

    @Override
    protected void setOptional(CreateRoutineLoadStmt stmt) throws UserException {
        super.setOptional(stmt);

        if (stmt.getTubeFilters() != null) {
            setFilters(stmt.getTubeFilters());
        }

        if (stmt.getTubeConsumePosition() != null) {
            setConsumePosition(stmt.getTubeConsumePosition());
        }
    }

    protected void setFilters(String filters) {
        this.filters = filters;
    }

    protected void setConsumePosition(int consumePosition) {
        this.consumePosition = consumePosition;
    }

    @Override
    protected String dataSourcePropertiesJsonToString() {
        Map<String, String> dataSourceProperties = Maps.newHashMap();
        dataSourceProperties.put("MasterAddr", masterAddr);
        dataSourceProperties.put("topic", topic);
        dataSourceProperties.put("GroupName", groupName);
        if (filters != null) {
            dataSourceProperties.put("Filters", filters);
        }
        if (consumePosition != null) {
            dataSourceProperties.put("ConsumePosition", String.valueOf(consumePosition));
        }
        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        return gson.toJson(dataSourceProperties);
    }

    @Override
    String customPropertiesJsonToString() {
        return new String();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        Text.writeString(out, masterAddr);
        Text.writeString(out, topic);
        Text.writeString(out, groupName);
        if (filters == null) {
            Text.writeString(out, EMPTY_FILTER);
        } else {
            Text.writeString(out, filters);
        }
    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        masterAddr = Text.readString(in);
        topic = Text.readString(in);
        groupName = Text.readString(in);
        filters = Text.readString(in);
        if (filters.equals(EMPTY_FILTER)) {
            filters = null;
        }
    }

    @Override
    public void modifyDataSourceProperties(RoutineLoadDataSourceProperties dataSourceProperties) throws DdlException {
        Integer consumePosition = null;

        if (dataSourceProperties.hasAnalyzedProperties()) {
            consumePosition = dataSourceProperties.getTubeConsumePosition();
        }

        if (consumePosition != null) {
            this.consumePosition = consumePosition;
        }

        LOG.info("modify the data source properties of tube routine load job: {}, datasource properties: {}", this.id,
                dataSourceProperties);
    }
}
