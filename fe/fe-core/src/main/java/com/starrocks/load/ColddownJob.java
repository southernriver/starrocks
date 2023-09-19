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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.MoreExecutors;
import com.starrocks.analysis.BrokerDesc;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.TableRef;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DynamicPartitionProperty;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.clone.DynamicPartitionScheduler;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.Pair;
import com.starrocks.common.UserException;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.common.util.DynamicPartitionUtil;
import com.starrocks.common.util.LogBuilder;
import com.starrocks.common.util.LogKey;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.load.export.ExportTargetType;
import com.starrocks.load.export.ExternalTableExportConfig;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.ExportStmtAnalyzer;
import com.starrocks.sql.ast.CreateColddownStmt;
import com.starrocks.sql.ast.DropPartitionClause;
import com.starrocks.sql.ast.ExportStmt;
import com.starrocks.sql.ast.LoadStmt;
import com.starrocks.sql.ast.PartitionNames;
import com.starrocks.sql.common.MetaUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class ColddownJob implements Writable {
    private static final Logger LOG = LogManager.getLogger(ColddownJob.class);

    private long id;
    private String name;
    private long dbId;
    private long tableId;
    private BrokerDesc brokerDesc;
    private Map<String, String> properties = Maps.newHashMap();
    private Map<String, String> targetProperties = Maps.newHashMap();
    private TableName tableName;
    private List<String> columnNames;
    private Expr whereExpr;
    private String sql = "";
    private JobState state;
    private long createTimeMs;
    private long finishTimeMs;
    private ExportFailMsg failMsg;
    private String type;
    private Map<Long, Pair<ExportJob, Boolean>> runningExportJobs;
    private AtomicLong totalExportedRows;
    private AtomicLong totalExportedBytes;
    private AtomicLong totalSuccessExportedRows;
    private AtomicLong totalSuccessExportedBytes;
    private long totalSuccessExportJobs;
    private long totalFailedExportJobs;
    private final Object schemaChangeLockObject = new Object();

    public ColddownJob() {
        this.id = -1;
        this.name = null;
        this.dbId = -1;
        this.tableId = -1;
        this.state = JobState.RUNNING;
        this.createTimeMs = System.currentTimeMillis();
        this.finishTimeMs = -1;
        this.failMsg = new ExportFailMsg(ExportFailMsg.CancelType.UNKNOWN, "");
    }

    public ColddownJob(long jobId, String name) {
        this();
        this.id = jobId;
        this.name = name;
    }

    public void setJob(CreateColddownStmt stmt) throws UserException {
        String dbName = stmt.getTblName().getDb();
        Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
        if (db == null) {
            throw new DdlException("Database " + dbName + " does not exist");
        }

        this.brokerDesc = stmt.getBrokerDesc();
        Preconditions.checkNotNull(brokerDesc);

        this.properties = stmt.getProperties();
        this.targetProperties = stmt.getTargetProperties();

        this.columnNames = stmt.getColumnNames();
        this.whereExpr = stmt.getWhereExpr();
        this.type = stmt.getTypeName();

        db.readLock();
        try {
            this.dbId = db.getId();
            Table exportTable = db.getTable(stmt.getTblName().getTbl());
            if (exportTable == null) {
                throw new DdlException("Table " + stmt.getTblName().getTbl() + " does not exist");
            }
            if (exportTable.getType() != Table.TableType.OLAP) {
                throw new DdlException("Table " + stmt.getTblName().getTbl() + " is not OlapTable");
            }
            if (getColddownPartitionEnd() >= 0) {
                throw new DdlException("colddown_partition_end should be negative");
            }
            if (getColddownPartitionStart() >= 0) {
                throw new DdlException("colddown_partition_start should be negative");
            }
            if (getColddownPartitionEnd() < getColddownPartitionStart()) {
                throw new DdlException("colddown_partition_end is smaller than colddown_partition_start");
            }
            if (DynamicPartitionUtil.isDynamicPartitionTable(exportTable)) {
                int ttlNumber = ((OlapTable) exportTable).getTableProperty().getDynamicPartitionProperty().getStart();
                int colddownEnd = getColddownPartitionEnd();
                if (ttlNumber > colddownEnd) {
                    throw new DdlException("colddown_partition_end " + colddownEnd + " is smaller than " +
                            DynamicPartitionProperty.START + " " + ttlNumber);
                }
            }
            this.tableId = exportTable.getId();
            this.tableName = stmt.getTblName();
        } finally {
            db.readUnlock();
        }

        this.sql = stmt.toSql();
        this.runningExportJobs = Maps.newConcurrentMap();
        this.totalExportedRows = new AtomicLong();
        this.totalExportedBytes = new AtomicLong();
        this.totalSuccessExportedRows = new AtomicLong();
        this.totalSuccessExportedBytes = new AtomicLong();
    }

    public long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        return tableId;
    }

    public JobState getState() {
        return state;
    }

    public BrokerDesc getBrokerDesc() {
        return brokerDesc;
    }

    public int getTimeoutSecond() {
        if (properties.containsKey(LoadStmt.TIMEOUT_PROPERTY)) {
            return Integer.parseInt(properties.get(LoadStmt.TIMEOUT_PROPERTY));
        } else {
            // for compatibility, some export job in old version does not have this property. use default.
            return Config.export_task_default_timeout_second;
        }
    }

    public long getColddownWaitSeconds() {
        if (properties.containsKey("colddown_wait_seconds")) {
            return Integer.parseInt(properties.get("colddown_wait_seconds"));
        } else {
            return Config.cold_down_wait_seconds;
        }
    }

    public int getColddownPartitionStart() {
        if (properties.containsKey("colddown_partition_start")) {
            return Integer.parseInt(properties.get("colddown_partition_start"));
        } else {
            return Integer.MIN_VALUE;
        }
    }

    public int getColddownPartitionEnd() {
        if (properties.containsKey("colddown_partition_end")) {
            return Integer.parseInt(properties.get("colddown_partition_end"));
        } else {
            return -1;
        }
    }

    public boolean isAddTargetTableAsColdTable() {
        if (targetProperties.containsKey("add_target_table_as_cold_table")) {
            return Boolean.parseBoolean(targetProperties.get("colddown_partition_start"));
        } else {
            return true;
        }
    }

    public String getTargetTable() {
        ExportTargetType exportTargetType = ExportTargetType.valueOf(type);
        switch (exportTargetType) {
            case EXTERNAL_TABLE:
                return targetProperties.get(ExternalTableExportConfig.EXTERNAL_TABLE);
            default:
                return null;
        }
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public Map<String, String> getTargetProperties() {
        return targetProperties;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public String getWhereSql() {
        return whereExpr == null ? "" : whereExpr.toSql();
    }

    public long getCreateTimeMs() {
        return createTimeMs;
    }

    public long getFinishTimeMs() {
        return finishTimeMs;
    }

    public ExportFailMsg getFailMsg() {
        return failMsg;
    }

    public String getSql() {
        return sql;
    }

    public TableName getTableName() {
        return tableName;
    }

    public String getType() {
        return type;
    }

    public long getTotalExportedRows() {
        return totalExportedRows.get();
    }

    public long getTotalExportedBytes() {
        return totalExportedBytes.get();
    }

    public long getTotalSuccessExportedRows() {
        return totalSuccessExportedRows.get();
    }

    public long getTotalSuccessExportedBytes() {
        return totalSuccessExportedBytes.get();
    }

    public long getTotalSuccessExportJobs() {
        return totalSuccessExportJobs;
    }

    public long getTotalFailedExportJobs() {
        return totalFailedExportJobs;
    }

    public Map<Long, Pair<ExportJob, Boolean>> getRunningExportJobs() {
        return runningExportJobs == null ? Collections.emptyMap() : runningExportJobs;
    }

    public synchronized boolean updateState(JobState newState) {
        return this.updateState(newState, false);
    }

    public synchronized boolean updateState(JobState newState, boolean isReplay) {
        if (isFinalState()) {
            LOG.warn("colddown job state is finished or cancelled");
            return false;
        }

        state = newState;
        switch (newState) {
            case RUNNING:
                break;
            case CANCELLED:
                finishTimeMs = System.currentTimeMillis();
                break;
            default:
                Preconditions.checkState(false, "wrong job state: " + newState.name());
                break;
        }
        if (!isReplay) {
            GlobalStateMgr.getCurrentState().getEditLog().logColddownUpdateState(id, newState);
        }
        return true;
    }

    public synchronized boolean updateProperties(Map<String, String> properties, boolean isReplay) {
        this.properties.putAll(properties);
        if (!isReplay) {
            GlobalStateMgr.getCurrentState().getEditLog()
                    .logColddownAlterProperties(new AlterProperties(id, properties));
        }
        return true;
    }

    public synchronized boolean isFinalState() {
        return state == JobState.CANCELLED;
    }

    public synchronized void cancel(ExportFailMsg.CancelType type, String msg) throws UserException {
        if (isFinalState()) {
            throw new UserException("Colddown job [" + name + "] is already cancelled");
        }

        cancelInternal(type, msg);
    }

    public synchronized void cancelInternal(ExportFailMsg.CancelType type, String msg) {
        if (!updateState(ColddownJob.JobState.CANCELLED)) {
            return;
        }

        if (msg != null) {
            failMsg = new ExportFailMsg(type, msg);
        }
        if (runningExportJobs != null) {
            for (Pair<ExportJob, Boolean> pair : runningExportJobs.values()) {
                try {
                    pair.first.cancel(type, msg);
                } catch (UserException e) {
                    LOG.warn("failed to cancel export job: " + pair.first.getQueryId(), e);
                }
            }
        }

        LOG.info("colddown job cancelled. job: {}", this);
    }

    public boolean preCheck() {
        // check if db and table exist
        Database database = GlobalStateMgr.getCurrentState().getDb(dbId);
        if (database == null) {
            LOG.warn(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, id)
                    .add("db_id", dbId)
                    .add("msg", "The database has been deleted. Change job state to cancelled").build());
            if (!isFinalState()) {
                updateState(JobState.CANCELLED);
                failMsg = new ExportFailMsg(ExportFailMsg.CancelType.UNKNOWN, "db " + dbId + "not exist");
                return false;
            }
            return true;
        }

        // check table belong to database
        database.readLock();
        Table table;
        try {
            table = database.getTable(tableId);
        } finally {
            database.readUnlock();
        }
        if (table == null) {
            LOG.warn(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, id).add("db_id", dbId)
                    .add("table_id", tableId)
                    .add("msg", "The table has been deleted change job state to cancelled").build());
            if (!isFinalState()) {
                updateState(JobState.CANCELLED);
                failMsg = new ExportFailMsg(ExportFailMsg.CancelType.UNKNOWN, "table " + tableId + "not exist");
                return false;
            }
            return true;
        }
        if (table.getType() != Table.TableType.OLAP) {
            return false;
        }
        // TODO: only partitioned table is supported
        return table.isPartitioned();
    }

    public void handleExportDone() {
        // check if export job is done, if it is done, add some information into partition properties
        Iterator<Map.Entry<Long, Pair<ExportJob, Boolean>>> it = runningExportJobs.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Long, Pair<ExportJob, Boolean>> entry = it.next();
            ExportJob job = entry.getValue().first;
            if (!job.isExportDone()) {
                continue;
            }
            LOG.info("colddown job {} export to partition {} is {}", name, job.getPartitions().get(0),
                    job.getState());
            boolean success = job.getState() == ExportJob.JobState.FINISHED;
            addInfoToPartition(entry.getKey(), job.getSnapshotTimeMs(), success, entry.getValue().second,
                    job.getExportedRowCount());
            totalSuccessExportJobs += success ? 1 : 0;
            totalFailedExportJobs += success ? 0 : 1;
            if (success) {
                totalSuccessExportedRows.addAndGet(job.getExportedRowCount());
                totalSuccessExportedBytes.addAndGet(job.getExportedBytesCount());
            }
            it.remove();
        }
    }

    private void addInfoToPartition(long partitionId, long snapshotTime, boolean success, boolean triggeredByTtl,
                                    long exportedRowCount) {
        PartitionColddownInfo info = new PartitionColddownInfo(dbId, tableId, partitionId, name, success,
                triggeredByTtl, snapshotTime, exportedRowCount);
        GlobalStateMgr.getCurrentState().getColddownMgr().addPartitionColddownInfo(info);
    }

    public void startCheck() throws Exception {
        LOG.info("colddown job {} start check", name);
        // check if it is time to submit export job, if true, find partitions to export and submit export job
        colddownPartitions();
        if (!ExportTargetType.EXTERNAL_TABLE.name().equalsIgnoreCase(type)) {
            return;
        }
        applySchemaChange();
    }

    private void applySchemaChange() {
        if (ExternalTableExportConfig.isAutomaticallyUpdateTargetTableSchema(targetProperties) && columnNames == null) {
            ExportJob.getIoExec().submit(() -> {
                try {
                    OlapTable table = (OlapTable) MetaUtils.getTable(dbId, tableId);
                    ExternalTableExportConfig externalTableExportConfig =
                            new ExternalTableExportConfig(tableName, properties, targetProperties, brokerDesc);
                    synchronized (schemaChangeLockObject) {
                        externalTableExportConfig.applySchemaChange(table, columnNames != null);
                    }
                } catch (Exception e) {
                    LOG.error(e.getMessage(), e);
                }
            });
        }
    }

    private void submitExportJob(Partition partition, boolean triggeredByTtl, Map<String, String> properties,
                                 boolean submitToIoThread) {
        final Object current = this;
        ExecutorService exec = submitToIoThread ? ExportJob.getIoExec() : MoreExecutors.newDirectExecutorService();
        exec.submit(() -> {
            try {
                TableRef tableRef = new TableRef(tableName, null, new PartitionNames(false,
                        Collections.singletonList(partition.getName())));
                ExportStmt exportStmt = new ExportStmt(tableRef, columnNames, whereExpr, type, targetProperties,
                        properties, brokerDesc) {
                    @Override
                    protected void checkExternalTable(ExternalTableExportConfig externalTableExportConfig, Table table) {
                        Table externalTable;
                        synchronized (schemaChangeLockObject) {
                            externalTable = externalTableExportConfig.applySchemaChange(table, getColumnNames() == null);
                        }
                        externalTableExportConfig.prepareProperties(table, externalTable, getPartitions().get(0));
                        prepareExternalTableExportProperties(externalTableExportConfig);
                    }
                };
                ExportStmtAnalyzer.ExportAnalyzerVisitor.visitExportStatement(exportStmt,
                        GlobalStateMgr.getCurrentState(), MetaUtils.getTable(tableName));
                UUID queryId = UUID.randomUUID();

                // lock to ensure runningExportJobs can be judged correctly
                synchronized (current) {
                    Pair<ExportJob, Boolean> runningExportJob = runningExportJobs.get(partition.getId());
                    if (runningExportJob != null) {
                        return null;
                    }
                    ExportJob exportJob = GlobalStateMgr.getCurrentState().getExportMgr().addExportJob(queryId, exportStmt);
                    exportJob.setTotalExportedRowCount(totalExportedRows);
                    exportJob.setTotalExportedBytesCount(totalExportedBytes);
                    runningExportJobs.put(partition.getId(), Pair.create(exportJob, triggeredByTtl));
                }
            } catch (Exception e) {
                LOG.error("failed to run export job for partition " + partition.getName() + " of colddown job " + name, e);
            }
            return null;
        });
    }

    private List<Partition> getPartitionsForColddown(long dbId, OlapTable table) throws DdlException {
        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        if (db == null) {
            return Collections.emptyList();
        }
        RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) table.getPartitionInfo();
        // it is ensured that there is only one partition column when create colddown job
        Column partitionColumn = rangePartitionInfo.getPartitionColumns().get(0);
        DynamicPartitionProperty dynamicPartitionProperty =
                table.getTableProperty().getDynamicPartitionProperty();
        String format = DynamicPartitionUtil.getPartitionFormat(partitionColumn, dynamicPartitionProperty.getTimeUnit());
        int lowerBoundOffset = getColddownPartitionEnd() + 1;
        return DynamicPartitionScheduler.getDropPartitionClause(db, table, partitionColumn, format, lowerBoundOffset, 0)
                .stream()
                .map(DropPartitionClause::getPartition)
                .collect(Collectors.toList());
    }

    private void colddownPartitions() throws Exception {
        OlapTable table = (OlapTable) MetaUtils.getTable(dbId, tableId);
        List<Partition> partitions = getPartitionsForColddown(dbId, table);
        partitions = Lists.reverse(partitions);
        int rangeSize = getColddownPartitionEnd() - getColddownPartitionStart() + 1;
        // rangeSize can be negative due to integer overflow
        if (rangeSize > 0 && rangeSize < partitions.size()) {
            partitions = partitions.subList(0, rangeSize);
        }
        if (partitions.isEmpty()) {
            LOG.info("no partition of job {} need to be colddown", name);
            return;
        }
        for (Partition partition : partitions) {
            submitColddownPartition(partition, false, true, false, true, properties);
        }
    }

    public boolean submitColddownPartition(String partition, Map<String, String> properties) throws Exception {
        OlapTable table = (OlapTable) MetaUtils.getTable(dbId, tableId);
        Partition p = table.getPartition(partition);
        if (p == null) {
            throw new UserException(partition + " is not existed in table " + table.getName());
        }
        Pair<ExportJob, Boolean> runningExportJob = runningExportJobs.get(p.getId());
        if (runningExportJob != null) {
            return false;
        }
        return submitColddownPartition(p, false, false, true, false, properties);
    }

    /**
     * return false only if no need to colddown
     */
    public boolean submitColddownPartitionFromTtl(Partition partition) throws Exception {
        return submitColddownPartition(partition, true, false, false, true, properties);
    }

    /**
     * return false only if no need to colddown
     */
    private boolean submitColddownPartition(Partition partition, boolean triggeredByTtl, boolean checkStable,
                                            boolean ignoreFailureCount, boolean submitToIoThread,
                                            Map<String, String> properties)
            throws Exception {
        ColddownMgr colddownMgr = GlobalStateMgr.getCurrentState().getColddownMgr();
        Pair<ExportJob, Boolean> runningExportJob = runningExportJobs.get(partition.getId());
        if (runningExportJob != null) {
            return true;
        }
        if (partition.getVisibleVersion() == Partition.PARTITION_INIT_VERSION) {
            LOG.debug("partition[{}]'s is init version. ignore", partition.getName());
            return false;
        }
        // check if this partition has changed since last colddown
        List<PartitionColddownInfo> partitionColddownInfos =
                colddownMgr.getPartitionColddownInfos(partition.getId(), name);
        if (!partitionColddownInfos.isEmpty()) {
            long lastSuccessSync = 0;
            for (PartitionColddownInfo info : partitionColddownInfos) {
                if (info.isSuccess()) {
                    lastSuccessSync = Math.max(lastSuccessSync, info.getSyncedTimeMs());
                    // if it is triggered by ttl, new data changed after ttl triggered can be ignored,
                    // so do not submit.
                    if (info.isTriggeredByTtl()) {
                        return false;
                    }
                    // synced time is when the export job started, so this if means data is not changed after last job.
                    // ignoreLastSuccess is only for test purpose.
                    if (info.getSyncedTimeMs() > partition.getVisibleVersionTime() && !ignoreLastSuccess(properties)) {
                        return false;
                    }
                }
            }
            if (!ignoreFailureCount) {
                // need to submit new export job now, if too many failures, do not submit new export job
                // because something must be wrong, should fix it first and use manual colddown command
                int failCountInTwoDays = 0;
                long twoDays = 2 * 86400 * 1000L;
                for (PartitionColddownInfo info : partitionColddownInfos) {
                    if (!info.isSuccess()) {
                        // count failures after last success
                        if (info.getSyncedTimeMs() > lastSuccessSync &&
                                System.currentTimeMillis() - info.getSyncedTimeMs() <= twoDays) {
                            failCountInTwoDays++;
                        }
                    }
                }
                // 3 fails in 2 days,
                if (failCountInTwoDays >= 3) {
                    LOG.info("partition {} of colddown job {} reaches 3 failures in two days, ignore this" +
                                    " partition. Please use manual colddown command after fix the problem ",
                            partition.getName(), name);
                    return true;
                }
            }
        }

        long coldDownWaitMillis = getColddownWaitSeconds() * 1000;
        if (triggeredByTtl) {
            LOG.info("partition {} of colddown job {} triggers ttl, submit export job",
                    partition.getName(), name);
            submitExportJob(partition, true, properties, submitToIoThread);
        } else if (!checkStable) {
            submitExportJob(partition, false, properties, submitToIoThread);
        } else if (System.currentTimeMillis() - partition.getVisibleVersionTime() > coldDownWaitMillis) {
            LOG.info("partition {} of colddown job {} is stable for {} milliseconds, submit export job",
                    partition.getName(), name, coldDownWaitMillis);
            submitExportJob(partition, false, properties, submitToIoThread);
        }
        return true;
    }

    private boolean ignoreLastSuccess(Map<String, String> properties) {
        if (properties.containsKey("ignore_last_success")) {
            return Boolean.parseBoolean(properties.get("ignore_last_success"));
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return "ColddownJob [jobId=" + id
                + ", jobName=" + name
                + ", dbId=" + dbId
                + ", tableId=" + tableId
                + ", state=" + state
                + ", createTimeMs=" + TimeUtils.longToTimeString(createTimeMs)
                + ", FinishTimeMs=" + TimeUtils.longToTimeString(finishTimeMs)
                + ", where " + getWhereSql()
                + ", failMsg=" + failMsg
                + "]";
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // base infos
        out.writeLong(id);
        Text.writeString(out, name);
        out.writeLong(dbId);
        out.writeLong(tableId);
        Text.writeString(out, type);
        out.writeInt(columnNames == null ? 0 : columnNames.size());
        if (columnNames != null) {
            for (String columnName : columnNames) {
                Text.writeString(out, columnName);
            }
        }
        Text.writeString(out, getWhereSql());
        out.writeInt(properties.size());
        for (Map.Entry<String, String> property : properties.entrySet()) {
            Text.writeString(out, property.getKey());
            Text.writeString(out, property.getValue());
        }
        out.writeInt(targetProperties.size());
        for (Map.Entry<String, String> property : targetProperties.entrySet()) {
            Text.writeString(out, property.getKey());
            Text.writeString(out, property.getValue());
        }

        // task info
        Text.writeString(out, state.name());
        out.writeLong(createTimeMs);
        out.writeLong(finishTimeMs);
        failMsg.write(out);

        if (brokerDesc == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            brokerDesc.write(out);
        }

        tableName.write(out);
        // runningExportJobs are not persisted here,
        // because export job is automatically canceled after fe reboot or fe leader change
    }

    public void readFields(DataInput in) throws IOException {
        id = in.readLong();
        name = Text.readString(in);
        dbId = in.readLong();
        tableId = in.readLong();
        type = Text.readString(in);

        int columnCount = in.readInt();
        if (columnCount > 0) {
            columnNames = Lists.newArrayListWithExpectedSize(columnCount);
        }
        for (int i = 0; i < columnCount; i++) {
            this.columnNames.add(Text.readString(in));
        }

        String whereStr = Text.readString(in);
        if (whereStr.length() != 0) {
            whereExpr = ExportJob.parseWhereExpr(whereStr);
        }

        int count = in.readInt();
        for (int i = 0; i < count; i++) {
            String propertyKey = Text.readString(in);
            String propertyValue = Text.readString(in);
            this.properties.put(propertyKey, propertyValue);
        }

        int count2 = in.readInt();
        for (int i = 0; i < count2; i++) {
            String propertyKey = Text.readString(in);
            String propertyValue = Text.readString(in);
            this.targetProperties.put(propertyKey, propertyValue);
        }

        state = JobState.valueOf(Text.readString(in));
        createTimeMs = in.readLong();
        finishTimeMs = in.readLong();
        failMsg.readFields(in);

        if (in.readBoolean()) {
            brokerDesc = BrokerDesc.read(in);
        }

        tableName = new TableName();
        tableName.readFields(in);
        if (!isFinalState()) {
            this.runningExportJobs = Maps.newConcurrentMap();
            this.totalExportedRows = new AtomicLong();
            this.totalExportedBytes = new AtomicLong();
            this.totalSuccessExportedRows = new AtomicLong();
            this.totalSuccessExportedBytes = new AtomicLong();
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        if (!(obj instanceof ColddownJob)) {
            return false;
        }

        ColddownJob job = (ColddownJob) obj;

        return this.id == job.id;
    }

    public enum JobState {
        RUNNING,
        CANCELLED;
    }

    // for only persist op when switching job state.
    public static class StateTransfer implements Writable {
        long jobId;
        JobState state;

        public StateTransfer() {
            this.jobId = -1;
            this.state = JobState.CANCELLED;
        }

        public StateTransfer(long jobId, JobState state) {
            this.jobId = jobId;
            this.state = state;
        }

        public long getJobId() {
            return jobId;
        }

        public JobState getState() {
            return state;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeLong(jobId);
            Text.writeString(out, state.name());
        }

        public void readFields(DataInput in) throws IOException {
            jobId = in.readLong();
            state = JobState.valueOf(Text.readString(in));
        }
    }

    public static class AlterProperties implements Writable {
        long jobId;
        Map<String, String> properties;

        public AlterProperties() {
            this.jobId = -1;
            this.properties = new HashMap<>();
        }

        public AlterProperties(long jobId, Map<String, String> properties) {
            this.jobId = jobId;
            this.properties = properties;
        }

        public long getJobId() {
            return jobId;
        }

        public Map<String, String> getProperties() {
            return properties;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeLong(jobId);
            out.writeInt(properties.size());
            for (Map.Entry<String, String> property : properties.entrySet()) {
                Text.writeString(out, property.getKey());
                Text.writeString(out, property.getValue());
            }
        }

        public void readFields(DataInput in) throws IOException {
            jobId = in.readLong();
            int count = in.readInt();
            for (int i = 0; i < count; i++) {
                String propertyKey = Text.readString(in);
                String propertyValue = Text.readString(in);
                this.properties.put(propertyKey, propertyValue);
            }
        }
    }
}
