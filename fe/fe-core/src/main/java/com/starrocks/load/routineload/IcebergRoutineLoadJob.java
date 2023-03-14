// Licensed to the Apache Software Foundation (ASF) under one

package com.starrocks.load.routineload;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.starrocks.analysis.BrokerDesc;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.RoutineLoadDataSourceProperties;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.Pair;
import com.starrocks.common.UserException;
import com.starrocks.common.io.Text;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.LogBuilder;
import com.starrocks.common.util.LogKey;
import com.starrocks.common.util.SmallFileMgr;
import com.starrocks.common.util.SmallFileMgr.SmallFile;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.iceberg.IcebergHiveCatalog;
import com.starrocks.connector.iceberg.IcebergUtil;
import com.starrocks.connector.iceberg.StarRocksIcebergException;
import com.starrocks.load.RoutineLoadDesc;
import com.starrocks.load.streamload.StreamLoadInfo;
import com.starrocks.planner.IcebergScanNode;
import com.starrocks.planner.IcebergStreamLoadPlanner;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.planner.ScanNode;
import com.starrocks.planner.StreamLoadPlanner;
import com.starrocks.planner.StreamLoadScanNode;
import com.starrocks.qe.OriginStatement;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreateRoutineLoadStmt;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TExecPlanFragmentParams;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TransactionStatus;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

/**
 * IcebergRoutineLoadJob is a kind of RoutineLoadJob which fetch data from iceberg.
 */
public class IcebergRoutineLoadJob extends RoutineLoadJob {
    private static final Logger LOG = LogManager.getLogger(IcebergRoutineLoadJob.class);

    public static final String ICEBERG_FILE_CATALOG = "iceberg";
    private static final long DEFAULT_SPLIT_SIZE = 2L * 1024 * 1024 * 1024; // 2 GB

    private String icebergCatalogType;
    private String icebergCatalogHiveMetastoreUris;
    private String icebergResourceName;
    private String icebergCatalogName;
    private String icebergDatabase;
    private String icebergTable = null;
    private String icebergConsumePosition = null;
    private BrokerDesc brokerDesc;
    // iceberg properties, property prefix will be mapped to iceberg custom parameters, which can be extended in the future
    private Map<String, String> customProperties = Maps.newHashMap();
    private Map<String, String> convertedCustomProperties = Maps.newHashMap();
    private org.apache.iceberg.Table iceTbl; // actual iceberg table

    private final Queue<Pair<IcebergSplitMeta, CombinedScanTask>> splitsQueue = new LinkedBlockingQueue<>();
    private IcebergSplitDiscover discover;

    public IcebergRoutineLoadJob() {
        // for serialization, id is dummy
        super(-1, LoadDataSourceType.ICEBERG);
    }

    public IcebergRoutineLoadJob(Long id, String name, long dbId, long tableId,
                                 IcebergCreateRoutineLoadStmtConfig config, BrokerDesc brokerDesc) {
        super(id, name, dbId, tableId, LoadDataSourceType.ICEBERG);
        this.icebergCatalogType = config.getIcebergCatalogType();
        this.icebergCatalogHiveMetastoreUris = config.getIcebergCatalogHiveMetastoreUris();
        this.icebergResourceName = config.getIcebergResourceName();
        this.icebergCatalogName = config.getIcebergCatalogName();
        this.icebergDatabase = config.getIcebergDatabase();
        this.icebergTable = config.getIcebergTable();
        this.icebergConsumePosition = config.getIcebergConsumePosition();
        this.brokerDesc = brokerDesc;
        this.progress = new IcebergProgress();
    }

    @Override
    public void prepare() throws UserException {
        super.prepare();
        // should reset converted properties each time the job being prepared.
        // because the file info can be changed anytime.
        convertCustomProperties(true);
    }

    public synchronized void convertCustomProperties(boolean rebuild) throws DdlException {
        if (customProperties.isEmpty()) {
            return;
        }

        if (!rebuild && !convertedCustomProperties.isEmpty()) {
            return;
        }

        if (rebuild) {
            convertedCustomProperties.clear();
        }

        SmallFileMgr smallFileMgr = GlobalStateMgr.getCurrentState().getSmallFileMgr();
        for (Map.Entry<String, String> entry : customProperties.entrySet()) {
            if (entry.getValue().startsWith("FILE:")) {
                // convert FILE:file_name -> FILE:file_id:md5
                String file = entry.getValue().substring(entry.getValue().indexOf(":") + 1);
                SmallFile smallFile = smallFileMgr.getSmallFile(dbId, ICEBERG_FILE_CATALOG, file, true);
                convertedCustomProperties.put(entry.getKey(), "FILE:" + smallFile.id + ":" + smallFile.md5);
            } else {
                convertedCustomProperties.put(entry.getKey(), entry.getValue());
            }
        }
    }

    private void getIceTbl() throws UserException {
        if (iceTbl != null) {
            return;
        }
        try {
            if (IcebergCreateRoutineLoadStmtConfig.isHiveCatalogType(icebergCatalogType)) {
                iceTbl = getIcbTblFromHiveMetastore();
            } else if (IcebergCreateRoutineLoadStmtConfig.isResourceCatalogType(icebergCatalogType)) {
                iceTbl = IcebergUtil.getTableFromResource(icebergResourceName, icebergDatabase, icebergTable);
            } else if (IcebergCreateRoutineLoadStmtConfig.isExternalCatalogType(icebergCatalogType)) {
                iceTbl = IcebergUtil.getTableFromCatalog(icebergCatalogName, icebergDatabase, icebergTable);
            }
        } catch (StarRocksIcebergException | AnalysisException e) {
            throw new UserException(e);
        }
    }

    private org.apache.iceberg.Table getIcbTblFromHiveMetastore() throws StarRocksIcebergException {
        IcebergHiveCatalog catalog =
                (IcebergHiveCatalog) IcebergUtil.getIcebergHiveCatalog(icebergCatalogHiveMetastoreUris, new HashMap<>(),
                        new HdfsEnvironment());
        TableIdentifier tableIdentifier = IcebergUtil.getIcebergTableIdentifier(icebergDatabase, icebergTable);
        return catalog.loadTable(tableIdentifier);
    }

    private Expression getIcebergPredicates(org.apache.iceberg.Table iceTbl) throws UserException {
        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        if (db == null) {
            throw new MetaNotFoundException("db " + dbId + " does not exist");
        }
        UUID uuid = UUID.randomUUID();
        TUniqueId loadId = new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
        db.readLock();
        try {
            Table table = db.getTable(this.tableId);
            if (table == null) {
                throw new MetaNotFoundException("table " + this.tableId + " does not exist");
            }
            final AtomicReference<List<Expr>> conjuncts = new AtomicReference<>();
            StreamLoadPlanner planner =
                    new StreamLoadPlanner(db, (OlapTable) table, StreamLoadInfo.fromRoutineLoadJob(this)) {
                        @Override
                        protected ScanNode createScanNode(TUniqueId loadId, TupleDescriptor tupleDesc)
                                throws UserException {
                            StreamLoadScanNode scanNode =
                                    new StreamLoadScanNode(loadId, new PlanNodeId(0), tupleDesc, destTable,
                                            streamLoadInfo) {
                                        @Override
                                        protected Expr analyzeAndCastFold(Expr whereExpr) {
                                            return whereExpr;
                                        }
                                    };
                            scanNode.setUseVectorizedLoad(true);
                            scanNode.init(analyzer);
                            scanNode.finalizeStats(analyzer);
                            conjuncts.set(scanNode.getConjuncts());
                            return scanNode;
                        }
                    };
            planner.plan(loadId);
            List<Expression> icebergPredicates = IcebergScanNode.preProcessConjuncts(iceTbl, conjuncts.get());
            Expression icebergWhereExpr = Expressions.alwaysTrue();
            if (!icebergPredicates.isEmpty()) {
                icebergWhereExpr = icebergPredicates.stream().reduce(Expressions.alwaysTrue(), Expressions::and);
            }
            return icebergWhereExpr;
        } finally {
            db.readUnlock();
        }
    }

    @Override
    public void divideRoutineLoadJob(int currentConcurrentTaskNum) throws UserException {
        getIceTbl();
        IcebergProgress icebergProgress = (IcebergProgress) progress;
        Long readIcebergSnapshotsAfterTimestamp =
                IcebergCreateRoutineLoadStmtConfig.getReadIcebergSnapshotsAfterTimestamp(customProperties);
        Expression icebergWhereExpr = null;
        if (whereExpr != null) {
            icebergWhereExpr = getIcebergPredicates(iceTbl);
        }
        if (discover == null) {
            discover = new IcebergSplitDiscover(name, iceTbl, icebergProgress, icebergConsumePosition,
                    readIcebergSnapshotsAfterTimestamp, splitsQueue);
        }
        Long icebergPlanSplitSize =
                IcebergCreateRoutineLoadStmtConfig.getIcebergPlanSplitSize(customProperties);
        discover.setWhereExpr(icebergWhereExpr);
        discover.setCurrentConcurrentTaskNum(currentConcurrentTaskNum);
        discover.setSplitSize(icebergPlanSplitSize != null ? icebergPlanSplitSize : DEFAULT_SPLIT_SIZE);
        List<RoutineLoadTaskInfo> result = new ArrayList<>();
        writeLock();
        try {
            if (state == JobState.NEED_SCHEDULE) {
                discover.start();
                // divide splits into tasks
                for (int i = 0; i < currentConcurrentTaskNum; i++) {
                    long timeToExecuteMs = System.currentTimeMillis() + taskSchedIntervalS * 1000;
                    IcebergTaskInfo icebergTaskInfo = new IcebergTaskInfo(UUID.randomUUID(), id,
                            taskSchedIntervalS * 1000, timeToExecuteMs, icebergConsumePosition, splitsQueue,
                            icebergProgress);
                    LOG.debug("iceberg routine load task created: " + icebergTaskInfo);
                    routineLoadTaskInfoList.add(icebergTaskInfo);
                    result.add(icebergTaskInfo);
                }
                // change job state to running
                if (result.size() != 0) {
                    unprotectUpdateState(JobState.RUNNING, null, false);
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
    public int calculateCurrentConcurrentTaskNum() {
        SystemInfoService systemInfoService = GlobalStateMgr.getCurrentSystemInfo();
        int aliveBeNum = systemInfoService.getAliveBackendNumber();
        if (desireTaskConcurrentNum == 0) {
            desireTaskConcurrentNum = Config.max_routine_load_task_concurrent_num;
        }

        LOG.debug("current concurrent task number is min"
                        + "(desire task concurrent num: {}, alive be num * per job per be: {}, config: {})",
                desireTaskConcurrentNum, aliveBeNum * Config.max_iceberg_routine_load_task_num_per_be_per_job,
                Config.max_iceberg_routine_load_task_concurrent_num);
        currentTaskConcurrentNum = Math.min(
                Math.min(desireTaskConcurrentNum, aliveBeNum * Config.max_iceberg_routine_load_task_num_per_be_per_job),
                Config.max_iceberg_routine_load_task_concurrent_num);
        return currentTaskConcurrentNum;
    }

    // Through the transaction status and attachment information, to determine whether the progress needs to be updated.
    @Override
    protected boolean checkCommitInfo(RLTaskTxnCommitAttachment rlTaskTxnCommitAttachment,
                                      TransactionState txnState,
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
        LOG.debug("no need to update the progress of iceberg routine load. txn status: {}, " +
                        "txnStatusChangeReason: {}, task: {}, job: {}",
                txnState.getTransactionStatus(), txnStatusChangeReason,
                DebugUtil.printId(rlTaskTxnCommitAttachment.getTaskId()), id);
        return false;
    }

    @Override
    protected void updateProgress(RLTaskTxnCommitAttachment attachment) throws UserException {
        super.updateProgress(attachment);
        this.progress.update(attachment);
    }

    @Override
    protected void clearTasks() {
        super.clearTasks();
        splitsQueue.clear();
        if (discover != null) {
            discover.stop();
        }
        if (state.isFinalState()) {
            discover = null;
            ((IcebergProgress) progress).clear();
        }
    }

    public int pendingAndRunningTasks() {
        int tasks = splitsQueue.size();
        for (RoutineLoadTaskInfo taskInfo : routineLoadTaskInfoList) {
            IcebergTaskInfo task = ((IcebergTaskInfo) taskInfo);
            if (task.hasSplits()) {
                tasks++;
            }
        }
        return tasks;
    }

    @Override
    protected void replayUpdateProgress(RLTaskTxnCommitAttachment attachment) {
        super.replayUpdateProgress(attachment);
        this.progress.update(attachment);
    }

    @Override
    protected RoutineLoadTaskInfo unprotectRenewTask(long timeToExecuteMs, RoutineLoadTaskInfo routineLoadTaskInfo) {
        IcebergTaskInfo oldIcebergTaskInfo = (IcebergTaskInfo) routineLoadTaskInfo;
        // add new task
        IcebergTaskInfo icebergTaskInfo = new IcebergTaskInfo(timeToExecuteMs, oldIcebergTaskInfo);
        // remove old task
        routineLoadTaskInfoList.remove(routineLoadTaskInfo);
        // add new task
        routineLoadTaskInfoList.add(icebergTaskInfo);
        LOG.debug("iceberg routine load task renewed: " + icebergTaskInfo);
        return icebergTaskInfo;
    }

    @Override
    protected boolean unprotectNeedReschedule() throws UserException {
        if (this.state == JobState.PAUSED) {
            boolean autoSchedule = ScheduleRule.isNeedAutoSchedule(this);
            if (autoSchedule) {
                LOG.info(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, name)
                        .add("current_state", this.state)
                        .add("msg", "should be rescheduled")
                        .build());
            }
            return autoSchedule;
        } else {
            return false;
        }
    }

    public TExecPlanFragmentParams plan(TUniqueId loadId, long txnId, long beId, int timeout,
                                        List<IcebergSplit> splits) throws UserException {
        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        if (db == null) {
            throw new MetaNotFoundException("db " + dbId + " does not exist");
        }
        db.readLock();
        try {
            Table table = db.getTable(this.tableId);
            if (table == null) {
                throw new MetaNotFoundException("table " + this.tableId + " does not exist");
            }
            StreamLoadInfo streamLoadInfo = StreamLoadInfo.fromRoutineLoadJob(this);
            streamLoadInfo.setTimeout(timeout);
            StreamLoadPlanner planner =
                    new IcebergStreamLoadPlanner(db, (OlapTable) table, streamLoadInfo, brokerDesc, beId, splits);
            TExecPlanFragmentParams planParams = planner.plan(loadId);
            // add table indexes to transaction state
            TransactionState txnState =
                    GlobalStateMgr.getCurrentGlobalTransactionMgr().getTransactionState(db.getId(), txnId);
            if (txnState == null) {
                throw new MetaNotFoundException("txn does not exist: " + txnId);
            }
            txnState.addTableIndexes(planner.getDestTable());

            return planParams;
        } finally {
            db.readUnlock();
        }
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
        summary.put("pendingAndRunningTasks", pendingAndRunningTasks());
        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        return gson.toJson(summary);
    }

    public static IcebergRoutineLoadJob fromCreateStmt(CreateRoutineLoadStmt stmt) throws UserException {
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

        // init iceberg routine load job
        long id = GlobalStateMgr.getCurrentState().getNextId();
        IcebergCreateRoutineLoadStmtConfig config = stmt.getCreateIcebergRoutineLoadStmtConfig();
        IcebergRoutineLoadJob icebergRoutineLoadJob = new IcebergRoutineLoadJob(id, stmt.getName(),
                db.getId(), tableId, config, config.getBrokerDesc());
        icebergRoutineLoadJob.setOptional(stmt);
        icebergRoutineLoadJob.checkCustomProperties();

        return icebergRoutineLoadJob;
    }

    private void checkCustomProperties() throws DdlException {
        SmallFileMgr smallFileMgr = GlobalStateMgr.getCurrentState().getSmallFileMgr();
        for (Map.Entry<String, String> entry : customProperties.entrySet()) {
            if (entry.getValue().startsWith("FILE:")) {
                String file = entry.getValue().substring(entry.getValue().indexOf(":") + 1);
                // check file
                if (!smallFileMgr.containsFile(dbId, ICEBERG_FILE_CATALOG, file)) {
                    throw new DdlException("File " + file + " does not exist in db "
                            + dbId + " with globalStateMgr: " + ICEBERG_FILE_CATALOG);
                }
            }
        }
    }

    @Override
    protected void setOptional(CreateRoutineLoadStmt stmt) throws UserException {
        super.setOptional(stmt);

        if (!stmt.getCreateIcebergRoutineLoadStmtConfig().getCustomIcebergProperties().isEmpty()) {
            setCustomIcebergProperties(stmt.getCreateIcebergRoutineLoadStmtConfig().getCustomIcebergProperties());
        }
    }

    private void setCustomIcebergProperties(Map<String, String> icebergProperties) {
        this.customProperties = icebergProperties;
    }

    @Override
    protected String dataSourcePropertiesJsonToString() {
        Map<String, String> dataSourceProperties = Maps.newHashMap();
        dataSourceProperties.put("icebergCatalogType", icebergCatalogType);
        dataSourceProperties.put("icebergCatalogHiveMetastoreUris", icebergCatalogHiveMetastoreUris);
        dataSourceProperties.put("icebergResourceName", icebergResourceName);
        dataSourceProperties.put("icebergCatalogName", icebergCatalogName);
        dataSourceProperties.put("icebergDatabase", icebergDatabase);
        dataSourceProperties.put("icebergTable", icebergTable);
        dataSourceProperties.put("icebergConsumePosition", icebergConsumePosition);
        if (brokerDesc != null) {
            dataSourceProperties.put("brokerDesc", brokerDesc.toString());
        }
        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        return gson.toJson(dataSourceProperties);
    }

    @Override
    protected String customPropertiesJsonToString() {
        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        return gson.toJson(customProperties);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        Text.writeString(out, icebergCatalogType);
        if (IcebergCreateRoutineLoadStmtConfig.isHiveCatalogType(icebergCatalogType)) {
            Text.writeString(out, icebergCatalogHiveMetastoreUris);
        } else if (IcebergCreateRoutineLoadStmtConfig.isResourceCatalogType(icebergCatalogType)) {
            Text.writeString(out, icebergResourceName);
        } else if (IcebergCreateRoutineLoadStmtConfig.isExternalCatalogType(icebergCatalogType)) {
            Text.writeString(out, icebergCatalogName);
        }
        Text.writeString(out, icebergDatabase);
        Text.writeString(out, icebergTable);
        Text.writeString(out, icebergConsumePosition);
        out.writeBoolean(brokerDesc != null);
        if (brokerDesc != null) {
            brokerDesc.write(out);
        }

        out.writeInt(customProperties.size());
        for (Map.Entry<String, String> property : customProperties.entrySet()) {
            Text.writeString(out, "property." + property.getKey());
            Text.writeString(out, property.getValue());
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        icebergCatalogType = Text.readString(in);
        if (IcebergCreateRoutineLoadStmtConfig.isHiveCatalogType(icebergCatalogType)) {
            icebergCatalogHiveMetastoreUris = Text.readString(in);
        } else if (IcebergCreateRoutineLoadStmtConfig.isResourceCatalogType(icebergCatalogType)) {
            icebergResourceName = Text.readString(in);
        } else if (IcebergCreateRoutineLoadStmtConfig.isExternalCatalogType(icebergCatalogType)) {
            icebergCatalogName = Text.readString(in);
        }
        icebergDatabase = Text.readString(in);
        icebergTable = Text.readString(in);
        icebergConsumePosition = Text.readString(in);
        if (in.readBoolean()) {
            brokerDesc = BrokerDesc.read(in);
        }

        int count = in.readInt();
        for (int i = 0; i < count; i++) {
            String propertyKey = Text.readString(in);
            String propertyValue = Text.readString(in);
            if (propertyKey.startsWith("property.")) {
                this.customProperties.put(propertyKey.substring(propertyKey.indexOf(".") + 1), propertyValue);
            }
        }
    }

    @Override
    public void modifyJob(RoutineLoadDesc routineLoadDesc, Map<String, String> jobProperties,
                          RoutineLoadDataSourceProperties dataSourceProperties, OriginStatement originStatement,
                          boolean isReplay) throws DdlException {
        if (!isReplay) {
            // modification to whereExpr is only allowed when all current splits are finished.
            // otherwise the discover.planTasks() return different result to that before pause,
            // which may cause incorrect progress when resume from recovery if any iceberg routine load job's task
            // is still running before resume, which cause duplicate data consumed
            boolean shouldCheckModify = routineLoadDesc != null && routineLoadDesc.getWherePredicate() != null;
            if (shouldCheckModify && !((IcebergProgress) progress).allDone()) {
                throw new DdlException(
                        "Modification to whereExpr when current tasks are unfinished" +
                                " may cause duplicate data consumed, please resume and wait all tasks finished," +
                                " then pause and do alter again." +
                                " Execute 'SHOW ROUTINE LOAD FOR " + name + ";' before pause" +
                                " and check pendingAndRunningTasks to see whether all tasks are finished.");
            }
        }
        super.modifyJob(routineLoadDesc, jobProperties, dataSourceProperties, originStatement, isReplay);
    }

    @Override
    public void modifyDataSourceProperties(RoutineLoadDataSourceProperties dataSourceProperties) throws DdlException {
        this.customProperties.putAll(dataSourceProperties.getCustomIcebergProperties());
        LOG.info("modify the data source properties of iceberg routine load job: {}, datasource properties: {}",
                this.id, dataSourceProperties);
    }
}
