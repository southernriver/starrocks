// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/load/ColddownMgr.java

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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.CatalogRecycleBin;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeConstants;
import com.starrocks.common.FeMetaVersion;
import com.starrocks.common.Pair;
import com.starrocks.common.UserException;
import com.starrocks.common.util.ListComparator;
import com.starrocks.common.util.OrderByPair;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.fs.hdfs.HdfsFsManager;
import com.starrocks.load.export.ExportTargetType;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.mysql.privilege.Privilege;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AlterColddownStmt;
import com.starrocks.sql.ast.CancelColddownStmt;
import com.starrocks.sql.ast.CreateColddownStmt;
import com.starrocks.sql.ast.ManualColddownStmt;
import com.starrocks.sql.common.MetaUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class ColddownMgr {
    private static final Logger LOG = LogManager.getLogger(ColddownMgr.class);

    // lock for colddown job
    // lock is private and must use after db lock
    private ReentrantReadWriteLock lock;

    private Map<Long, ColddownJob> idToJob; // colddownJobId to colddownJob

    private Map<Long, List<PartitionColddownInfo>> idToUnPartitionedTableColddownInfo;
    private Map<Long, List<PartitionColddownInfo>> idToPartitionColddownInfo;

    public ColddownMgr() {
        idToJob = Maps.newHashMap();
        idToUnPartitionedTableColddownInfo = Maps.newConcurrentMap();
        idToPartitionColddownInfo = Maps.newConcurrentMap();
        lock = new ReentrantReadWriteLock(true);
    }

    public void readLock() {
        lock.readLock().lock();
    }

    public void readUnlock() {
        lock.readLock().unlock();
    }

    private void writeLock() {
        lock.writeLock().lock();
    }

    private void writeUnlock() {
        lock.writeLock().unlock();
    }

    public Map<Long, ColddownJob> getIdToJob() {
        return idToJob;
    }

    public void addColddownJob(CreateColddownStmt stmt) throws UserException {
        long jobId = GlobalStateMgr.getCurrentState().getNextId();
        ColddownJob job = createJob(jobId, stmt);
        writeLock();
        try {
            // check if job.name has been used
            for (ColddownJob j : idToJob.values()) {
                if (j.isFinalState()) {
                    continue;
                }
                if (j.getName().equals(job.getName())) {
                    throw new UserException("Name " + job.getName() + " is already used in dbId " + j.getDbId());
                }
                ExportTargetType exportTargetType = ExportTargetType.valueOf(j.getType());
                if (exportTargetType == ExportTargetType.EXTERNAL_TABLE &&
                        j.getTargetTable().equals(job.getTargetTable())) {
                    throw new UserException("Same external table " + j.getTargetTable() + " is already used in job "
                            + j.getName());
                }
            }
            unprotectAddJob(job);
            GlobalStateMgr.getCurrentState().getEditLog().logColddownCreate(job);
            if (job.isAddTargetTableAsColdTable()) {
                try {
                    addColdTableToOlapTable(job);
                } catch (Exception e) {
                    job.cancel(ExportFailMsg.CancelType.UNKNOWN, e.getMessage());
                    idToJob.remove(jobId);
                    throw e;
                }
            }
        } finally {
            writeUnlock();
        }
        LOG.info("add colddown job. {}", job);
    }

    private void unprotectAddJob(ColddownJob job) {
        idToJob.put(job.getId(), job);
    }

    private void addColdTableToOlapTable(ColddownJob job) throws UserException {
        ExportTargetType exportTargetType = ExportTargetType.valueOf(job.getType());
        if (exportTargetType != ExportTargetType.EXTERNAL_TABLE) {
            return;
        }
        if (job.getColumnNames() != null || !job.getWhereSql().isEmpty()) {
            return;
        }
        // ColddownJob itself will ensure table is OlapTable
        OlapTable table = (OlapTable) MetaUtils.getTable(job.getDbId(), job.getTableId());
        String coldTable = table.getTableProperty() == null ? null :
                table.getTableProperty().getProperties().get(PropertyAnalyzer.PROPERTIES_COLD_TABLE_INFO);
        if (coldTable != null && !coldTable.equals(job.getTargetTable())) {
            throw new UserException(
                    PropertyAnalyzer.PROPERTIES_COLD_TABLE_INFO + " has been set in table properties and it is " +
                            coldTable + " which is different to " + job.getTargetTable());
        }
        if (coldTable == null) {
            Database db = MetaUtils.getDatabase(job.getDbId());
            db.writeLock();
            try {
                Map<String, String> prop = new HashMap<>();
                prop.put(PropertyAnalyzer.PROPERTIES_COLD_TABLE_INFO, job.getTargetTable());
                GlobalStateMgr.getCurrentState().modifyColdTableInfoProperty(db, table, prop);
            } finally {
                db.writeUnlock();
            }
        }
    }

    public void addPartitionColddownInfo(PartitionColddownInfo partitionColddownInfo) {
        writeLock();
        try {
            if (partitionColddownInfo.getPartitionId() == -1) {
                idToUnPartitionedTableColddownInfo.computeIfAbsent(partitionColddownInfo.getTableId(),
                        i -> new ArrayList<>()).add(partitionColddownInfo);
            } else {
                idToPartitionColddownInfo.computeIfAbsent(partitionColddownInfo.getPartitionId(),
                                i -> new ArrayList<>())
                        .add(partitionColddownInfo);
            }
            GlobalStateMgr.getCurrentState().getEditLog().logColddownAddPartitionInfo(partitionColddownInfo);
        } finally {
            writeUnlock();
        }
    }

    public void unprotectAddPartitionColddownInfo(PartitionColddownInfo partitionColddownInfo) {
        if (partitionColddownInfo.getPartitionId() == -1) {
            idToUnPartitionedTableColddownInfo.computeIfAbsent(partitionColddownInfo.getTableId(),
                    i -> new ArrayList<>()).add(partitionColddownInfo);
        } else {
            idToPartitionColddownInfo.computeIfAbsent(partitionColddownInfo.getPartitionId(), i -> new ArrayList<>())
                    .add(partitionColddownInfo);
        }
    }

    private ColddownJob createJob(long jobId, CreateColddownStmt stmt) throws UserException {
        ColddownJob job = new ColddownJob(jobId, stmt.getJobName());
        job.setJob(stmt);
        return job;
    }

    public void cancelColddownJob(CancelColddownStmt stmt) throws UserException {
        ColddownJob matchedJob = findJob(stmt.getDbName(), stmt.getJobName());
        matchedJob.cancel(ExportFailMsg.CancelType.USER_CANCEL, "user cancel");
    }

    public void manualColddownPartition(ManualColddownStmt stmt) throws UserException {
        ColddownJob matchedJob = findJob(stmt.getDbName(), stmt.getJobName());

        try {
            Map<String, String> newProperties = matchedJob.getProperties();
            if (!stmt.getProperties().isEmpty()) {
                newProperties = new HashMap<>(matchedJob.getProperties());
                newProperties.putAll(stmt.getProperties());
            }
            if (!matchedJob.submitColddownPartition(stmt.getPartition(), newProperties)) {
                throw new UserException("this partition has no change or is exporting now or has no data, ignore");
            }
        } catch (UserException e) {
            throw e;
        } catch (Exception e) {
            throw new UserException(e);
        }
    }

    public void alterColddownJob(AlterColddownStmt stmt) throws UserException {
        ColddownJob matchedJob = findJob(stmt.getDbName(), stmt.getJobName());
        matchedJob.updateProperties(stmt.getProperties(), false);
    }

    private ColddownJob findJob(String dbName, String name) throws DdlException, AnalysisException {
        Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
        MetaUtils.checkDbNullAndReport(db, dbName);
        long dbId = db.getId();

        ColddownJob matchedJob = null;
        readLock();
        try {
            for (ColddownJob job : idToJob.values()) {
                String jobName = job.getName();
                if (job.getDbId() == dbId && (jobName != null && jobName.equals(name))) {
                    matchedJob = job;
                    if (!job.isFinalState()) {
                        break;
                    }
                }
            }
        } finally {
            readUnlock();
        }
        if (matchedJob == null) {
            throw new AnalysisException("Colddown job [" + name + "] is not found in db " + db.getOriginName());
        }
        if (matchedJob.isFinalState()) {
            throw new AnalysisException("Colddown job [" + name + "] is cancelled");
        }

        // check auth
        TableName tableName = matchedJob.getTableName();
        if (!GlobalStateMgr.getCurrentState().getAuth().checkTblPriv(ConnectContext.get(),
                tableName.getDb(), tableName.getTbl(),
                PrivPredicate.SELECT)) {
            ErrorReport.reportDdlException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, Privilege.SELECT_PRIV);
        }

        return matchedJob;
    }

    public List<ColddownJob> getColddownJobs(ColddownJob.JobState state) {
        List<ColddownJob> result = Lists.newArrayList();
        readLock();
        try {
            for (ColddownJob job : idToJob.values()) {
                if (job.getState() == state) {
                    result.add(job);
                }
            }
        } finally {
            readUnlock();
        }

        return result;
    }

    public List<ColddownJob> getColddownJobs(long tableId) {
        List<ColddownJob> result = Lists.newArrayList();
        readLock();
        try {
            for (ColddownJob job : idToJob.values()) {
                if (job.getTableId() == tableId) {
                    result.add(job);
                }
            }
        } finally {
            readUnlock();
        }

        return result;
    }

    // NOTE: jobid and states may both specified, or only one of them, or neither
    public List<List<String>> getColddownJobInfosByIdOrState(
            long dbId, long jobId, Set<ColddownJob.JobState> states, String jobName, String table,
            ArrayList<OrderByPair> orderByPairs, long limit) {

        long resultNum = limit == -1L ? Integer.MAX_VALUE : limit;
        LinkedList<List<Comparable>> colddownJobInfos = new LinkedList<List<Comparable>>();
        readLock();
        try {
            int counter = 0;
            for (ColddownJob job : idToJob.values()) {
                long id = job.getId();
                ColddownJob.JobState state = job.getState();

                if (job.getDbId() != dbId) {
                    continue;
                }

                // filter job
                if (jobId != 0) {
                    if (id != jobId) {
                        continue;
                    }
                }

                if (states != null) {
                    if (!states.contains(state)) {
                        continue;
                    }
                }

                String name = job.getName();
                if (jobName != null && (!name.equals(jobName))) {
                    continue;
                }

                TableName tableName = job.getTableName();
                if (table != null && (tableName != null && !tableName.toString().equals(table))) {
                    continue;
                }

                // check auth
                if (!GlobalStateMgr.getCurrentState().getAuth().checkTblPriv(ConnectContext.get(),
                        tableName.getDb(), tableName.getTbl(),
                        PrivPredicate.SHOW)) {
                    continue;
                }

                List<Comparable> jobInfo = new ArrayList<Comparable>();

                jobInfo.add(id);
                // job name
                jobInfo.add(name != null ? name : FeConstants.null_string);
                jobInfo.add(state.name());
                jobInfo.add(job.getTableName().toString());
                jobInfo.add(StringUtils.defaultString(
                        job.getBrokerDesc().getProperties().get(HdfsFsManager.USER_NAME_KEY)));

                // task infos
                Map<String, Object> infoMap = Maps.newHashMap();
                infoMap.putAll(job.getProperties());
                List<String> columns = job.getColumnNames() == null ? Lists.newArrayList("*") : job.getColumnNames();
                infoMap.put("columns", columns);
                infoMap.put("broker", job.getBrokerDesc().getName());
                infoMap.put("where", job.getWhereSql());
                jobInfo.add(new Gson().toJson(infoMap));
                jobInfo.add(job.getType());

                // target info
                jobInfo.add(new Gson().toJson(job.getTargetProperties()));

                jobInfo.add(TimeUtils.longToTimeString(job.getCreateTimeMs()));
                jobInfo.add(TimeUtils.longToTimeString(job.getFinishTimeMs()));
                jobInfo.add(job.getTimeoutSecond());
                Collection<Pair<ExportJob, Boolean>> exportJobs = job.getRunningExportJobs().values();
                if (!exportJobs.isEmpty()) {
                    jobInfo.add(new Gson().toJson(exportJobs.stream().map(pair -> {
                        ExportJob exportJob = pair.first;
                        Map<String, Object> result = new HashMap<>();
                        result.put("queryId", exportJob.getQueryId());
                        result.put("partitions", exportJob.getPartitions());
                        result.put("create time", TimeUtils.longToTimeString(exportJob.getCreateTimeMs()));
                        result.put("progress", exportJob.getProgress() + "%");
                        return result;
                    }).collect(Collectors.toList())));
                } else {
                    jobInfo.add("");
                }

                // error msg
                if (job.getState() == ColddownJob.JobState.CANCELLED) {
                    ExportFailMsg failMsg = job.getFailMsg();
                    jobInfo.add("type:" + failMsg.getCancelType() + "; msg:" + failMsg.getMsg());
                } else {
                    jobInfo.add(FeConstants.null_string);
                }

                colddownJobInfos.add(jobInfo);

                if (++counter >= resultNum) {
                    break;
                }
            }
        } finally {
            readUnlock();
        }

        // TODO: fix order by first, then limit
        // order by
        ListComparator<List<Comparable>> comparator = null;
        if (orderByPairs != null) {
            OrderByPair[] orderByPairArr = new OrderByPair[orderByPairs.size()];
            comparator = new ListComparator<List<Comparable>>(orderByPairs.toArray(orderByPairArr));
        } else {
            // sort by id asc
            comparator = new ListComparator<List<Comparable>>(0);
        }
        Collections.sort(colddownJobInfos, comparator);

        List<List<String>> results = Lists.newArrayList();
        for (List<Comparable> list : colddownJobInfos) {
            results.add(list.stream().map(e -> e.toString()).collect(Collectors.toList()));
        }

        return results;
    }

    private boolean isJobExpired(ColddownJob job, long currentTimeMs) {
        return (currentTimeMs - job.getCreateTimeMs()) / 1000 > Config.history_job_keep_max_second
                && job.getState() == ColddownJob.JobState.CANCELLED;
    }

    public List<PartitionColddownInfo> getPartitionColddownInfos(long partitionId) {
        readLock();
        try {
            List<PartitionColddownInfo> list = idToPartitionColddownInfo.get(partitionId);
            if (list == null) {
                return Collections.emptyList();
            }
            return list;
        } finally {
            readUnlock();
        }
    }

    public List<PartitionColddownInfo> getPartitionColddownInfos(long partitionId, String jobName) {
        readLock();
        try {
            List<PartitionColddownInfo> list = idToPartitionColddownInfo.get(partitionId);
            if (list == null) {
                return Collections.emptyList();
            }
            List<PartitionColddownInfo> result = Lists.newArrayList();
            for (PartitionColddownInfo info : list) {
                if (info.getJobName().equals(jobName)) {
                    result.add(info);
                }
            }

            return result;
        } finally {
            readUnlock();
        }
    }

    public void removeOldColddownJobs() {
        long currentTimeMs = System.currentTimeMillis();

        writeLock();
        try {
            CatalogRecycleBin recycleBin = GlobalStateMgr.getCurrentRecycleBin();
            Iterator<Map.Entry<Long, ColddownJob>> iter = idToJob.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry<Long, ColddownJob> entry = iter.next();
                ColddownJob job = entry.getValue();
                if (isJobExpired(job, currentTimeMs)) {
                    LOG.info("remove expired colddown job: {}", job);
                    iter.remove();
                } else if (isTableNotExist(job.getDbId(), job.getTableId(), recycleBin)) {
                    job.cancelInternal(ExportFailMsg.CancelType.UNKNOWN, "table not exist");
                    LOG.info("remove colddown job {} whose table is not existed any more", job);
                    iter.remove();
                }
            }
            // if partition is dropped, remove its partition colddown info
            Iterator<Map.Entry<Long, List<PartitionColddownInfo>>> iter2 =
                    idToPartitionColddownInfo.entrySet().iterator();
            while (iter2.hasNext()) {
                Map.Entry<Long, List<PartitionColddownInfo>> entry = iter2.next();
                List<PartitionColddownInfo> partitionColddownInfos = entry.getValue();
                if (partitionColddownInfos.isEmpty()) {
                    iter2.remove();
                    continue;
                }
                PartitionColddownInfo info = partitionColddownInfos.get(0);
                if (!isPartitionExist(info, recycleBin)) {
                    LOG.info("remove dropped partition colddown info: dbId: {}, tableId: {}, partitionId: {}",
                            info.getDbId(), info.getTableId(), info.getPartitionId());
                    iter2.remove();
                }
            }
            Iterator<Map.Entry<Long, List<PartitionColddownInfo>>> iter3 =
                    idToUnPartitionedTableColddownInfo.entrySet().iterator();
            while (iter3.hasNext()) {
                Map.Entry<Long, List<PartitionColddownInfo>> entry = iter3.next();
                List<PartitionColddownInfo> partitionColddownInfos = entry.getValue();
                if (partitionColddownInfos.isEmpty()) {
                    iter3.remove();
                    continue;
                }
                PartitionColddownInfo info = partitionColddownInfos.get(0);
                if (isTableNotExist(info.getDbId(), info.getTableId(), recycleBin)) {
                    LOG.info("remove unpartitioned table colddown info: dbId: {}, tableId: {}",
                            info.getDbId(), info.getTableId());
                    iter3.remove();
                }
            }
        } finally {
            writeUnlock();
        }
    }

    private boolean isTableNotExist(long dbId, long tableId, CatalogRecycleBin recycleBin) {
        try {
            MetaUtils.getTable(dbId, tableId);
            return false;
        } catch (SemanticException e) {
            if (recycleBin.getDatabase(dbId) == null) {
                return true;
            }
            return recycleBin.getTable(dbId, tableId) == null;
        }
    }

    private boolean isPartitionExist(PartitionColddownInfo info, CatalogRecycleBin recycleBin) {
        Table table;
        try {
            table = MetaUtils.getTable(info.getDbId(), info.getTableId());
        } catch (SemanticException e) {
            if (recycleBin.getDatabase(info.getDbId()) == null) {
                return false;
            }
            table = recycleBin.getTable(info.getDbId(), info.getTableId());
            if (table == null) {
                return false;
            }
        }
        if (table.getPartition(info.getPartitionId()) != null) {
            return true;
        }
        return recycleBin.getPartition(info.getPartitionId()) != null;
    }

    public void replayCreateColddownJob(ColddownJob job) {
        writeLock();
        try {
            unprotectAddJob(job);
        } finally {
            writeUnlock();
        }
    }

    public void replayCreatePartitionColddownInfo(PartitionColddownInfo info) {
        writeLock();
        try {
            List<PartitionColddownInfo> infos = idToPartitionColddownInfo.computeIfAbsent(info.getPartitionId(),
                    i -> new ArrayList<>());
            infos.add(info);
        } finally {
            writeUnlock();
        }
    }

    public void replayUpdateJobState(long jobId, ColddownJob.JobState newState) {
        writeLock();
        try {
            ColddownJob job = idToJob.get(jobId);
            if (job == null) {
                return;
            }
            job.updateState(newState, true);
            if (isJobExpired(job, System.currentTimeMillis())) {
                LOG.info("remove expired job: {}", job);
                idToJob.remove(jobId);
            }
        } finally {
            writeUnlock();
        }
    }

    public void replayAlterProperties(ColddownJob.AlterProperties alterProperties) {
        writeLock();
        try {
            ColddownJob job = idToJob.get(alterProperties.jobId);
            if (job == null) {
                return;
            }
            job.updateProperties(alterProperties.properties, true);
            if (isJobExpired(job, System.currentTimeMillis())) {
                LOG.info("remove expired job: {}", job);
                idToJob.remove(alterProperties.jobId);
            }
        } finally {
            writeUnlock();
        }
    }

    public long getJobNum(ColddownJob.JobState state, long dbId) {
        int size = 0;
        readLock();
        try {
            for (ColddownJob job : idToJob.values()) {
                if (job.getState() == state && job.getDbId() == dbId) {
                    ++size;
                }
            }
        } finally {
            readUnlock();
        }
        return size;
    }

    public long loadColddownJob(DataInputStream dis, long checksum) throws IOException, DdlException {
        long currentTimeMs = System.currentTimeMillis();
        long newChecksum = checksum;
        if (GlobalStateMgr.getCurrentStateJournalVersion() >= FeMetaVersion.VERSION_93) {
            int size = dis.readInt();
            newChecksum = checksum ^ size;
            for (int i = 0; i < size; ++i) {
                long jobId = dis.readLong();
                newChecksum ^= jobId;
                ColddownJob job = new ColddownJob();
                job.readFields(dis);
                // discard expired job right away
                if (isJobExpired(job, currentTimeMs)) {
                    LOG.info("discard expired job: {}", job);
                    continue;
                }
                unprotectAddJob(job);
            }
        }
        LOG.info("finished replay colddownJob from image");
        return newChecksum;
    }

    public long saveColddownJob(DataOutputStream dos, long checksum) throws IOException {
        Map<Long, ColddownJob> idToJob = getIdToJob();
        int size = idToJob.size();
        checksum ^= size;
        dos.writeInt(size);
        for (ColddownJob job : idToJob.values()) {
            long jobId = job.getId();
            checksum ^= jobId;
            dos.writeLong(jobId);
            job.write(dos);
        }

        return checksum;
    }

    public long loadPartitionColddownInfo(DataInputStream dis, long checksum) throws IOException {
        long newChecksum = checksum;
        if (GlobalStateMgr.getCurrentStateJournalVersion() >= FeMetaVersion.VERSION_93) {
            int size = dis.readInt();
            newChecksum = checksum ^ size;
            for (int i = 0; i < size; ++i) {
                int size2 = dis.readInt();
                newChecksum ^= size2;
                for (int j = 0; j < size2; j++) {
                    PartitionColddownInfo info = new PartitionColddownInfo();
                    info.readFields(dis);
                    newChecksum ^= info.getPartitionId();
                    unprotectAddPartitionColddownInfo(info);
                }
            }
        }
        LOG.info("finished replay partition colddown info from image");
        return newChecksum;
    }

    public long savePartitionColddownInfo(DataOutputStream dos, long checksum) throws IOException {
        int size = idToPartitionColddownInfo.size();
        checksum ^= size;
        dos.writeInt(size);
        for (List<PartitionColddownInfo> list : idToPartitionColddownInfo.values()) {
            int size2 = list.size();
            checksum ^= size2;
            dos.writeInt(size2);
            for (int i = 0; i < size2; i++) {
                PartitionColddownInfo info = list.get(i);
                info.write(dos);
                checksum ^= info.getPartitionId();
            }
        }

        return checksum;
    }
}
