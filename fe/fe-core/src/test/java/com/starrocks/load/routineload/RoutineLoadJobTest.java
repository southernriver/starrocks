// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/load/routineload/RoutineLoadJobTest.java

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
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.InternalErrorCode;
import com.starrocks.common.UserException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.util.KafkaUtil;
import com.starrocks.load.RoutineLoadDesc;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.RoutineLoadOperation;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.OriginStatement;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AlterRoutineLoadStmt;
import com.starrocks.sql.ast.CreateRoutineLoadStmt;
import com.starrocks.thrift.TKafkaRLTaskProgress;
import com.starrocks.transaction.TransactionState;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.UUID;

public class RoutineLoadJobTest {
    @Test
    public void testAfterAbortedReasonOffsetOutOfRange(@Mocked GlobalStateMgr globalStateMgr,
                                                       @Injectable TransactionState transactionState,
                                                       @Injectable RoutineLoadTaskInfo routineLoadTaskInfo)
            throws UserException {

        List<RoutineLoadTaskInfo> routineLoadTaskInfoList = Lists.newArrayList();
        routineLoadTaskInfoList.add(routineLoadTaskInfo);
        long txnId = 1L;

        new Expectations() {
            {
                transactionState.getTransactionId();
                minTimes = 0;
                result = txnId;
                routineLoadTaskInfo.getTxnId();
                minTimes = 0;
                result = txnId;
            }
        };

        new MockUp<RoutineLoadJob>() {
            @Mock
            void writeUnlock() {
            }
        };

        String txnStatusChangeReasonString = TransactionState.TxnStatusChangeReason.OFFSET_OUT_OF_RANGE.toString();
        RoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob();
        Deencapsulation.setField(routineLoadJob, "routineLoadTaskInfoList", routineLoadTaskInfoList);
        routineLoadJob.afterAborted(transactionState, true, txnStatusChangeReasonString);

        Assert.assertEquals(RoutineLoadJob.JobState.PAUSED, routineLoadJob.getState());
    }

    @Test
    public void testAfterAborted(@Injectable TransactionState transactionState,
                                 @Injectable KafkaTaskInfo routineLoadTaskInfo) throws UserException {
        List<RoutineLoadTaskInfo> routineLoadTaskInfoList = Lists.newArrayList();
        routineLoadTaskInfoList.add(routineLoadTaskInfo);
        long txnId = 1L;

        RLTaskTxnCommitAttachment attachment = new RLTaskTxnCommitAttachment();
        TKafkaRLTaskProgress tKafkaRLTaskProgress = new TKafkaRLTaskProgress();
        tKafkaRLTaskProgress.partitionCmtOffset = Maps.newHashMap();
        KafkaProgress kafkaProgress = new KafkaProgress(tKafkaRLTaskProgress);
        Deencapsulation.setField(attachment, "progress", kafkaProgress);

        KafkaProgress currentProgress = new KafkaProgress(tKafkaRLTaskProgress);

        new Expectations() {
            {
                transactionState.getTransactionId();
                minTimes = 0;
                result = txnId;
                routineLoadTaskInfo.getTxnId();
                minTimes = 0;
                result = txnId;
                transactionState.getTxnCommitAttachment();
                minTimes = 0;
                result = attachment;
                routineLoadTaskInfo.getPartitions();
                minTimes = 0;
                result = Lists.newArrayList();
                routineLoadTaskInfo.getId();
                minTimes = 0;
                result = UUID.randomUUID();
            }
        };

        new MockUp<RoutineLoadJob>() {
            @Mock
            void writeUnlock() {
            }
        };

        String txnStatusChangeReasonString = "no data";
        RoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob();
        Deencapsulation.setField(routineLoadJob, "state", RoutineLoadJob.JobState.RUNNING);
        Deencapsulation.setField(routineLoadJob, "routineLoadTaskInfoList", routineLoadTaskInfoList);
        Deencapsulation.setField(routineLoadJob, "progress", currentProgress);
        routineLoadJob.afterAborted(transactionState, true, txnStatusChangeReasonString);

        Assert.assertEquals(RoutineLoadJob.JobState.RUNNING, routineLoadJob.getState());
        Assert.assertEquals(new Long(1), Deencapsulation.getField(routineLoadJob, "abortedTaskNum"));
    }

    @Test
    public void testGetShowInfo(@Mocked KafkaProgress kafkaProgress) {
        RoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob();
        Deencapsulation.setField(routineLoadJob, "state", RoutineLoadJob.JobState.PAUSED);
        ErrorReason errorReason = new ErrorReason(InternalErrorCode.INTERNAL_ERR,
                TransactionState.TxnStatusChangeReason.OFFSET_OUT_OF_RANGE.toString());
        Deencapsulation.setField(routineLoadJob, "pauseReason", errorReason);
        Deencapsulation.setField(routineLoadJob, "progress", kafkaProgress);

        List<String> showInfo = routineLoadJob.getShowInfo();
        Assert.assertEquals(true, showInfo.stream().filter(entity -> !Strings.isNullOrEmpty(entity))
                .anyMatch(entity -> entity.equals(errorReason.toString())));
    }

    @Test
    public void testUpdateWhileDbDeleted(@Mocked GlobalStateMgr globalStateMgr) throws UserException {
        new Expectations() {
            {
                globalStateMgr.getDb(anyLong);
                minTimes = 0;
                result = null;
            }
        };

        RoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob();
        routineLoadJob.update();

        Assert.assertEquals(RoutineLoadJob.JobState.CANCELLED, routineLoadJob.getState());
    }

    @Test
    public void testUpdateWhileTableDeleted(@Mocked GlobalStateMgr globalStateMgr,
                                            @Injectable Database database) throws UserException {
        new Expectations() {
            {
                globalStateMgr.getDb(anyLong);
                minTimes = 0;
                result = database;
                database.getTable(anyLong);
                minTimes = 0;
                result = null;
            }
        };
        RoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob();
        routineLoadJob.update();

        Assert.assertEquals(RoutineLoadJob.JobState.CANCELLED, routineLoadJob.getState());
    }

    @Test
    public void testUpdateWhilePartitionChanged(@Mocked GlobalStateMgr globalStateMgr,
                                                @Injectable Database database,
                                                @Injectable Table table,
                                                @Injectable KafkaProgress kafkaProgress) throws UserException {

        new Expectations() {
            {
                globalStateMgr.getDb(anyLong);
                minTimes = 0;
                result = database;
                database.getTable(anyLong);
                minTimes = 0;
                result = table;
            }
        };

        new MockUp<KafkaUtil>() {
            @Mock
            public List<Integer> getAllKafkaPartitions(String brokerList, String topic,
                                                       ImmutableMap<String, String> properties) throws UserException {
                return Lists.newArrayList(1, 2, 3);
            }
        };

        new MockUp<EditLog>() {
            @Mock
            public void logOpRoutineLoadJob(RoutineLoadOperation routineLoadOperation) {

            }
        };

        RoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob();
        Deencapsulation.setField(routineLoadJob, "state", RoutineLoadJob.JobState.RUNNING);
        Deencapsulation.setField(routineLoadJob, "progress", kafkaProgress);
        routineLoadJob.update();

        Assert.assertEquals(RoutineLoadJob.JobState.NEED_SCHEDULE, routineLoadJob.getState());
    }

    @Test
    public void testUpdateNumOfDataErrorRowMoreThanMax(@Mocked GlobalStateMgr globalStateMgr) {
        RoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob();
        Deencapsulation.setField(routineLoadJob, "maxErrorNum", 0);
        Deencapsulation.setField(routineLoadJob, "maxBatchRows", 0);
        Deencapsulation.invoke(routineLoadJob, "updateNumOfData", 1L, 1L, 0L, 1L, 1L, false);

        Assert.assertEquals(RoutineLoadJob.JobState.PAUSED, Deencapsulation.getField(routineLoadJob, "state"));

    }

    @Test
    public void testUpdateTotalMoreThanBatch() {
        RoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob();
        Deencapsulation.setField(routineLoadJob, "state", RoutineLoadJob.JobState.RUNNING);
        Deencapsulation.setField(routineLoadJob, "maxErrorNum", 10);
        Deencapsulation.setField(routineLoadJob, "maxBatchRows", 10);
        Deencapsulation.setField(routineLoadJob, "currentErrorRows", 1);
        Deencapsulation.setField(routineLoadJob, "currentTotalRows", 99);
        Deencapsulation.invoke(routineLoadJob, "updateNumOfData", 2L, 0L, 0L, 1L, 1L, false);

        Assert.assertEquals(RoutineLoadJob.JobState.RUNNING, Deencapsulation.getField(routineLoadJob, "state"));
        Assert.assertEquals(new Long(0), Deencapsulation.getField(routineLoadJob, "currentErrorRows"));
        Assert.assertEquals(new Long(0), Deencapsulation.getField(routineLoadJob, "currentTotalRows"));

    }

    @Test
    public void testModifyJobProperties() throws Exception {
        RoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob();
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();
        // alter job properties
        String desiredConcurrentNumber = "3";
        String maxBatchInterval = "60";
        String maxErrorNumber = "10000";
        String maxBatchRows = "200000";
        String strictMode = "true";
        String timeZone = "UTC";
        String jsonPaths = "[\\\"$.category\\\",\\\"$.author\\\",\\\"$.price\\\",\\\"$.timestamp\\\"]";
        String stripOuterArray = "true";
        String jsonRoot = "$.RECORDS";
        String originStmt = "alter routine load for db.job1 " +
                "properties (" +
                "   \"desired_concurrent_number\" = \"" + desiredConcurrentNumber + "\"," +
                "   \"max_batch_interval\" = \"" + maxBatchInterval + "\"," +
                "   \"max_error_number\" = \"" + maxErrorNumber + "\"," +
                "   \"max_batch_rows\" = \"" + maxBatchRows + "\"," +
                "   \"strict_mode\" = \"" + strictMode + "\"," +
                "   \"timezone\" = \"" + timeZone + "\"," +
                "   \"jsonpaths\" = \"" + jsonPaths + "\"," +
                "   \"strip_outer_array\" = \"" + stripOuterArray + "\"," +
                "   \"json_root\" = \"" + jsonRoot + "\"" +
                ")";
        AlterRoutineLoadStmt stmt =
                (AlterRoutineLoadStmt) UtFrameUtils.parseStmtWithNewParser(originStmt, connectContext);
        routineLoadJob.modifyJob(stmt.getRoutineLoadDesc(), stmt.getAnalyzedJobProperties(),
                stmt.getDataSourceProperties(), new OriginStatement(originStmt, 0), true);
        Assert.assertEquals(Integer.parseInt(desiredConcurrentNumber),
                (int) Deencapsulation.getField(routineLoadJob, "desireTaskConcurrentNum"));
        Assert.assertEquals(Long.parseLong(maxBatchInterval),
                (long) Deencapsulation.getField(routineLoadJob, "taskSchedIntervalS"));
        Assert.assertEquals(Long.parseLong(maxErrorNumber),
                (long) Deencapsulation.getField(routineLoadJob, "maxErrorNum"));
        Assert.assertEquals(Long.parseLong(maxBatchRows),
                (long) Deencapsulation.getField(routineLoadJob, "maxBatchRows"));
        Assert.assertEquals(Boolean.parseBoolean(strictMode), routineLoadJob.isStrictMode());
        Assert.assertEquals(timeZone, routineLoadJob.getTimezone());
        Assert.assertEquals(jsonPaths.replace("\\", ""), routineLoadJob.getJsonPaths());
        Assert.assertEquals(Boolean.parseBoolean(stripOuterArray), routineLoadJob.isStripOuterArray());
        Assert.assertEquals(jsonRoot, routineLoadJob.getJsonRoot());
    }

    @Test
    public void testModifyDataSourceProperties() throws Exception {
        KafkaRoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob();
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();
        //alter data source custom properties
        String groupId = "group1";
        String clientId = "client1";
        String defaultOffsets = "OFFSET_BEGINNING";
        String originStmt = "alter routine load for db.job1 " +
                "FROM KAFKA (" +
                "   \"property.group.id\" = \"" + groupId + "\"," +
                "   \"property.client.id\" = \"" + clientId + "\"," +
                "   \"property.kafka_default_offsets\" = \"" + defaultOffsets + "\"" +
                ")";
        routineLoadJob.setOrigStmt(new OriginStatement(originStmt, 0));
        AlterRoutineLoadStmt stmt =
                (AlterRoutineLoadStmt) UtFrameUtils.parseStmtWithNewParser(originStmt, connectContext);
        routineLoadJob.modifyJob(stmt.getRoutineLoadDesc(), stmt.getAnalyzedJobProperties(),
                stmt.getDataSourceProperties(), new OriginStatement(originStmt, 0), true);
        routineLoadJob.convertCustomProperties(true);
        Map<String, String> properties = routineLoadJob.getConvertedCustomProperties();
        Assert.assertEquals(groupId, properties.get("group.id"));
        Assert.assertEquals(clientId, properties.get("client.id"));
        Assert.assertEquals(-2L,
                (long) Deencapsulation.getField(routineLoadJob, "kafkaDefaultOffSet"));
    }

    @Test
    public void testModifyLoadDesc() throws Exception {
        KafkaRoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob();
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();
        //alter load desc
        String originStmt = "alter routine load for db.job1 " +
                "COLUMNS (a, b, c, d=a), " +
                "WHERE a = 1," +
                "COLUMNS TERMINATED BY \",\"," +
                "PARTITION(p1, p2, p3)," +
                "ROWS TERMINATED BY \"A\"";
        routineLoadJob.setOrigStmt(new OriginStatement(originStmt, 0));
        AlterRoutineLoadStmt stmt =
                (AlterRoutineLoadStmt) UtFrameUtils.parseStmtWithNewParser(originStmt, connectContext);
        routineLoadJob.modifyJob(stmt.getRoutineLoadDesc(), stmt.getAnalyzedJobProperties(),
                stmt.getDataSourceProperties(), new OriginStatement(originStmt, 0), true);
        Assert.assertEquals("a,b,c,d=a", Joiner.on(",").join(routineLoadJob.getColumnDescs()));
        Assert.assertEquals("`a` = 1", routineLoadJob.getWhereExpr().toSql());
        Assert.assertEquals("','", routineLoadJob.getColumnSeparator().toString());
        Assert.assertEquals("'A'", routineLoadJob.getRowDelimiter().toString());
        Assert.assertEquals("p1,p2,p3", Joiner.on(",").join(routineLoadJob.getPartitions().getPartitionNames()));
    }

    @Test
    public void testShowCreateKafkaRoutineLoadWithOnlyLoadDesc() throws Exception {
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();
        String originStmt = "CREATE ROUTINE LOAD db.job ON unknown " +
                "PROPERTIES (\"desired_concurrent_number\"=\"1\") " +
                "FROM KAFKA (\"kafka_topic\" = \"topic\",\"kafka_broker_list\" = \"192.168.1.2:10000\"," +
                "\"kafka_partitions\" = \"1,2,3\",\"kafka_offsets\" = \"0,2,4\");";
        CreateRoutineLoadStmt stmt =
                (CreateRoutineLoadStmt) UtFrameUtils.parseStmtWithNewParser(originStmt, connectContext);
        long id = GlobalStateMgr.getCurrentState().getNextId();
        KafkaRoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob(id, stmt.getName(),
                0, 0, stmt.getKafkaBrokerList(), stmt.getKafkaTopic());
        routineLoadJob.setOptional(stmt);
        routineLoadJob.setOrigStmt(new OriginStatement(originStmt, 0));
        Assert.assertEquals("CREATE ROUTINE LOAD db.job ON unknown " +
                        "PROPERTIES (\"desired_concurrent_number\"=\"1\") " +
                        "FROM KAFKA (\"kafka_topic\" = \"topic\",\"kafka_broker_list\" = \"192.168.1.2:10000\"," +
                        "\"kafka_partitions\" = \"1,2,3\",\"kafka_offsets\" = \"0,2,4\");",
                routineLoadJob.getOrigStmt().originStmt);

        // alter routine load
        String alterStr = "ALTER ROUTINE LOAD FOR db.job " +
                "COLUMNS (a, b, c, d=a), " +
                "WHERE a = 1 and b = 2," +
                "COLUMNS TERMINATED BY ';'," +
                "PARTITION(p1, p2, p3)," +
                "ROWS TERMINATED BY \"A\"" +
                "FROM KAFKA (\"property.group.id\" = \"test-group\");";
        AlterRoutineLoadStmt alterStmt =
                (AlterRoutineLoadStmt) UtFrameUtils.parseStmtWithNewParser(alterStr, connectContext);
        routineLoadJob.modifyJob(alterStmt.getRoutineLoadDesc(), null,
                alterStmt.getDataSourceProperties(), new OriginStatement(alterStr, 0), true);
        Assert.assertEquals(
                "CREATE ROUTINE LOAD job ON unknown COLUMNS TERMINATED BY ';', ROWS TERMINATED BY 'A', COLUMNS(`a`, " +
                        "`b`, `c`, `d` = a), PARTITION(`p1`, `p2`, `p3`), WHERE (`a` = 1) AND (`b` = 2) PROPERTIES ( " +
                        "\"max_batch_rows\" = \"200000\", \"desired_concurrent_number\" = \"1\", \"timezone\" = " +
                        "\"Asia/Shanghai\", \"format\" = \"csv\", \"max_error_number\" = \"0\", " +
                        "\"ignore_tail_columns\" = \"false\", \"json_root\" = \"\", \"task_timeout_second\" = \"60\"," +
                        " \"strict_mode\" = \"false\", \"jsonpaths\" = \"\", \"task_consume_second\" = \"15\", " +
                        "\"skip_utf8_check\" = \"false\", \"max_batch_interval\" = \"10\", \"strip_outer_array\" = " +
                        "\"false\" ) FROM KAFKA ( \"kafka_offsets\" = \"0, 2, 4\", \"kafka_partitions\" = \"1, 2, " +
                        "3\", \"kafka_broker_list\" = \"192.168.1.2:10000\", \"kafka_topic\" = \"topic\", \"property" +
                        ".group.id\" = \"test-group\" ) ;",
                routineLoadJob.getOrigStmt().originStmt);
    }

    @Test
    public void testShowCreateKafkaRoutineLoad() throws Exception {
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();
        String originStmt = "CREATE ROUTINE LOAD db.job ON unknown " +
                "PROPERTIES (\"desired_concurrent_number\"=\"1\") " +
                "FROM KAFKA (\"kafka_topic\" = \"topic\",\"kafka_broker_list\" = \"192.168.1.2:10000\"," +
                "\"kafka_partitions\" = \"1,2,3\",\"kafka_offsets\" = \"0,2,4\");";
        CreateRoutineLoadStmt stmt =
                (CreateRoutineLoadStmt) UtFrameUtils.parseStmtWithNewParser(originStmt, connectContext);
        long id = GlobalStateMgr.getCurrentState().getNextId();
        KafkaRoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob(id, stmt.getName(),
                0, 0, stmt.getKafkaBrokerList(), stmt.getKafkaTopic());
        routineLoadJob.setOptional(stmt);
        routineLoadJob.setOrigStmt(new OriginStatement(originStmt, 0));
        Assert.assertEquals("CREATE ROUTINE LOAD db.job ON unknown " +
                        "PROPERTIES (\"desired_concurrent_number\"=\"1\") " +
                        "FROM KAFKA (\"kafka_topic\" = \"topic\",\"kafka_broker_list\" = \"192.168.1.2:10000\"," +
                        "\"kafka_partitions\" = \"1,2,3\",\"kafka_offsets\" = \"0,2,4\");",
                routineLoadJob.getOrigStmt().originStmt);

        // alter routine load
        String alterStr = "ALTER ROUTINE LOAD FOR db.job " +
                "COLUMNS (a, b, c, d=a), " +
                "WHERE a = 1 and b = 2," +
                "COLUMNS TERMINATED BY ';'," +
                "PARTITION(p1, p2, p3)," +
                "ROWS TERMINATED BY \"A\"" +
                "PROPERTIES (\"desired_concurrent_number\"=\"2\"," +
                "\"max_batch_rows\"=\"300000\"," +
                "\"strict_mode\"=\"true\"," +
                "\"max_batch_size\"=\"240000\"," +
                "\"max_error_number\"=\"5\"," +
                "\"skip_utf8_check\"=\"true\"," +
                "\"max_batch_interval\"=\"15\"," +
                " \"strip_outer_array\" = \"true\"," +
                " \"ignore_tail_columns\" = \"true\") " +
                "FROM KAFKA (\"kafka_partitions\" = \"1,2,3\"," +
                "\"kafka_offsets\" = \"11,22,33\"," +
                "\"property.group.id\" = \"test-group\"," +
                "\"property.kafka_default_offsets\" = \"OFFSET_BEGINNING\");";
        AlterRoutineLoadStmt alterStmt =
                (AlterRoutineLoadStmt) UtFrameUtils.parseStmtWithNewParser(alterStr, connectContext);
        routineLoadJob.modifyJob(alterStmt.getRoutineLoadDesc(), alterStmt.getAnalyzedJobProperties(),
                alterStmt.getDataSourceProperties(), new OriginStatement(alterStr, 0), true);
        Assert.assertEquals(
                "CREATE ROUTINE LOAD job ON unknown COLUMNS TERMINATED BY ';', ROWS TERMINATED BY 'A', COLUMNS(`a`, " +
                        "`b`, `c`, `d` = a), PARTITION(`p1`, `p2`, `p3`), WHERE (`a` = 1) AND (`b` = 2) PROPERTIES ( " +
                        "\"max_batch_rows\" = \"300000\", \"desired_concurrent_number\" = \"2\", \"timezone\" = " +
                        "\"Asia/Shanghai\", \"format\" = \"csv\", \"max_error_number\" = \"5\", " +
                        "\"ignore_tail_columns\" = \"true\", \"json_root\" = \"\", \"task_timeout_second\" = \"60\", " +
                        "\"strict_mode\" = \"true\", \"jsonpaths\" = \"\", \"task_consume_second\" = \"15\", " +
                        "\"skip_utf8_check\" = \"true\", \"max_batch_interval\" = \"15\", \"strip_outer_array\" = " +
                        "\"true\" ) FROM KAFKA ( \"kafka_offsets\" = \"11, 22, 33\", \"property" +
                        ".kafka_default_offsets\" = \"OFFSET_BEGINNING\", \"kafka_partitions\" = \"1, 2, 3\", " +
                        "\"kafka_broker_list\" = \"192.168.1.2:10000\", \"kafka_topic\" = \"topic\", \"property.group" +
                        ".id\" = \"test-group\" ) ;",
                routineLoadJob.getOrigStmt().originStmt);
    }

    @Test
    public void testShowCreatePulsarRoutineLoad() throws Exception {
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();
        String originStmt = "CREATE ROUTINE LOAD db.job ON unknown " +
                "PROPERTIES (\"desired_concurrent_number\"=\"1\",\"ignore_tail_columns\" = \"false\"," +
                "\"skip_utf8_check\" = \"false\") " +
                "FROM PULSAR (\"pulsar_service_url\" = \"pulsar://localhost:6650 \",\n" +
                "\"pulsar_topic\" = \"persistent://tenant/namespace/topic-name \",\n" +
                "\"pulsar_subscription\" = \"load-test\",\n" +
                "\"pulsar_partitions\" = \"0,1\",\n" +
                "\"pulsar_initial_positions\" = \"POSITION_EARLIEST,POSITION_EARLIEST\",\n" +
                "\"property.pulsar_default_initial_position\" = \"POSITION_LATEST\",\n" +
                "\"property.auth.token\" = \"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUJzdWIiOiJqaXV0aWFuY2hlbiJ9" +
                ".lulGngOC72vE70OW54zcbyw7XdKSOxET94WT_hIqD5Y\");";
        CreateRoutineLoadStmt stmt =
                (CreateRoutineLoadStmt) UtFrameUtils.parseStmtWithNewParser(originStmt, connectContext);
        long id = GlobalStateMgr.getCurrentState().getNextId();
        PulsarRoutineLoadJob routineLoadJob = new PulsarRoutineLoadJob(id, stmt.getName(),
                0, 0, stmt.getPulsarServiceUrl(), stmt.getPulsarTopic(), stmt.getPulsarSubscription());
        routineLoadJob.setOptional(stmt);
        routineLoadJob.setOrigStmt(new OriginStatement(originStmt, 0));
        Assert.assertEquals("CREATE ROUTINE LOAD db.job ON unknown " +
                        "PROPERTIES (\"desired_concurrent_number\"=\"1\",\"ignore_tail_columns\" = \"false\"," +
                        "\"skip_utf8_check\" = \"false\") " +
                        "FROM PULSAR (\"pulsar_service_url\" = \"pulsar://localhost:6650 \",\n" +
                        "\"pulsar_topic\" = \"persistent://tenant/namespace/topic-name \",\n" +
                        "\"pulsar_subscription\" = \"load-test\",\n" +
                        "\"pulsar_partitions\" = \"0,1\",\n" +
                        "\"pulsar_initial_positions\" = \"POSITION_EARLIEST,POSITION_EARLIEST\",\n" +
                        "\"property.pulsar_default_initial_position\" = \"POSITION_LATEST\",\n" +
                        "\"property.auth.token\" = \"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUJzdWIiOiJqaXV0aWFuY2hlbiJ9" +
                        ".lulGngOC72vE70OW54zcbyw7XdKSOxET94WT_hIqD5Y\");",
                routineLoadJob.getOrigStmt().originStmt);

        // alter routine load
        String alterStr = "ALTER ROUTINE LOAD FOR db.job " +
                "COLUMNS (a, b, c, d=a), " +
                "WHERE a = 1 and b = 2," +
                "COLUMNS TERMINATED BY ';'," +
                "PARTITION(p1, p2, p3)," +
                "ROWS TERMINATED BY \"A\"" +
                "PROPERTIES (\"desired_concurrent_number\"=\"2\",\"max_batch_rows\"=\"300000\"," +
                "\"strict_mode\"=\"true\",\"max_batch_size\"=\"240000\",\"max_error_number\"=\"5\"," +
                "\"skip_utf8_check\"=\"true\",\"max_batch_interval\"=\"15\", \"strip_outer_array\" = \"true\", " +
                "\"ignore_tail_columns\" = \"true\") " +
                "FROM PULSAR (\"pulsar_partitions\" = \"0\",\"pulsar_initial_positions\" = \"POSITION_LATEST\"," +
                "\"property.pulsar_default_initial_position\" = \"POSITION_EARLIEST\",\"property.auth.token\" = " +
                "\"testToken\");";
        AlterRoutineLoadStmt alterStmt =
                (AlterRoutineLoadStmt) UtFrameUtils.parseStmtWithNewParser(alterStr, connectContext);
        routineLoadJob.modifyJob(alterStmt.getRoutineLoadDesc(), alterStmt.getAnalyzedJobProperties(),
                alterStmt.getDataSourceProperties(), new OriginStatement(alterStr, 0), true);
        Assert.assertEquals(
                "CREATE ROUTINE LOAD job ON unknown COLUMNS TERMINATED BY ';', ROWS TERMINATED BY 'A', COLUMNS(`a`, " +
                        "`b`, `c`, `d` = a), PARTITION(`p1`, `p2`, `p3`), WHERE (`a` = 1) AND (`b` = 2) PROPERTIES ( " +
                        "\"max_batch_rows\" = \"300000\", \"desired_concurrent_number\" = \"2\", \"timezone\" = " +
                        "\"Asia/Shanghai\", \"format\" = \"csv\", \"max_error_number\" = \"5\", " +
                        "\"ignore_tail_columns\" = \"true\", \"json_root\" = \"\", \"task_timeout_second\" = \"60\", " +
                        "\"strict_mode\" = \"true\", \"jsonpaths\" = \"\", \"task_consume_second\" = \"15\", " +
                        "\"skip_utf8_check\" = \"true\", \"max_batch_interval\" = \"15\", \"strip_outer_array\" = " +
                        "\"true\" ) FROM PULSAR ( \"pulsar_service_url\" = \"pulsar://localhost:6650\", \"property" +
                        ".auth.token\" = \"testToken\", \"property.pulsar_default_initial_position\" = " +
                        "\"POSITION_EARLIEST\", \"pulsar_subscription\" = \"load-test\", \"pulsar_partitions\" = \"0," +
                        "1\", \"pulsar_initial_positions\" = \"POSITION_LATEST,POSITION_EARLIEST\", \"pulsar_topic\" " +
                        "= \"persistent://tenant/namespace/topic-name\" ) ;",
                routineLoadJob.getOrigStmt().originStmt);
    }

    @Test
    public void testShowCreateTubeRoutineLoad() throws Exception {
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();
        String originStmt = "CREATE ROUTINE LOAD db.job ON unknown " +
                "COLUMNS TERMINATED BY \",\",\n" +
                "ROWS TERMINATED BY \"\\n\",\n" +
                "COLUMNS (event_time, channel, user, is_anonymous, is_minor, is_new, is_robot, is_unpatrolled, delta," +
                " added, deleted),\n" +
                "WHERE event_time > \"2022-01-01 00:00:00\"\n" +
                "PROPERTIES\n" +
                "(\n" +
                "\"desired_concurrent_number\" = \"8\",\n" +
                "\"max_batch_interval\" = \"60\",\n" +
                "\"max_error_number\" = \"1000\"\n" +
                ")\n" +
                "FROM TUBE\n" +
                "(\n" +
                "  \"tube_master_addr\" = \"localhost:8099\",\n" +
                "  \"tube_topic\" = \"starrocks_test\",\n" +
                "  \"tube_group_name\" = \"test_consume\",\n" +
                "  \"tube_tid\" = \"tid_filter\"\n" +
                ");";
        CreateRoutineLoadStmt stmt =
                (CreateRoutineLoadStmt) UtFrameUtils.parseStmtWithNewParser(originStmt, connectContext);
        long id = GlobalStateMgr.getCurrentState().getNextId();
        TubeRoutineLoadJob routineLoadJob = new TubeRoutineLoadJob(id, stmt.getName(),
                0, 0, stmt.getTubeMasterAddr(), stmt.getTubeTopic(), stmt.getTubeGroupName());
        routineLoadJob.setOptional(stmt);
        routineLoadJob.setOrigStmt(new OriginStatement(originStmt, 0));
        Assert.assertEquals("CREATE ROUTINE LOAD db.job ON unknown " +
                        "COLUMNS TERMINATED BY \",\",\n" +
                        "ROWS TERMINATED BY \"\\n\",\n" +
                        "COLUMNS (event_time, channel, user, is_anonymous, is_minor, is_new, is_robot, " +
                        "is_unpatrolled, delta," +
                        " added, deleted),\n" +
                        "WHERE event_time > \"2022-01-01 00:00:00\"\n" +
                        "PROPERTIES\n" +
                        "(\n" +
                        "\"desired_concurrent_number\" = \"8\",\n" +
                        "\"max_batch_interval\" = \"60\",\n" +
                        "\"max_error_number\" = \"1000\"\n" +
                        ")\n" +
                        "FROM TUBE\n" +
                        "(\n" +
                        "  \"tube_master_addr\" = \"localhost:8099\",\n" +
                        "  \"tube_topic\" = \"starrocks_test\",\n" +
                        "  \"tube_group_name\" = \"test_consume\",\n" +
                        "  \"tube_tid\" = \"tid_filter\"\n" +
                        ");",
                routineLoadJob.getOrigStmt().originStmt);

        // alter routine load
        String alterStr = "ALTER ROUTINE LOAD FOR db.job " +
                "COLUMNS (a, b, c, d=a), " +
                "WHERE a = 1 and b = 2," +
                "COLUMNS TERMINATED BY ';'," +
                "PARTITION(p1, p2, p3)," +
                "ROWS TERMINATED BY \"A\"" +
                "PROPERTIES (\"desired_concurrent_number\"=\"2\"," +
                "\"max_batch_rows\"=\"300000\"," +
                "\"strict_mode\"=\"true\"," +
                "\"max_batch_size\"=\"240000\"," +
                "\"max_error_number\"=\"5\"," +
                "\"skip_utf8_check\"=\"true\"," +
                "\"max_batch_interval\"=\"15\"," +
                " \"strip_outer_array\" = \"true\"," +
                " \"ignore_tail_columns\" = \"true\") " +
                "FROM TUBE (\"tube_consume_position\" = \"FROM_MAX\");";
        AlterRoutineLoadStmt alterStmt =
                (AlterRoutineLoadStmt) UtFrameUtils.parseStmtWithNewParser(alterStr, connectContext);
        routineLoadJob.modifyJob(alterStmt.getRoutineLoadDesc(), alterStmt.getAnalyzedJobProperties(),
                alterStmt.getDataSourceProperties(), new OriginStatement(alterStr, 0), true);
        Assert.assertEquals(
                "CREATE ROUTINE LOAD job ON unknown COLUMNS TERMINATED BY ';', ROWS TERMINATED BY 'A', COLUMNS(`a`, " +
                        "`b`, `c`, `d` = a), PARTITION(`p1`, `p2`, `p3`), WHERE (`a` = 1) AND (`b` = 2) PROPERTIES ( " +
                        "\"max_batch_rows\" = \"300000\", \"desired_concurrent_number\" = \"2\", \"timezone\" = " +
                        "\"Asia/Shanghai\", \"format\" = \"csv\", \"max_error_number\" = \"5\", " +
                        "\"ignore_tail_columns\" = \"true\", \"json_root\" = \"\", \"task_timeout_second\" = \"60\", " +
                        "\"strict_mode\" = \"true\", \"jsonpaths\" = \"\", \"task_consume_second\" = \"15\", " +
                        "\"skip_utf8_check\" = \"true\", \"max_batch_interval\" = \"15\", \"strip_outer_array\" = " +
                        "\"true\" ) FROM TUBE ( \"tube_group_name\" = \"test_consume\", \"tube_filters\" = " +
                        "\"tid_filter\", \"tube_master_addr\" = \"localhost:8099\", \"tube_consume_position\" = " +
                        "\"1\", \"tube_topic\" = \"starrocks_test\" ) ;",
                routineLoadJob.getOrigStmt().originStmt);
    }

    @Test
    public void testShowCreateIcebergRoutineLoad() throws Exception {
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();
        String originStmt = "CREATE ROUTINE LOAD db.job ON unknown " +
                "COLUMNS TERMINATED BY \",\",\n" +
                "ROWS TERMINATED BY \"\\n\",\n" +
                "COLUMNS (event_time, channel, user, is_anonymous, is_minor, is_new, is_robot, is_unpatrolled, delta," +
                " added, deleted),\n" +
                "WHERE event_time > \"2022-01-01 00:00:00\"\n" +
                "PROPERTIES\n" +
                "(\n" +
                "\"desired_concurrent_number\" = \"8\",\n" +
                "\"max_batch_interval\" = \"60\",\n" +
                "\"max_error_number\" = \"1000\"\n" +
                ")\n" +
                "FROM ICEBERG\n" +
                "(\n" +
                "\"iceberg_catalog_type\"=\"EXTERNAL_CATALOG\", \n" +
                "\"iceberg_catalog_name\" = \"iceberg\",\n" +
                "\"iceberg_database\" = \"iceberg\", \n" +
                "\"iceberg_table\" = \"iceberg_table\",\n" +
                "\"iceberg_where_expr\" = \"event_time > '2022-01-01 00:00:00'\",\n" +
                "\"iceberg_consume_position\" = \"FROM_LATEST\",\n" +
                "\"property.read_iceberg_snapshots_after_timestamp\" = \"1673595411640\",\n" +
                "\"property.plan_split_size\" = \"268435456\"\n" +
                ")\n" +
                "WITH BROKER \"hdfs_broker\";";
        CreateRoutineLoadStmt stmt =
                (CreateRoutineLoadStmt) UtFrameUtils.parseStmtWithNewParser(originStmt, connectContext);
        long id = GlobalStateMgr.getCurrentState().getNextId();
        IcebergRoutineLoadJob routineLoadJob = new IcebergRoutineLoadJob(id, stmt.getName(),
                0, 0, stmt.getCreateIcebergRoutineLoadStmtConfig(), stmt.getBrokerDesc());
        routineLoadJob.setOptional(stmt);
        routineLoadJob.setOrigStmt(new OriginStatement(originStmt, 0));
        Assert.assertEquals("CREATE ROUTINE LOAD db.job ON unknown " +
                "COLUMNS TERMINATED BY \",\",\n" +
                "ROWS TERMINATED BY \"\\n\",\n" +
                "COLUMNS (event_time, channel, user, is_anonymous, is_minor, is_new, is_robot, is_unpatrolled, delta," +
                " added, deleted),\n" +
                "WHERE event_time > \"2022-01-01 00:00:00\"\n" +
                "PROPERTIES\n" +
                "(\n" +
                "\"desired_concurrent_number\" = \"8\",\n" +
                "\"max_batch_interval\" = \"60\",\n" +
                "\"max_error_number\" = \"1000\"\n" +
                ")\n" +
                "FROM ICEBERG\n" +
                "(\n" +
                "\"iceberg_catalog_type\"=\"EXTERNAL_CATALOG\", \n" +
                "\"iceberg_catalog_name\" = \"iceberg\",\n" +
                "\"iceberg_database\" = \"iceberg\", \n" +
                "\"iceberg_table\" = \"iceberg_table\",\n" +
                "\"iceberg_where_expr\" = \"event_time > '2022-01-01 00:00:00'\",\n" +
                "\"iceberg_consume_position\" = \"FROM_LATEST\",\n" +
                "\"property.read_iceberg_snapshots_after_timestamp\" = \"1673595411640\",\n" +
                "\"property.plan_split_size\" = \"268435456\"\n" +
                ")\n" +
                "WITH BROKER \"hdfs_broker\";", routineLoadJob.getOrigStmt().originStmt);

        // alter routine load
        String alterStr = "ALTER ROUTINE LOAD FOR db.job " +
                "COLUMNS (a, b, c, d=a), " +
                "WHERE a = 1 and b = 2," +
                "COLUMNS TERMINATED BY ';'," +
                "PARTITION(p1, p2, p3)," +
                "ROWS TERMINATED BY \"A\"" +
                "PROPERTIES (\"desired_concurrent_number\"=\"2\"," +
                "\"max_batch_rows\"=\"300000\"," +
                "\"strict_mode\"=\"true\"," +
                "\"max_batch_size\"=\"240000\"," +
                "\"max_error_number\"=\"5\"," +
                "\"skip_utf8_check\"=\"true\"," +
                "\"max_batch_interval\"=\"15\"," +
                " \"strip_outer_array\" = \"true\"," +
                " \"ignore_tail_columns\" = \"true\") " +
                "FROM ICEBERG (\"property.plan_split_size\" = \"999999999\");";
        AlterRoutineLoadStmt alterStmt =
                (AlterRoutineLoadStmt) UtFrameUtils.parseStmtWithNewParser(alterStr, connectContext);
        routineLoadJob.modifyJob(alterStmt.getRoutineLoadDesc(), alterStmt.getAnalyzedJobProperties(),
                alterStmt.getDataSourceProperties(), new OriginStatement(alterStr, 0), true);
        Assert.assertEquals(
                "CREATE ROUTINE LOAD job ON unknown COLUMNS TERMINATED BY ';', ROWS TERMINATED BY 'A', COLUMNS(`a`, " +
                        "`b`, `c`, `d` = a), PARTITION(`p1`, `p2`, `p3`), WHERE (`a` = 1) AND (`b` = 2) PROPERTIES ( " +
                        "\"max_batch_rows\" = \"300000\", \"desired_concurrent_number\" = \"2\", \"timezone\" = " +
                        "\"Asia/Shanghai\", \"format\" = \"csv\", \"max_error_number\" = \"5\", " +
                        "\"ignore_tail_columns\" = \"true\", \"json_root\" = \"\", \"task_timeout_second\" = \"900\"," +
                        " \"strict_mode\" = \"true\", \"jsonpaths\" = \"\", \"task_consume_second\" = \"15\", " +
                        "\"skip_utf8_check\" = \"true\", \"max_batch_interval\" = \"15\", \"strip_outer_array\" = " +
                        "\"true\" ) FROM ICEBERG ( \"iceberg_catalog_type\" = \"EXTERNAL_CATALOG\", " +
                        "\"iceberg_consume_position\" = \"FROM_LATEST\", \"property.plan_split_size\" = " +
                        "\"999999999\", \"iceberg_catalog_name\" = \"iceberg\", \"iceberg_database\" = \"iceberg\", " +
                        "\"iceberg_where_expr\" = \"event_time > '2022-01-01 00:00:00'\", \"iceberg_table\" = " +
                        "\"iceberg_table\", \"property.iceberg_where_expr\" = \"event_time > '2022-01-01 00:00:00'\"," +
                        " \"property.read_iceberg_snapshots_after_timestamp\" = \"1673595411640\" ) WITH BROKER " +
                        "hdfs_broker;",
                routineLoadJob.getOrigStmt().originStmt);
    }

    @Test
    public void testShowCreateIcebergRoutineLoad2() throws Exception {
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();
        String originStmt = "CREATE ROUTINE LOAD db.job ON unknown " +
                "COLUMNS TERMINATED BY \",\",\n" +
                "ROWS TERMINATED BY \"\\n\",\n" +
                "COLUMNS (event_time, channel, user, is_anonymous, is_minor, is_new, is_robot, is_unpatrolled, delta," +
                " added, deleted),\n" +
                "WHERE event_time > \"2022-01-01 00:00:00\"\n" +
                "PROPERTIES\n" +
                "(\n" +
                "\"desired_concurrent_number\" = \"8\",\n" +
                "\"max_batch_interval\" = \"60\",\n" +
                "\"max_error_number\" = \"1000\"\n" +
                ")\n" +
                "FROM ICEBERG\n" +
                "(\n" +
                "\"iceberg_catalog_type\"=\"EXTERNAL_CATALOG\", \n" +
                "\"iceberg_catalog_name\" = \"iceberg\",\n" +
                "\"iceberg_database\" = \"iceberg\", \n" +
                "\"iceberg_table\" = \"iceberg_table\",\n" +
                "\"iceberg_where_expr\" = \"event_time > '2022-01-01 00:00:00'\",\n" +
                "\"iceberg_consume_position\" = \"FROM_LATEST\",\n" +
                "\"property.read_iceberg_snapshots_after_timestamp\" = \"1673595411640\",\n" +
                "\"property.plan_split_size\" = \"268435456\"\n" +
                ");";
        CreateRoutineLoadStmt stmt =
                (CreateRoutineLoadStmt) UtFrameUtils.parseStmtWithNewParser(originStmt, connectContext);
        long id = GlobalStateMgr.getCurrentState().getNextId();
        IcebergRoutineLoadJob routineLoadJob = new IcebergRoutineLoadJob(id, stmt.getName(),
                0, 0, stmt.getCreateIcebergRoutineLoadStmtConfig(), stmt.getBrokerDesc());
        routineLoadJob.setOptional(stmt);
        routineLoadJob.setOrigStmt(new OriginStatement(originStmt, 0));

        // alter routine load
        String alterStr = "ALTER ROUTINE LOAD FOR db.job " +
                "COLUMNS (a, b, c, d=a), " +
                "WHERE a = 1 and b = 2," +
                "COLUMNS TERMINATED BY ';'," +
                "PARTITION(p1, p2, p3)," +
                "ROWS TERMINATED BY \"A\"" +
                "PROPERTIES (\"desired_concurrent_number\"=\"2\"," +
                "\"max_batch_rows\"=\"300000\"," +
                "\"strict_mode\"=\"true\"," +
                "\"max_batch_size\"=\"240000\"," +
                "\"max_error_number\"=\"5\"," +
                "\"skip_utf8_check\"=\"true\"," +
                "\"max_batch_interval\"=\"15\"," +
                " \"strip_outer_array\" = \"true\"," +
                " \"ignore_tail_columns\" = \"true\") " +
                "FROM ICEBERG (\"property.plan_split_size\" = \"999999999\");";
        AlterRoutineLoadStmt alterStmt =
                (AlterRoutineLoadStmt) UtFrameUtils.parseStmtWithNewParser(alterStr, connectContext);
        routineLoadJob.modifyJob(alterStmt.getRoutineLoadDesc(), alterStmt.getAnalyzedJobProperties(),
                alterStmt.getDataSourceProperties(), new OriginStatement(alterStr, 0), true);
        Assert.assertEquals(
                "CREATE ROUTINE LOAD job ON unknown COLUMNS TERMINATED BY ';', ROWS TERMINATED BY 'A', COLUMNS(`a`, " +
                        "`b`, `c`, `d` = a), PARTITION(`p1`, `p2`, `p3`), WHERE (`a` = 1) AND (`b` = 2) PROPERTIES ( " +
                        "\"max_batch_rows\" = \"300000\", \"desired_concurrent_number\" = \"2\", \"timezone\" = " +
                        "\"Asia/Shanghai\", \"format\" = \"csv\", \"max_error_number\" = \"5\", " +
                        "\"ignore_tail_columns\" = \"true\", \"json_root\" = \"\", \"task_timeout_second\" = \"900\"," +
                        " \"strict_mode\" = \"true\", \"jsonpaths\" = \"\", \"task_consume_second\" = \"15\", " +
                        "\"skip_utf8_check\" = \"true\", \"max_batch_interval\" = \"15\", \"strip_outer_array\" = " +
                        "\"true\" ) FROM ICEBERG ( \"iceberg_catalog_type\" = \"EXTERNAL_CATALOG\", " +
                        "\"iceberg_consume_position\" = \"FROM_LATEST\", \"property.plan_split_size\" = " +
                        "\"999999999\", \"iceberg_catalog_name\" = \"iceberg\", \"iceberg_database\" = \"iceberg\", " +
                        "\"iceberg_where_expr\" = \"event_time > '2022-01-01 00:00:00'\", \"iceberg_table\" = " +
                        "\"iceberg_table\", \"property.iceberg_where_expr\" = \"event_time > '2022-01-01 00:00:00'\"," +
                        " \"property.read_iceberg_snapshots_after_timestamp\" = \"1673595411640\" ) ;",
                routineLoadJob.getOrigStmt().originStmt);
    }

    @Test
    public void testMergeLoadDescToOriginStatement() throws Exception {
        KafkaRoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob(1L, "job",
                2L, 3L, "192.168.1.2:10000", "topic");
        String originStmt = "CREATE ROUTINE LOAD job ON unknown " +
                "PROPERTIES (\"desired_concurrent_number\"=\"1\") " +
                "FROM KAFKA (\"kafka_topic\" = \"my_topic\")";
        routineLoadJob.setOrigStmt(new OriginStatement(originStmt, 0));

        // alter columns terminator
        RoutineLoadDesc loadDesc = CreateRoutineLoadStmt.getLoadDesc(new OriginStatement(
                "ALTER ROUTINE LOAD FOR job " +
                        "COLUMNS TERMINATED BY ';'", 0), null);
        routineLoadJob.setRoutineLoadDesc(loadDesc);
        routineLoadJob.updateOriginStatement();
        Assert.assertEquals(
                "CREATE ROUTINE LOAD job ON unknown COLUMNS TERMINATED BY ';' PROPERTIES ( \"max_batch_rows\" = " +
                        "\"200000\", \"desired_concurrent_number\" = \"0\", \"timezone\" = \"Asia/Shanghai\", " +
                        "\"format\" = \"csv\", \"max_error_number\" = \"0\", \"ignore_tail_columns\" = \"false\", " +
                        "\"json_root\" = \"\", \"task_timeout_second\" = \"60\", \"strict_mode\" = \"false\", " +
                        "\"jsonpaths\" = \"\", \"task_consume_second\" = \"15\", \"skip_utf8_check\" = \"false\", " +
                        "\"max_batch_interval\" = \"10\", \"strip_outer_array\" = \"false\" ) FROM KAFKA ( " +
                        "\"kafka_broker_list\" = \"192.168.1.2:10000\", \"kafka_topic\" = \"topic\" ) ;",
                routineLoadJob.getOrigStmt().originStmt);

        // alter rows terminator
        loadDesc = CreateRoutineLoadStmt.getLoadDesc(new OriginStatement(
                "ALTER ROUTINE LOAD FOR job " +
                        "ROWS TERMINATED BY '\n'", 0), null);
        routineLoadJob.setRoutineLoadDesc(loadDesc);
        routineLoadJob.updateOriginStatement();
        Assert.assertEquals("CREATE ROUTINE LOAD job ON unknown " +
                        "COLUMNS TERMINATED BY ';', " +
                        "ROWS TERMINATED BY '\n' " +
                        "PROPERTIES ( \"max_batch_rows\" = \"200000\", \"desired_concurrent_number\" = \"0\", " +
                        "\"timezone\" = \"Asia/Shanghai\", \"format\" = \"csv\", \"max_error_number\" = \"0\", " +
                        "\"ignore_tail_columns\" = \"false\", \"json_root\" = \"\", \"task_timeout_second\" = \"60\"," +
                        " \"strict_mode\" = \"false\", \"jsonpaths\" = \"\", \"task_consume_second\" = \"15\", " +
                        "\"skip_utf8_check\" = \"false\", \"max_batch_interval\" = \"10\", \"strip_outer_array\" = " +
                        "\"false\" ) FROM KAFKA ( \"kafka_broker_list\" = \"192.168.1.2:10000\", \"kafka_topic\" = " +
                        "\"topic\" ) ;",
                routineLoadJob.getOrigStmt().originStmt);

        // alter columns
        loadDesc = CreateRoutineLoadStmt.getLoadDesc(new OriginStatement(
                "ALTER ROUTINE LOAD FOR job " +
                        "COLUMNS(`a`, `b`, `c`=1)", 0), null);
        routineLoadJob.setRoutineLoadDesc(loadDesc);
        routineLoadJob.updateOriginStatement();
        Assert.assertEquals("CREATE ROUTINE LOAD job ON unknown " +
                        "COLUMNS TERMINATED BY ';', " +
                        "ROWS TERMINATED BY '\n', " +
                        "COLUMNS(`a`, `b`, `c` = 1) PROPERTIES ( \"max_batch_rows\" = \"200000\", " +
                        "\"desired_concurrent_number\" = \"0\", \"timezone\" = \"Asia/Shanghai\", \"format\" = " +
                        "\"csv\", \"max_error_number\" = \"0\", \"ignore_tail_columns\" = \"false\", \"json_root\" = " +
                        "\"\", \"task_timeout_second\" = \"60\", \"strict_mode\" = \"false\", \"jsonpaths\" = \"\", " +
                        "\"task_consume_second\" = \"15\", \"skip_utf8_check\" = \"false\", \"max_batch_interval\" = " +
                        "\"10\", \"strip_outer_array\" = \"false\" ) FROM KAFKA ( \"kafka_broker_list\" = \"192.168.1" +
                        ".2:10000\", \"kafka_topic\" = \"topic\" ) ;",
                routineLoadJob.getOrigStmt().originStmt);

        // alter partition
        loadDesc = CreateRoutineLoadStmt.getLoadDesc(new OriginStatement(
                "ALTER ROUTINE LOAD FOR job " +
                        "TEMPORARY PARTITION(`p1`, `p2`)", 0), null);
        routineLoadJob.setRoutineLoadDesc(loadDesc);
        routineLoadJob.updateOriginStatement();
        Assert.assertEquals(
                "CREATE ROUTINE LOAD job ON unknown " +
                        "COLUMNS TERMINATED BY ';', " +
                        "ROWS TERMINATED BY '\n', " +
                        "COLUMNS(`a`, `b`, `c` = 1), TEMPORARY PARTITION(`p1`, `p2`) PROPERTIES ( \"max_batch_rows\" " +
                        "= \"200000\", \"desired_concurrent_number\" = \"0\", \"timezone\" = \"Asia/Shanghai\", " +
                        "\"format\" = \"csv\", \"max_error_number\" = \"0\", \"ignore_tail_columns\" = \"false\", " +
                        "\"json_root\" = \"\", \"task_timeout_second\" = \"60\", \"strict_mode\" = \"false\", " +
                        "\"jsonpaths\" = \"\", \"task_consume_second\" = \"15\", \"skip_utf8_check\" = \"false\", " +
                        "\"max_batch_interval\" = \"10\", \"strip_outer_array\" = \"false\" ) FROM KAFKA ( " +
                        "\"kafka_broker_list\" = \"192.168.1.2:10000\", \"kafka_topic\" = \"topic\" ) ;",
                routineLoadJob.getOrigStmt().originStmt);

        // alter where
        loadDesc = CreateRoutineLoadStmt.getLoadDesc(new OriginStatement(
                "ALTER ROUTINE LOAD FOR job " +
                        "WHERE a = 1", 0), null);
        routineLoadJob.setRoutineLoadDesc(loadDesc);
        routineLoadJob.updateOriginStatement();
        Assert.assertEquals("CREATE ROUTINE LOAD job ON unknown " +
                        "COLUMNS TERMINATED BY ';', " +
                        "ROWS TERMINATED BY '\n', " +
                        "COLUMNS(`a`, `b`, `c` = 1), TEMPORARY PARTITION(`p1`, `p2`), WHERE `a` = 1 PROPERTIES ( " +
                        "\"max_batch_rows\" = \"200000\", \"desired_concurrent_number\" = \"0\", \"timezone\" = " +
                        "\"Asia/Shanghai\", \"format\" = \"csv\", \"max_error_number\" = \"0\", " +
                        "\"ignore_tail_columns\" = \"false\", \"json_root\" = \"\", \"task_timeout_second\" = \"60\"," +
                        " \"strict_mode\" = \"false\", \"jsonpaths\" = \"\", \"task_consume_second\" = \"15\", " +
                        "\"skip_utf8_check\" = \"false\", \"max_batch_interval\" = \"10\", \"strip_outer_array\" = " +
                        "\"false\" ) FROM KAFKA ( \"kafka_broker_list\" = \"192.168.1.2:10000\", \"kafka_topic\" = " +
                        "\"topic\" ) ;",
                routineLoadJob.getOrigStmt().originStmt);

        // alter columns terminator again
        loadDesc = CreateRoutineLoadStmt.getLoadDesc(new OriginStatement(
                "ALTER ROUTINE LOAD FOR job " +
                        "COLUMNS TERMINATED BY '\t'", 0), null);
        routineLoadJob.setRoutineLoadDesc(loadDesc);
        routineLoadJob.updateOriginStatement();
        Assert.assertEquals("CREATE ROUTINE LOAD job ON unknown " +
                        "COLUMNS TERMINATED BY '\t', " +
                        "ROWS TERMINATED BY '\n', " +
                        "COLUMNS(`a`, `b`, `c` = 1), TEMPORARY PARTITION(`p1`, `p2`), WHERE `a` = 1 PROPERTIES ( " +
                        "\"max_batch_rows\" = \"200000\", \"desired_concurrent_number\" = \"0\", \"timezone\" = " +
                        "\"Asia/Shanghai\", \"format\" = \"csv\", \"max_error_number\" = \"0\", " +
                        "\"ignore_tail_columns\" = \"false\", \"json_root\" = \"\", \"task_timeout_second\" = \"60\"," +
                        " \"strict_mode\" = \"false\", \"jsonpaths\" = \"\", \"task_consume_second\" = \"15\", " +
                        "\"skip_utf8_check\" = \"false\", \"max_batch_interval\" = \"10\", \"strip_outer_array\" = " +
                        "\"false\" ) FROM KAFKA ( \"kafka_broker_list\" = \"192.168.1.2:10000\", \"kafka_topic\" = " +
                        "\"topic\" ) ;",
                routineLoadJob.getOrigStmt().originStmt);

        // alter rows terminator again
        loadDesc = CreateRoutineLoadStmt.getLoadDesc(new OriginStatement(
                "ALTER ROUTINE LOAD FOR job " +
                        "ROWS TERMINATED BY 'a'", 0), null);
        routineLoadJob.setRoutineLoadDesc(loadDesc);
        routineLoadJob.updateOriginStatement();
        Assert.assertEquals(
                "CREATE ROUTINE LOAD job ON unknown COLUMNS TERMINATED BY '\t', ROWS TERMINATED BY 'a', COLUMNS(`a`, " +
                        "`b`, `c` = 1), TEMPORARY PARTITION(`p1`, `p2`), WHERE `a` = 1 PROPERTIES ( " +
                        "\"max_batch_rows\" = \"200000\", \"desired_concurrent_number\" = \"0\", \"timezone\" = " +
                        "\"Asia/Shanghai\", \"format\" = \"csv\", \"max_error_number\" = \"0\", " +
                        "\"ignore_tail_columns\" = \"false\", \"json_root\" = \"\", \"task_timeout_second\" = \"60\"," +
                        " \"strict_mode\" = \"false\", \"jsonpaths\" = \"\", \"task_consume_second\" = \"15\", " +
                        "\"skip_utf8_check\" = \"false\", \"max_batch_interval\" = \"10\", \"strip_outer_array\" = " +
                        "\"false\" ) FROM KAFKA ( \"kafka_broker_list\" = \"192.168.1.2:10000\", \"kafka_topic\" = " +
                        "\"topic\" ) ;",
                routineLoadJob.getOrigStmt().originStmt);

        // alter columns again
        loadDesc = CreateRoutineLoadStmt.getLoadDesc(new OriginStatement(
                "ALTER ROUTINE LOAD FOR job " +
                        "COLUMNS(`a`)", 0), null);
        routineLoadJob.setRoutineLoadDesc(loadDesc);
        routineLoadJob.updateOriginStatement();
        Assert.assertEquals(
                "CREATE ROUTINE LOAD job ON unknown COLUMNS TERMINATED BY '\t', ROWS TERMINATED BY 'a', COLUMNS(`a`)," +
                        " TEMPORARY PARTITION(`p1`, `p2`), WHERE `a` = 1 PROPERTIES ( \"max_batch_rows\" = " +
                        "\"200000\", \"desired_concurrent_number\" = \"0\", \"timezone\" = \"Asia/Shanghai\", " +
                        "\"format\" = \"csv\", \"max_error_number\" = \"0\", \"ignore_tail_columns\" = \"false\", " +
                        "\"json_root\" = \"\", \"task_timeout_second\" = \"60\", \"strict_mode\" = \"false\", " +
                        "\"jsonpaths\" = \"\", \"task_consume_second\" = \"15\", \"skip_utf8_check\" = \"false\", " +
                        "\"max_batch_interval\" = \"10\", \"strip_outer_array\" = \"false\" ) FROM KAFKA ( " +
                        "\"kafka_broker_list\" = \"192.168.1.2:10000\", \"kafka_topic\" = \"topic\" ) ;",
                routineLoadJob.getOrigStmt().originStmt);
        // alter partition again
        loadDesc = CreateRoutineLoadStmt.getLoadDesc(new OriginStatement(
                "ALTER ROUTINE LOAD FOR job " +
                        " PARTITION(`p1`, `p2`)", 0), null);
        routineLoadJob.setRoutineLoadDesc(loadDesc);
        routineLoadJob.updateOriginStatement();
        Assert.assertEquals(
                "CREATE ROUTINE LOAD job ON unknown COLUMNS TERMINATED BY '\t', ROWS TERMINATED BY 'a', COLUMNS(`a`)," +
                        " PARTITION(`p1`, `p2`), WHERE `a` = 1 PROPERTIES ( \"max_batch_rows\" = \"200000\", " +
                        "\"desired_concurrent_number\" = \"0\", \"timezone\" = \"Asia/Shanghai\", \"format\" = " +
                        "\"csv\", \"max_error_number\" = \"0\", \"ignore_tail_columns\" = \"false\", \"json_root\" = " +
                        "\"\", \"task_timeout_second\" = \"60\", \"strict_mode\" = \"false\", \"jsonpaths\" = \"\", " +
                        "\"task_consume_second\" = \"15\", \"skip_utf8_check\" = \"false\", \"max_batch_interval\" = " +
                        "\"10\", \"strip_outer_array\" = \"false\" ) FROM KAFKA ( \"kafka_broker_list\" = \"192.168.1" +
                        ".2:10000\", \"kafka_topic\" = \"topic\" ) ;",
                routineLoadJob.getOrigStmt().originStmt);

        // alter where again
        loadDesc = CreateRoutineLoadStmt.getLoadDesc(new OriginStatement(
                "ALTER ROUTINE LOAD FOR job " +
                        "WHERE a = 5", 0), null);
        routineLoadJob.setRoutineLoadDesc(loadDesc);
        routineLoadJob.updateOriginStatement();
        Assert.assertEquals(
                "CREATE ROUTINE LOAD job ON unknown COLUMNS TERMINATED BY '\t', ROWS TERMINATED BY 'a', COLUMNS(`a`)," +
                        " PARTITION(`p1`, `p2`), WHERE `a` = 5 PROPERTIES ( \"max_batch_rows\" = \"200000\", " +
                        "\"desired_concurrent_number\" = \"0\", \"timezone\" = \"Asia/Shanghai\", \"format\" = " +
                        "\"csv\", \"max_error_number\" = \"0\", \"ignore_tail_columns\" = \"false\", \"json_root\" = " +
                        "\"\", \"task_timeout_second\" = \"60\", \"strict_mode\" = \"false\", \"jsonpaths\" = \"\", " +
                        "\"task_consume_second\" = \"15\", \"skip_utf8_check\" = \"false\", \"max_batch_interval\" = " +
                        "\"10\", \"strip_outer_array\" = \"false\" ) FROM KAFKA ( \"kafka_broker_list\" = \"192.168.1" +
                        ".2:10000\", \"kafka_topic\" = \"topic\" ) ;",
                routineLoadJob.getOrigStmt().originStmt);

        // alter where again
        loadDesc = CreateRoutineLoadStmt.getLoadDesc(new OriginStatement(
                        "ALTER ROUTINE LOAD FOR job " +
                                "WHERE a = 5 and b like 'c1%' and c between 1 and 100 and substring(d,1,5) = 'cefd' ", 0),
                null);
        routineLoadJob.setRoutineLoadDesc(loadDesc);
        routineLoadJob.updateOriginStatement();
        Assert.assertEquals(
                "CREATE ROUTINE LOAD job ON unknown COLUMNS TERMINATED BY '\t', ROWS TERMINATED BY 'a', COLUMNS(`a`)," +
                        " PARTITION(`p1`, `p2`), WHERE (((`a` = 5) AND (`b` LIKE 'c1%')) AND (`c` BETWEEN 1 AND 100))" +
                        " AND (substring(`d`, 1, 5) = 'cefd') PROPERTIES ( \"max_batch_rows\" = \"200000\", " +
                        "\"desired_concurrent_number\" = \"0\", \"timezone\" = \"Asia/Shanghai\", \"format\" = " +
                        "\"csv\", \"max_error_number\" = \"0\", \"ignore_tail_columns\" = \"false\", \"json_root\" = " +
                        "\"\", \"task_timeout_second\" = \"60\", \"strict_mode\" = \"false\", \"jsonpaths\" = \"\", " +
                        "\"task_consume_second\" = \"15\", \"skip_utf8_check\" = \"false\", \"max_batch_interval\" = " +
                        "\"10\", \"strip_outer_array\" = \"false\" ) FROM KAFKA ( \"kafka_broker_list\" = \"192.168.1" +
                        ".2:10000\", \"kafka_topic\" = \"topic\" ) ;",
                routineLoadJob.getOrigStmt().originStmt);
    }
}
