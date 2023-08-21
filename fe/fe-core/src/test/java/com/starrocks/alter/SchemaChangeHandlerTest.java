// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/alter/SchemaChangeHandlerTest.java

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

package com.starrocks.alter;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.analysis.ColumnPosition;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.FeConstants;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.utframe.TestWithFeService;
import mockit.Expectations;
import mockit.Injectable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class SchemaChangeHandlerTest extends TestWithFeService {
    private static final Logger LOG = LogManager.getLogger(SchemaChangeHandlerTest.class);
    private int jobSize = 0;

    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.default_scheduler_interval_millisecond = 10;
        //create database db1
        createDatabase("test");

        //create tables
        String createAggTblStmtStr = "CREATE TABLE IF NOT EXISTS test.sc_agg (\n" + "user_id LARGEINT NOT NULL,\n"
                + "date DATE NOT NULL,\n" + "city VARCHAR(20),\n" + "age SMALLINT,\n" + "sex TINYINT,\n"
                + "last_visit_date DATETIME REPLACE DEFAULT '1970-01-01 00:00:00',\n" + "cost BIGINT SUM DEFAULT '0',\n"
                + "max_dwell_time INT MAX DEFAULT '0',\n" + "min_dwell_time INT MIN DEFAULT '99999')\n"
                + "AGGREGATE KEY(user_id, date, city, age, sex)\n" + "DISTRIBUTED BY HASH(user_id) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1', 'light_schema_change' = 'true');";
        createTable(createAggTblStmtStr);

        String createUniqTblStmtStr = "CREATE TABLE IF NOT EXISTS test.sc_uniq (\n" + "user_id LARGEINT NOT NULL,\n"
                + "username VARCHAR(50) NOT NULL,\n" + "city VARCHAR(20),\n" + "age SMALLINT,\n" + "sex TINYINT,\n"
                + "phone LARGEINT,\n" + "address VARCHAR(500),\n" + "register_time DATETIME)\n"
                + "UNIQUE  KEY(user_id, username)\n" + "DISTRIBUTED BY HASH(user_id) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1', 'light_schema_change' = 'true');";
        createTable(createUniqTblStmtStr);

        String createDupTblStmtStr = "CREATE TABLE IF NOT EXISTS test.sc_dup (\n" + "timestamp DATETIME,\n"
                + "type INT,\n" + "error_code INT,\n" + "error_msg VARCHAR(1024),\n" + "op_id BIGINT,\n"
                + "op_time DATETIME)\n" + "DUPLICATE  KEY(timestamp, type)\n" + "DISTRIBUTED BY HASH(type) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1', 'light_schema_change' = 'true');";
        createTable(createDupTblStmtStr);

        String createDupTblNLSCStmtStr = "CREATE TABLE IF NOT EXISTS test.sc_dup_nlsc (\n" + "timestamp DATETIME,\n"
                + "type INT,\n" + "error_code INT,\n" + "error_msg VARCHAR(1024),\n" + "op_id BIGINT,\n"
                + "op_time DATETIME)\n" + "DUPLICATE  KEY(timestamp, type)\n" + "DISTRIBUTED BY HASH(type) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1', 'light_schema_change' = 'false');";
        createTable(createDupTblNLSCStmtStr);
    }

    private void waitAlterJobDone(Map<Long, AlterJobV2> alterJobs) throws Exception {
        for (AlterJobV2 alterJobV2 : alterJobs.values()) {
            while (!alterJobV2.getJobState().isFinalState()) {
                LOG.info("alter job {} is running. state: {}", alterJobV2.getJobId(), alterJobV2.getJobState());
                Thread.sleep(1000);
            }
            LOG.info("alter job {} is done. state: {}", alterJobV2.getJobId(), alterJobV2.getJobState());
            Assert.assertEquals(AlterJobV2.JobState.FINISHED, alterJobV2.getJobState());

            Database db = GlobalStateMgr.getCurrentState().getDb(alterJobV2.getDbId());
            OlapTable tbl = (OlapTable) db.getTable(alterJobV2.getTableId());
            while (tbl.getState() != OlapTable.OlapTableState.NORMAL) {
                Thread.sleep(1000);
            }
        }
    }

    @Test
    public void testAggAddOrDropColumn() throws Exception {
        LOG.info("dbName: {}", GlobalStateMgr.getCurrentState().getDbNames());

        Database db = GlobalStateMgr.getCurrentState().getDb("test");
        OlapTable tbl = (OlapTable) db.getTable("sc_agg");
        db.readLock();
        try {
            Assertions.assertNotNull(tbl);
            System.out.println(tbl.getName());
            Assertions.assertEquals("StarRocks", tbl.getEngine());
            Assertions.assertEquals(9, tbl.getBaseSchema().size());
        } finally {
            db.readUnlock();
        }

        //process agg add value column schema change
        String addValColStmtStr = "alter table test.sc_agg add column new_v1 int MAX default '0'";
        AlterTableStmt addValColStmt = (AlterTableStmt) parseAndAnalyzeStmt(addValColStmtStr);
        GlobalStateMgr.getCurrentState().getAlterInstance().processAlterTable(addValColStmt);
        jobSize++;
        // check alter job, do not create job
        Map<Long, AlterJobV2> alterJobs = GlobalStateMgr.getCurrentState().getSchemaChangeHandler().getAlterJobsV2();
        Assertions.assertEquals(jobSize, alterJobs.size());

        db.readLock();
        try {
            Assertions.assertEquals(10, tbl.getBaseSchema().size());
            String baseIndexName = tbl.getIndexNameById(tbl.getBaseIndexId());
            Assertions.assertEquals(baseIndexName, tbl.getName());
            MaterializedIndexMeta indexMeta = tbl.getIndexMetaByIndexId(tbl.getBaseIndexId());
            Assertions.assertNotNull(indexMeta);
        } finally {
            db.readUnlock();
        }

        //process agg add  key column schema change
        String addKeyColStmtStr = "alter table test.sc_agg add column new_k1 int default '1'";
        AlterTableStmt addKeyColStmt = (AlterTableStmt) parseAndAnalyzeStmt(addKeyColStmtStr);
        GlobalStateMgr.getCurrentState().getAlterInstance().processAlterTable(addKeyColStmt);

        //check alter job
        jobSize++;
        Assertions.assertEquals(jobSize, alterJobs.size());
        waitAlterJobDone(alterJobs);

        db.readLock();
        try {
            Assertions.assertEquals(11, tbl.getBaseSchema().size());
            String baseIndexName = tbl.getIndexNameById(tbl.getBaseIndexId());
            Assertions.assertEquals(baseIndexName, tbl.getName());
            MaterializedIndexMeta indexMeta = tbl.getIndexMetaByIndexId(tbl.getBaseIndexId());
            Assertions.assertNotNull(indexMeta);
        } finally {
            db.readUnlock();
        }

        //process agg drop value column schema change
        String dropValColStmtStr = "alter table test.sc_agg drop column new_v1";
        AlterTableStmt dropValColStmt = (AlterTableStmt) parseAndAnalyzeStmt(dropValColStmtStr);
        GlobalStateMgr.getCurrentState().getAlterInstance().processAlterTable(dropValColStmt);
        jobSize++;
        //check alter job, do not create job
        LOG.info("alterJobs:{}", alterJobs);
        Assertions.assertEquals(jobSize, alterJobs.size());

        db.readLock();
        try {
            Assertions.assertEquals(10, tbl.getBaseSchema().size());
            String baseIndexName = tbl.getIndexNameById(tbl.getBaseIndexId());
            Assertions.assertEquals(baseIndexName, tbl.getName());
            MaterializedIndexMeta indexMeta = tbl.getIndexMetaByIndexId(tbl.getBaseIndexId());
            Assertions.assertNotNull(indexMeta);
        } finally {
            db.readUnlock();
        }

        try {
            //process agg drop key column with replace schema change, expect exception.
            String dropKeyColStmtStr = "alter table test.sc_agg drop column new_k1";
            AlterTableStmt dropKeyColStmt = (AlterTableStmt) parseAndAnalyzeStmt(dropKeyColStmtStr);
            GlobalStateMgr.getCurrentState().getAlterInstance().processAlterTable(dropKeyColStmt);
            Assertions.fail();
        } catch (Exception e) {
            LOG.info(e.getMessage());
        }

        LOG.info("getIndexIdToSchema 1: {}", tbl.getIndexIdToSchema());
    }

    @Test
    public void testUniqAddOrDropColumn() throws Exception {

        LOG.info("dbName: {}", GlobalStateMgr.getCurrentState().getDbNames());

        Database db = GlobalStateMgr.getCurrentState().getDb("test");
        OlapTable tbl = (OlapTable) db.getTable("sc_uniq");
        db.readLock();
        try {
            Assertions.assertNotNull(tbl);
            System.out.println(tbl.getName());
            Assertions.assertEquals("StarRocks", tbl.getEngine());
            Assertions.assertEquals(8, tbl.getBaseSchema().size());
        } finally {
            db.readUnlock();
        }

        //process uniq add value column schema change
        String addValColStmtStr = "alter table test.sc_uniq add column new_v1 int default '0'";
        AlterTableStmt addValColStmt = (AlterTableStmt) parseAndAnalyzeStmt(addValColStmtStr);
        GlobalStateMgr.getCurrentState().getAlterInstance().processAlterTable(addValColStmt);
        jobSize++;
        //check alter job, do not create job
        Map<Long, AlterJobV2> alterJobs = GlobalStateMgr.getCurrentState().getSchemaChangeHandler().getAlterJobsV2();
        LOG.info("alterJobs:{}", alterJobs);
        Assertions.assertEquals(jobSize, alterJobs.size());

        db.readLock();
        try {
            Assertions.assertEquals(9, tbl.getBaseSchema().size());
            String baseIndexName = tbl.getIndexNameById(tbl.getBaseIndexId());
            Assertions.assertEquals(baseIndexName, tbl.getName());
            MaterializedIndexMeta indexMeta = tbl.getIndexMetaByIndexId(tbl.getBaseIndexId());
            Assertions.assertNotNull(indexMeta);
        } finally {
            db.readUnlock();
        }

        //process uniq drop val column schema change
        String dropValColStmtStr = "alter table test.sc_uniq drop column new_v1";
        AlterTableStmt dropValColStm = (AlterTableStmt) parseAndAnalyzeStmt(dropValColStmtStr);
        GlobalStateMgr.getCurrentState().getAlterInstance().processAlterTable(dropValColStm);
        jobSize++;
        //check alter job
        Assertions.assertEquals(jobSize, alterJobs.size());
        db.readLock();
        try {
            Assertions.assertEquals(8, tbl.getBaseSchema().size());
            String baseIndexName = tbl.getIndexNameById(tbl.getBaseIndexId());
            Assertions.assertEquals(baseIndexName, tbl.getName());
            MaterializedIndexMeta indexMeta = tbl.getIndexMetaByIndexId(tbl.getBaseIndexId());
            Assertions.assertNotNull(indexMeta);
        } finally {
            db.readUnlock();
        }
    }

    @Test
    public void testDupAddOrDropColumn() throws Exception {

        LOG.info("dbName: {}", GlobalStateMgr.getCurrentState().getDbNames());

        Database db = GlobalStateMgr.getCurrentState().getDb("test");
        OlapTable tbl = (OlapTable) db.getTable("sc_dup");
        db.readLock();
        try {
            Assertions.assertNotNull(tbl);
            System.out.println(tbl.getName());
            Assertions.assertEquals("StarRocks", tbl.getEngine());
            Assertions.assertEquals(6, tbl.getBaseSchema().size());
        } finally {
            db.readUnlock();
        }

        //process uniq add value column schema change
        String addValColStmtStr = "alter table test.sc_dup add column new_v1 int default '0'";
        AlterTableStmt addValColStmt = (AlterTableStmt) parseAndAnalyzeStmt(addValColStmtStr);
        GlobalStateMgr.getCurrentState().getAlterInstance().processAlterTable(addValColStmt);
        jobSize++;
        //check alter job, do not create job
        Map<Long, AlterJobV2> alterJobs = GlobalStateMgr.getCurrentState().getSchemaChangeHandler().getAlterJobsV2();
        LOG.info("alterJobs:{}", alterJobs);
        Assertions.assertEquals(jobSize, alterJobs.size());

        db.readLock();
        try {
            Assertions.assertEquals(7, tbl.getBaseSchema().size());
            String baseIndexName = tbl.getIndexNameById(tbl.getBaseIndexId());
            Assertions.assertEquals(baseIndexName, tbl.getName());
            MaterializedIndexMeta indexMeta = tbl.getIndexMetaByIndexId(tbl.getBaseIndexId());
            Assertions.assertNotNull(indexMeta);
        } finally {
            db.readUnlock();
        }

        //process uniq drop val column schema change
        String dropValColStmtStr = "alter table test.sc_dup drop column new_v1";
        AlterTableStmt dropValColStm = (AlterTableStmt) parseAndAnalyzeStmt(dropValColStmtStr);
        GlobalStateMgr.getCurrentState().getAlterInstance().processAlterTable(dropValColStm);
        jobSize++;
        //check alter job
        Assertions.assertEquals(jobSize, alterJobs.size());
        db.readLock();
        try {
            Assertions.assertEquals(6, tbl.getBaseSchema().size());
            String baseIndexName = tbl.getIndexNameById(tbl.getBaseIndexId());
            Assertions.assertEquals(baseIndexName, tbl.getName());
            MaterializedIndexMeta indexMeta = tbl.getIndexMetaByIndexId(tbl.getBaseIndexId());
            Assertions.assertNotNull(indexMeta);
        } finally {
            db.readUnlock();
        }
    }

    @Test
    public void testDupAddOrDropColumnWithAlterLSCProp() throws Exception {

        LOG.info("dbName: {}", GlobalStateMgr.getCurrentState().getDbNames());

        Database db = GlobalStateMgr.getCurrentState().getDb("test");
        OlapTable tbl = (OlapTable) db.getTable("sc_dup_nlsc");
        db.readLock();
        try {
            Assertions.assertNotNull(tbl);
            System.out.println(tbl.getName());
            Assertions.assertEquals("StarRocks", tbl.getEngine());
            Assertions.assertEquals(6, tbl.getBaseSchema().size());
        } finally {
            db.readUnlock();
        }

        String alterTableLSCStmtStr =
                "alter table test.sc_dup_nlsc set ('light_schema_change' = 'true')";
        AlterTableStmt alterTableLSCStmt = (AlterTableStmt) parseAndAnalyzeStmt(alterTableLSCStmtStr);
        GlobalStateMgr.getCurrentState().getAlterInstance().processAlterTable(alterTableLSCStmt);
        Assertions.assertTrue(tbl.getUseLightSchemaChange());

        //process uniq add value column schema change
        String addValColStmtStr = "alter table test.sc_dup_nlsc add column new_v1 int default '0'";
        AlterTableStmt addValColStmt = (AlterTableStmt) parseAndAnalyzeStmt(addValColStmtStr);
        GlobalStateMgr.getCurrentState().getAlterInstance().processAlterTable(addValColStmt);
        jobSize++;
        //check alter job, do not create job
        Map<Long, AlterJobV2> alterJobs = GlobalStateMgr.getCurrentState().getSchemaChangeHandler().getAlterJobsV2();
        LOG.info("alterJobs:{}", alterJobs);
        Assertions.assertEquals(jobSize, alterJobs.size());

        db.readLock();
        try {
            Assertions.assertEquals(7, tbl.getBaseSchema().size());
            String baseIndexName = tbl.getIndexNameById(tbl.getBaseIndexId());
            Assertions.assertEquals(baseIndexName, tbl.getName());
            MaterializedIndexMeta indexMeta = tbl.getIndexMetaByIndexId(tbl.getBaseIndexId());
            Assertions.assertNotNull(indexMeta);
        } finally {
            db.readUnlock();
        }

        //process uniq drop val column schema change
        String dropValColStmtStr = "alter table test.sc_dup_nlsc drop column new_v1";
        AlterTableStmt dropValColStm = (AlterTableStmt) parseAndAnalyzeStmt(dropValColStmtStr);
        GlobalStateMgr.getCurrentState().getAlterInstance().processAlterTable(dropValColStm);
        jobSize++;
        //check alter job
        Assertions.assertEquals(jobSize, alterJobs.size());
        db.readLock();
        try {
            Assertions.assertEquals(6, tbl.getBaseSchema().size());
            String baseIndexName = tbl.getIndexNameById(tbl.getBaseIndexId());
            Assertions.assertEquals(baseIndexName, tbl.getName());
            MaterializedIndexMeta indexMeta = tbl.getIndexMetaByIndexId(tbl.getBaseIndexId());
            Assertions.assertNotNull(indexMeta);
        } finally {
            db.readUnlock();
        }
    }

    @Test
    public void testAddValueColumnOnAggMV(@Injectable OlapTable olapTable,
            @Injectable Column newColumn,
            @Injectable ColumnPosition columnPosition) {
        SchemaChangeHandler schemaChangeHandler = new SchemaChangeHandler();
        new Expectations() {
            {
                olapTable.getKeysType();
                result = KeysType.DUP_KEYS;
                newColumn.getAggregationType();
                result = null;
                olapTable.getIndexMetaByIndexId(2).getKeysType();
                result = KeysType.AGG_KEYS;
                newColumn.isKey();
                result = false;
            }
        };

        try {
            Deencapsulation.invoke(schemaChangeHandler, "addColumnInternal", olapTable, newColumn, columnPosition,
                new Long(2L), new Long(1L), Maps.newHashMap(), Sets.newHashSet(), false, Maps.newHashMap());
            Assertions.fail();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
