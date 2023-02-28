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

package com.starrocks.load.routineload;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.LabelName;
import com.starrocks.analysis.ParseNode;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.LoadException;
import com.starrocks.common.UserException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.load.RoutineLoadDesc;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.ColumnSeparator;
import com.starrocks.sql.ast.CreateRoutineLoadStmt;
import com.starrocks.sql.ast.PartitionNames;
import com.starrocks.thrift.TResourceInfo;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TubeRoutineLoadJobTest {
    private static final Logger LOG = LogManager.getLogger(TubeRoutineLoadJobTest.class);

    private String jobName = "job1";
    private String dbName = "db1";
    private LabelName labelName = new LabelName(dbName, jobName);
    private String tableNameString = "table1";
    private String topicName = "topic1";
    private String masterAddr = "http://127.0.0.1:8080";
    private String groupName = "test_tube";
    private PartitionNames partitionNames;

    private ColumnSeparator columnSeparator = new ColumnSeparator(",");

    @Mocked
    ConnectContext connectContext;
    @Mocked
    TResourceInfo tResourceInfo;

    @Before
    public void init() {
        List<String> partitionNameList = Lists.newArrayList();
        partitionNameList.add("p1");
        partitionNames = new PartitionNames(false, partitionNameList);
    }

    @Test
    public void testFromCreateStmtWithErrorTable(@Mocked Catalog catalog,
                                                 @Injectable Database database) throws LoadException {
        CreateRoutineLoadStmt createRoutineLoadStmt = initCreateRoutineLoadStmt();
        RoutineLoadDesc routineLoadDesc = new RoutineLoadDesc(columnSeparator, null, null, null, partitionNames);
        Deencapsulation.setField(createRoutineLoadStmt, "routineLoadDesc", routineLoadDesc);

        new Expectations() {
            {
                database.getTable(tableNameString);
                minTimes = 0;
                result = null;
            }
        };

        try {
            TubeRoutineLoadJob tubeRoutineLoadJob = TubeRoutineLoadJob.fromCreateStmt(createRoutineLoadStmt);
            Assert.fail();
        } catch (UserException e) {
            LOG.info(e.getMessage());
        }
    }

    @Test
    public void testFromCreateStmt(@Mocked Catalog catalog,
                                   @Injectable Database database,
                                   @Injectable OlapTable table) throws UserException {
        CreateRoutineLoadStmt createRoutineLoadStmt = initCreateRoutineLoadStmt();
        RoutineLoadDesc routineLoadDesc = new RoutineLoadDesc(columnSeparator, null, null, null, null);

        Deencapsulation.setField(createRoutineLoadStmt, "routineLoadDesc", routineLoadDesc);
        Deencapsulation.setField(createRoutineLoadStmt, "tubeMasterAddr", masterAddr);
        Deencapsulation.setField(createRoutineLoadStmt, "tubeTopic", topicName);
        Deencapsulation.setField(createRoutineLoadStmt, "tubeGroupName", groupName);
        long dbId = 1L;
        long tableId = 2L;

        new Expectations() {
            {
                database.getTable(tableNameString);
                minTimes = 0;
                result = table;
                database.getId();
                minTimes = 0;
                result = dbId;
                table.getId();
                minTimes = 0;
                result = tableId;
                table.getType();
                minTimes = 0;
                result = Table.TableType.OLAP;
            }
        };

        TubeRoutineLoadJob tubeRoutineLoadJob = TubeRoutineLoadJob.fromCreateStmt(createRoutineLoadStmt);
        Assert.assertEquals(jobName, tubeRoutineLoadJob.getName());
        Assert.assertEquals(dbId, tubeRoutineLoadJob.getDbId());
        Assert.assertEquals(tableId, tubeRoutineLoadJob.getTableId());
        Assert.assertEquals(masterAddr, Deencapsulation.getField(tubeRoutineLoadJob, "masterAddr"));
        Assert.assertEquals(topicName, Deencapsulation.getField(tubeRoutineLoadJob, "topic"));
        Assert.assertEquals(groupName, Deencapsulation.getField(tubeRoutineLoadJob, "groupName"));
    }

    private CreateRoutineLoadStmt initCreateRoutineLoadStmt() {
        List<ParseNode> loadPropertyList = new ArrayList<>();
        loadPropertyList.add(columnSeparator);
        Map<String, String> properties = Maps.newHashMap();
        properties.put(CreateRoutineLoadStmt.DESIRED_CONCURRENT_NUMBER_PROPERTY, "2");
        String typeName = LoadDataSourceType.TUBE.name();
        Map<String, String> customProperties = Maps.newHashMap();

        customProperties.put(CreateRoutineLoadStmt.TUBE_MASTER_ADDR_PROPERTY, masterAddr);
        customProperties.put(CreateRoutineLoadStmt.TUBE_TOPIC_PROPERTY, topicName);
        customProperties.put(CreateRoutineLoadStmt.TUBE_GROUP_NAME_PROPERTY, groupName);

        CreateRoutineLoadStmt createRoutineLoadStmt = new CreateRoutineLoadStmt(labelName, tableNameString,
                loadPropertyList, properties,
                typeName, customProperties);
        Deencapsulation.setField(createRoutineLoadStmt, "name", jobName);
        return createRoutineLoadStmt;
    }
}
