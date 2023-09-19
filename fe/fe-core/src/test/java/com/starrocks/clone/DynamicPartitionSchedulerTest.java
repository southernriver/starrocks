// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.clone;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DynamicPartitionProperty;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.DynamicPartitionUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.DropPartitionClause;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;

import static com.starrocks.common.util.DynamicPartitionUtilTest.getZonedDateTimeFromStr;

public class DynamicPartitionSchedulerTest {

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        FeConstants.default_scheduler_interval_millisecond = 100;
        Config.dynamic_partition_enable = true;
        Config.dynamic_partition_check_interval_seconds = 1;
        Config.enable_strict_storage_medium_check = false;
        UtFrameUtils.createMinStarRocksCluster();
        UtFrameUtils.addMockBackend(10002);
        UtFrameUtils.addMockBackend(10003);
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
    }

    @Test
    public void testPartitionTTLProperties() throws Exception {
        starRocksAssert.withDatabase("test").useDatabase("test")
                .withTable("CREATE TABLE test.tbl1\n" +
                        "(\n" +
                        "    k1 date,\n" +
                        "    v1 int \n" +
                        ")\n" +
                        "PARTITION BY RANGE(k1)\n" +
                        "(\n" +
                        "    PARTITION p1 values less than('2020-02-01'),\n" +
                        "    PARTITION p2 values less than('2020-03-01'),\n" +
                        "    PARTITION p3 values less than('2020-04-01'),\n" +
                        "    PARTITION p4 values less than('2020-05-01')\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH (k1) BUCKETS 3\n" +
                        "PROPERTIES" +
                        "(" +
                        "    'replication_num' = '1'\n" +
                        ");");

        DynamicPartitionScheduler dynamicPartitionScheduler = GlobalStateMgr.getCurrentState()
                .getDynamicPartitionScheduler();
        Database db = GlobalStateMgr.getCurrentState().getDb("test");
        OlapTable tbl = (OlapTable) db.getTable("tbl1");
        // Now the table does not actually support partition ttl,
        // so in order to simplify the test, it is directly set like this
        tbl.getTableProperty().getProperties().put("partition_ttl_number", "3");
        tbl.getTableProperty().setPartitionTTLNumber(3);

        dynamicPartitionScheduler.registerTtlPartitionTable(db.getId(), tbl.getId());
        dynamicPartitionScheduler.runAfterCatalogReady();


        Assert.assertEquals(3, tbl.getPartitions().size());
    }

    @Test
    public void testPartitionTTLPropertiesZero() throws Exception {
        starRocksAssert.withDatabase("test").useDatabase("test")
                .withTable("CREATE TABLE test.base\n" +
                        "(\n" +
                        "    k1 date,\n" +
                        "    k2 int,\n" +
                        "    v1 int sum\n" +
                        ")\n" +
                        "PARTITION BY RANGE(k1)\n" +
                        "(\n" +
                        "    PARTITION p1 values less than('2020-02-01'),\n" +
                        "    PARTITION p2 values less than('2020-03-01')\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');");
        String sql = "create materialized view mv1 " +
                "partition by k1 " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"partition_ttl_number\" = \"0\"\n" +
                ") " +
                "as select k1, k2 from test.base;";
        Config.enable_experimental_mv = true;
        CreateMaterializedViewStatement createMaterializedViewStatement =
                (CreateMaterializedViewStatement) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        try {
            GlobalStateMgr.getCurrentState().createMaterializedView(createMaterializedViewStatement);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertTrue(ex.getMessage().contains("Illegal Partition TTL Number"));
        }
    }

    @Test
    public void testGetDropPartitionClause() throws Exception {
        starRocksAssert.withDatabase("test").useDatabase("test")
                .withTable("CREATE TABLE test.tbl1\n" +
                "(\n" +
                "    k1 date,\n" +
                "    v1 int \n" +
                ")\n" +
                "PARTITION BY RANGE(k1)\n" +
                "(\n" +
                "    PARTITION p20230913 VALUES [(\"2023-09-13\"), (\"2023-09-14\")),\n" +
                "    PARTITION p20230914 VALUES [(\"2023-09-14\"), (\"2023-09-15\")),\n" +
                "    PARTITION p20230915 VALUES [(\"2023-09-15\"), (\"2023-09-16\")),\n" +
                "    PARTITION p20230916 VALUES [(\"2023-09-16\"), (\"2023-09-17\")),\n" +
                "    PARTITION p20230917 VALUES [(\"2023-09-17\"), (\"2023-09-18\")),\n" +
                "    PARTITION p20230918 VALUES [(\"2023-09-18\"), (\"2023-09-19\")),\n" +
                "    PARTITION p20230919 VALUES [(\"2023-09-19\"), (\"2023-09-20\")),\n" +
                "    PARTITION p20230920 VALUES [(\"2023-09-20\"), (\"2023-09-21\"))\n" +
                ")\n" +
                "DISTRIBUTED BY HASH (k1) BUCKETS 3\n" +
                "PROPERTIES" +
                "(" +
                "   'replication_num' = '1',\n" +
                "   'dynamic_partition.enable' = 'true',\n" +
                "   'dynamic_partition.time_unit' = 'DAY',\n" +
                "   'dynamic_partition.time_zone' = 'Asia/Shanghai',\n" +
                "   'dynamic_partition.start' = '-3',\n" +
                "   'dynamic_partition.end' = '1',\n" +
                "   'dynamic_partition.prefix' = 'p',\n" +
                "   'dynamic_partition.buckets' = '1',\n" +
                "   'dynamic_partition.history_partition_num' = '0'\n" +
                ");");

        Database db = GlobalStateMgr.getCurrentState().getDb("test");
        OlapTable olapTable = (OlapTable) db.getTable("tbl1");
        DynamicPartitionProperty dynamicPartitionProperty =
                olapTable.getTableProperty().getDynamicPartitionProperty();
        RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) olapTable.getPartitionInfo();
        Column partitionColumn = rangePartitionInfo.getPartitionColumns().get(0);
        String partitionFormat = DynamicPartitionUtil.getPartitionFormat(partitionColumn,
                dynamicPartitionProperty.getTimeUnit());
        int lowerBoundOffset = dynamicPartitionProperty.getStart();
        int upperBoundOffset = dynamicPartitionProperty.getEnd();
        ArrayList<DropPartitionClause> dropPartitionClauses;
        dropPartitionClauses = DynamicPartitionScheduler.getDropPartitionClause(db, olapTable,
                getZonedDateTimeFromStr("2023-09-18"), partitionColumn, partitionFormat, lowerBoundOffset, upperBoundOffset);
        printValue(dropPartitionClauses);
        Assert.assertEquals(3, dropPartitionClauses.size());
        dropPartitionClauses = DynamicPartitionScheduler.getDropPartitionClause(db, olapTable,
                getZonedDateTimeFromStr("2023-09-17"), partitionColumn, partitionFormat, lowerBoundOffset, upperBoundOffset);
        printValue(dropPartitionClauses);
        Assert.assertEquals(3, dropPartitionClauses.size());

        dropPartitionClauses = DynamicPartitionScheduler.getDropPartitionClause(db, olapTable,
                getZonedDateTimeFromStr("2023-09-13"), partitionColumn, partitionFormat, lowerBoundOffset, upperBoundOffset);
        printValue(dropPartitionClauses);
        Assert.assertEquals(6, dropPartitionClauses.size());

        dropPartitionClauses = DynamicPartitionScheduler.getDropPartitionClause(db, olapTable,
                getZonedDateTimeFromStr("2023-09-14"), partitionColumn, partitionFormat, lowerBoundOffset, upperBoundOffset);
        printValue(dropPartitionClauses);
        Assert.assertEquals(5, dropPartitionClauses.size());

        dropPartitionClauses = DynamicPartitionScheduler.getDropPartitionClause(db, olapTable,
                getZonedDateTimeFromStr("2023-09-20"), partitionColumn, partitionFormat, lowerBoundOffset, upperBoundOffset);
        printValue(dropPartitionClauses);
        Assert.assertEquals(4, dropPartitionClauses.size());
        dropPartitionClauses = DynamicPartitionScheduler.getDropPartitionClause(db, olapTable,
                getZonedDateTimeFromStr("2023-09-21"), partitionColumn, partitionFormat, lowerBoundOffset, upperBoundOffset);
        printValue(dropPartitionClauses);
        Assert.assertEquals(5, dropPartitionClauses.size());
    }

    private static void printValue(ArrayList<DropPartitionClause> values) {
        for (DropPartitionClause value : values) {
            System.out.println(value.getPartitionName());
        }
    }
}
