// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/analysis/ShowTableStmtTest.java

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

package com.starrocks.analysis;

import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.AstToStringBuilder;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.ShowTableStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ShowTableStmtTest {

    private ConnectContext ctx;

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void testNormal() throws Exception {
        ctx = UtFrameUtils.createDefaultCtx();
        ctx.setDatabase("testDb");

        ShowTableStmt stmt = new ShowTableStmt("", false, null);

        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assert.assertEquals("testDb", stmt.getDb());
        Assert.assertFalse(stmt.isVerbose());
        Assert.assertEquals(1, stmt.getMetaData().getColumnCount());
        Assert.assertEquals("Tables_in_testDb", stmt.getMetaData().getColumn(0).getName());

        stmt = new ShowTableStmt("abc", true, null);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assert.assertEquals(2, stmt.getMetaData().getColumnCount());
        Assert.assertEquals("Tables_in_abc", stmt.getMetaData().getColumn(0).getName());
        Assert.assertEquals("Table_type", stmt.getMetaData().getColumn(1).getName());

        stmt = new ShowTableStmt("abc", true, "bcd");
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assert.assertEquals("bcd", stmt.getPattern());
        Assert.assertEquals(2, stmt.getMetaData().getColumnCount());
        Assert.assertEquals("Tables_in_abc", stmt.getMetaData().getColumn(0).getName());
        Assert.assertEquals("Table_type", stmt.getMetaData().getColumn(1).getName());
        Assert.assertEquals("bcd", stmt.getPattern());

        String sql = "show full tables where table_type !='VIEW'";
        stmt = (ShowTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        QueryStatement queryStatement = stmt.toSelectStmt();
        String expect = "SELECT information_schema.tables.TABLE_NAME AS Tables_in_testDb, " +
                "information_schema.tables.TABLE_TYPE AS Table_type FROM " +
                "information_schema.tables WHERE (information_schema.tables.TABLE_SCHEMA = 'testDb') AND (information_schema.tables.TABLE_TYPE != 'VIEW')";
        Assert.assertEquals(expect, AstToStringBuilder.toString(queryStatement));
    }

    @Test
    public void testQueryTableByProperties() throws Exception {
        ctx = UtFrameUtils.createDefaultCtx();
        ctx.setDatabase("testDb");

        String sql = "show full tables where `table_properties.light_schema_change` = 'true'";
        ShowTableStmt stmt = (ShowTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        QueryStatement queryStatement = stmt.toSelectStmt();
        String expect =
                "SELECT tables_table.Tables_in_testDb AS Tables_in_testDb, tables_table.Table_type AS Table_type FROM"
                        + " (SELECT information_schema.tables.TABLE_NAME AS Tables_in_testDb,"
                        + " information_schema.tables.TABLE_TYPE AS Table_type, information_schema.tables.TABLE_SCHEMA"
                        + " FROM information_schema.tables WHERE information_schema.tables.TABLE_SCHEMA = 'testDb') tables_table"
                        + " INNER JOIN (SELECT information_schema.tables_config.TABLE_SCHEMA,"
                        + " information_schema.tables_config.TABLE_NAME FROM information_schema.tables_config"
                        + " WHERE (json_query(information_schema.tables_config.properties, '$.light_schema_change')) = 'true')"
                        + " config_table ON (tables_table.TABLE_SCHEMA = config_table.TABLE_SCHEMA)"
                        + " AND (tables_table.Tables_in_testDb = config_table.TABLE_NAME)";
        Assert.assertEquals(expect, AstToStringBuilder.toString(queryStatement));

        sql = "show full tables where `table_properties.light_schema_change` = 'true'"
                + " and `table_properties.replication_num` = '1'";
        stmt = (ShowTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        queryStatement = stmt.toSelectStmt();
        expect = "SELECT tables_table.Tables_in_testDb AS Tables_in_testDb, tables_table.Table_type AS Table_type"
                + " FROM (SELECT information_schema.tables.TABLE_NAME AS Tables_in_testDb,"
                + " information_schema.tables.TABLE_TYPE AS Table_type,"
                + " information_schema.tables.TABLE_SCHEMA FROM information_schema.tables"
                + " WHERE information_schema.tables.TABLE_SCHEMA = 'testDb') tables_table"
                + " INNER JOIN (SELECT information_schema.tables_config.TABLE_SCHEMA, information_schema.tables_config.TABLE_NAME"
                + " FROM information_schema.tables_config"
                + " WHERE ((json_query(information_schema.tables_config.properties, '$.light_schema_change')) = 'true')"
                + " AND ((json_query(information_schema.tables_config.properties, '$.replication_num')) = '1')) config_table"
                + " ON (tables_table.TABLE_SCHEMA = config_table.TABLE_SCHEMA)"
                + " AND (tables_table.Tables_in_testDb = config_table.TABLE_NAME)";
        Assert.assertEquals(expect, AstToStringBuilder.toString(queryStatement));

        // OR query can not be transformed;
        sql = "show full tables where `table_properties.light_schema_change` = 'true'"
                + " or `table_properties.replication_num` = '1'";
        stmt = (ShowTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        queryStatement = stmt.toSelectStmt();
        expect =
                "SELECT information_schema.tables.TABLE_NAME AS Tables_in_testDb,"
                        + " information_schema.tables.TABLE_TYPE AS Table_type"
                        + " FROM information_schema.tables WHERE (information_schema.tables.TABLE_SCHEMA = 'testDb')"
                        + " AND ((table_properties.light_schema_change = 'true') OR (table_properties.replication_num = '1'))";
        Assert.assertEquals(expect, AstToStringBuilder.toString(queryStatement));
    }

    @Test(expected = SemanticException.class)
    public void testNoDb() throws Exception {
        ctx = UtFrameUtils.createDefaultCtx();
        ShowTableStmt stmt = new ShowTableStmt("", false, null);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assert.fail("No exception throws");
    }
}