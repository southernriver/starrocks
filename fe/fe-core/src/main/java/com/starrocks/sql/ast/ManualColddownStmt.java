// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.google.gson.Gson;
import com.starrocks.analysis.RedirectStatus;

import java.util.Map;

//
// syntax:
//      MANUAL COLDDOWN PARTITION your_partition WITH JOB name
public class ManualColddownStmt extends StatementBase {

    private String dbName;
    private final String partition;
    private final String jobName;
    private final Map<String, String> properties;

    public ManualColddownStmt(String partition, String jobName, Map<String, String> properties) {
        super();
        this.partition = partition;
        this.jobName = jobName;
        this.properties = properties;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getPartition() {
        return partition;
    }

    public String getJobName() {
        return jobName;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public String toSql() {
        String sql = "MANUAL COLDDOWN PARTITION " + partition + " WITH JOB " + jobName;
        if (!properties.isEmpty()) {
            return sql + " PROPERTIES (" + new Gson().toJson(properties) + ")";
        }
        return sql;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitManualColddownStatement(this, context);
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_WITH_SYNC;
    }

    @Override
    public String toString() {
        return toSql();
    }
}