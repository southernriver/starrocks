// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.RedirectStatus;

//
// syntax:
//      MANUAL COLDDOWN PARTITION your_partition WITH JOB name
public class ManualColddownStmt extends StatementBase {

    private String dbName;
    private final String partition;
    private final String jobName;

    public ManualColddownStmt(String partition, String jobName) {
        super();
        this.partition = partition;
        this.jobName = jobName;
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

    @Override
    public String toSql() {
        return "MANUAL COLDDOWN PARTITION " + partition + " WITH JOB " + jobName;
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