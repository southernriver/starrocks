// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

/**
 * syntax:
 * CANCEL COLDDOWN "you_job_name"
 */
public class CancelColddownStmt extends DdlStmt {
    private String dbName;

    private String jobName;

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public void setJobName(String value) {
        this.jobName = value;
    }

    public String getDbName() {
        return dbName;
    }

    public String getJobName() {
        return jobName;
    }

    public CancelColddownStmt(String jobName) {
        this.jobName = jobName;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCancelColddownStatement(this, context);
    }
}

