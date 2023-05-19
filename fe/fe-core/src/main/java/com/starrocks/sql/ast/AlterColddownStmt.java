// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.common.util.PrintableMap;

import java.util.Map;

// Alter Colddown statement
//
// syntax:
//      ALTER COLDDOWN JOB name
//          PROPERTIES("key"="value")
public class AlterColddownStmt extends DdlStmt {

    private String dbName;
    private final String jobName;
    private final Map<String, String> properties;

    public AlterColddownStmt(String jobName, Map<String, String> properties) {
        this.jobName = jobName;
        this.properties = properties;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getJobName() {
        return jobName;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER COLDDOWN JOB ");
        sb.append(jobName);

        sb.append("\nPROPERTIES (");
        sb.append(new PrintableMap<>(getProperties(), "=", true, false));
        sb.append(")");

        return sb.toString();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAlterColddownStatement(this, context);
    }
}