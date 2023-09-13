// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.LabelName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.common.UserException;
import com.starrocks.qe.ShowResultSetMetaData;

// Show create database statement
//  Syntax:
//      SHOW CREATE ROUTINE LOAD name
public class ShowCreateRoutineLoadStmt extends ShowStmt {
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Name", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Create Routine", ScalarType.createVarchar(30)))
                    .build();

    private final LabelName labelName;

    public ShowCreateRoutineLoadStmt(LabelName labelName) {
        this.labelName = labelName;
    }

    public String getDbFullName() {
        return labelName.getDbName();
    }

    public void setDbFullName(String dbFullName) {
        labelName.setDbName(dbFullName);
    }

    public String getJobName() {
        return labelName.getLabelName();
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
    }

    @Override
    public String toSql() {
        return super.toSql();
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowCreateRoutineLoadStatement(this, context);
    }
}
