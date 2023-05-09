// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.Expr;
import com.starrocks.analysis.LimitElement;
import com.starrocks.analysis.OrderByElement;
import com.starrocks.common.proc.ColddownProcNode;
import com.starrocks.load.ColddownJob.JobState;
import com.starrocks.qe.ShowResultSetMetaData;

import java.util.List;

// SHOW COLDDOWN STATUS statement used to get status of load job.
//
// syntax:
//      SHOW COLDDOWN [FROM db] [WHERE name = "you_job_name"]
public class ShowColddownStmt extends ShowExportStmt {
    private String jobName = null;

    private JobState jobState;

    public ShowColddownStmt(String db, Expr whereExpr, List<OrderByElement> orderByElements,
                            LimitElement limitElement) {
        super(db, whereExpr, orderByElements, limitElement);
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public void setJobState(JobState jobState) {
        this.jobState = jobState;
    }

    public JobState getColddownJobState() {
        return jobState;
    }

    public String getJobName() {
        return jobName;
    }

    @Override
    protected String getCommandName() {
        return "COLDDOWN";
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return getMetaData(ColddownProcNode.TITLE_NAMES);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowColddownStatement(this, context);
    }
}
