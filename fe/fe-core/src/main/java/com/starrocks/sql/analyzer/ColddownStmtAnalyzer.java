// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.analyzer;

import com.starrocks.analysis.Expr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.load.ColddownJob;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AlterColddownStmt;
import com.starrocks.sql.ast.CancelColddownStmt;
import com.starrocks.sql.ast.CreateColddownStmt;
import com.starrocks.sql.ast.ManualColddownStmt;
import com.starrocks.sql.ast.ShowColddownStmt;
import com.starrocks.sql.ast.ShowExportStmt;
import com.starrocks.sql.ast.StatementBase;

import java.util.HashSet;
import java.util.Set;

// [CANCEL | SHOW] Colddown Statement Analyzer
// ColddownStmtAnalyzer
public class ColddownStmtAnalyzer {

    public static void analyze(StatementBase stmt, ConnectContext session) {
        new ColddownAnalyzerVisitor().visit(stmt, session);
    }

    static class ColddownAnalyzerVisitor extends ExportStmtAnalyzer.ExportAnalyzerVisitor {

        @Override
        public Void visitCreateColddownStatement(CreateColddownStmt statement, ConnectContext context) {
            return visitExportStatement(statement, context);
        }

        @Override
        public Void visitCancelColddownStatement(CancelColddownStmt statement, ConnectContext context) {
            // analyze dbName
            statement.setDbName(analyzeDbName(statement.getDbName(), context));
            return null;
        }

        @Override
        public Void visitShowColddownStatement(ShowColddownStmt statement, ConnectContext context) {
            SemanticException exception = new SemanticException(
                    "Where clause should look like : jobName = \"your_job_name\"" +
                            " or STATE = \"RUNNING|CANCELLED\"" +
                            " or tableName=\"your_table\"");
            // analyze where clause if not null
            Expr whereExpr = statement.getWhereClause();
            if (whereExpr != null) {
                checkPredicateType(whereExpr, exception);

                // left child
                if (!(whereExpr.getChild(0) instanceof SlotRef)) {
                    throw exception;
                }
                String leftKey = ((SlotRef) whereExpr.getChild(0)).getColumnName();
                if (leftKey.equalsIgnoreCase("jobName")) {
                    statement.setJobName(analyzeJobName(whereExpr, exception));
                }
            }
            return visitShowExportStatement(statement, context, exception);
        }

        @Override
        protected Set<String> getAcceptableWhereKeys() {
            Set<String> newSet = new HashSet<>(super.getAcceptableWhereKeys());
            newSet.add("jobname");
            return newSet;
        }

        @Override
        protected void setJobState(ShowExportStmt statement, String stateValue) {
            ((ShowColddownStmt) statement).setJobState(ColddownJob.JobState.valueOf(stateValue.toUpperCase()));
        }

        private String analyzeJobName(Expr whereExpr, SemanticException exception) {
            if (!(whereExpr.getChild(1) instanceof StringLiteral)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, exception.getMessage());
            }
            return ((StringLiteral) whereExpr.getChild(1)).getStringValue();
        }

        @Override
        public Void visitManualColddownStatement(ManualColddownStmt statement, ConnectContext context) {
            // analyze dbName
            statement.setDbName(analyzeDbName(statement.getDbName(), context));
            return null;
        }

        @Override
        public Void visitAlterColddownStatement(AlterColddownStmt statement, ConnectContext context) {
            // analyze dbName
            statement.setDbName(analyzeDbName(statement.getDbName(), context));
            return null;
        }
    }
}