// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.analyzer;

import com.starrocks.analysis.PrepareStatement;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;

public class PreparedStmtAnalyzer {
    public static void analyze(StatementBase statement, ConnectContext context) {
        new PreparedAnalyzerVisitor().visit(statement, context);
    }

    static class PreparedAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {

        @Override
        public Void visitPreparedStatement(PrepareStatement statement, ConnectContext context) {
            StatementBase innerStmt = statement.getInnerStmt();
            if (!(innerStmt instanceof QueryStatement)) {
                throw new SemanticException("Prepared statement only support in DQL");
            }

            // Analyzer inner
            Analyzer.analyze(innerStmt, context);



            return null;
        }
    }
}
