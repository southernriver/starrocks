// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.analyzer;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.BrokerDesc;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.OrderByElement;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.FsBroker;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.proc.ExportProcNode;
import com.starrocks.common.util.OrderByPair;
import com.starrocks.fs.hdfs.HdfsFsManager;
import com.starrocks.load.ExportJob;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.CancelExportStmt;
import com.starrocks.sql.ast.ExportStmt;
import com.starrocks.sql.ast.PartitionNames;
import com.starrocks.sql.ast.ShowExportStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.utils.TdwUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;


// [CANCEL | SHOW] Export Statement Analyzer
// ExportStmtAnalyzer
public class ExportStmtAnalyzer {

    public static void analyze(StatementBase stmt, ConnectContext session) {
        new ExportStmtAnalyzer.ExportAnalyzerVisitor().visit(stmt, session);
    }


    public static class ExportAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {
        private static final Set<String> ACCEPTABLE_WHERE_KEYS = ImmutableSet.of("id", "state", "queryid", "tablename");

        public void analyze(StatementBase statement, ConnectContext session) {
            visit(statement, session);
        }

        @Override
        public Void visitExportStatement(ExportStmt statement, ConnectContext context) {
            GlobalStateMgr mgr = context.getGlobalStateMgr();
            TableName tableName = statement.getTableRef().getName();
            // make sure catalog, db, table
            MetaUtils.normalizationTableName(context, tableName);
            analyzeDbName(tableName.getDb(), context);
            Table table = MetaUtils.getTable(context, tableName);
            statement.setTblName(tableName);

            return visitExportStatement(statement, mgr, table);
        }

        public static Void visitExportStatement(ExportStmt statement, GlobalStateMgr mgr, Table table) {
            TableName tableName = statement.getTableRef().getName();
            if (table.getType() == Table.TableType.OLAP &&
                    (((OlapTable) table).getState() == OlapTable.OlapTableState.RESTORE ||
                            ((OlapTable) table).getState() == OlapTable.OlapTableState.RESTORE_WITH_LOAD)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_TABLE_STATE, "RESTORING");
            }
            statement.setTblName(tableName);
            PartitionNames partitionNames = statement.getTableRef().getPartitionNames();
            if (partitionNames != null) {
                if (partitionNames.isTemp()) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                            "Do not support exporting temporary partitions");
                }
                statement.setPartitions(partitionNames.getPartitionNames());
            }

            // check db, table && partitions && columns whether exist
            // check path is valid
            // generate file name prefix
            statement.checkTable(mgr);

            // check broker whether exist
            BrokerDesc brokerDesc = statement.getBrokerDesc();
            if (brokerDesc == null) {
                throw new SemanticException("broker is not provided");
            }

            if (brokerDesc.hasBroker()) {
                if (!mgr.getBrokerMgr().containsBroker(brokerDesc.getName())) {
                    throw new SemanticException("broker " + brokerDesc.getName() + " does not exist");
                }

                FsBroker broker = mgr.getBrokerMgr().getAnyBroker(brokerDesc.getName());
                if (broker == null) {
                    throw new SemanticException("failed to get alive broker");
                }
            } else {
                String user = Config.is_tdw_hive ? TdwUtil.getCurrentTdwUserName() :
                        (ConnectContext.get() == null ? null : ConnectContext.get().getQualifiedUser());
                String userInBroker = brokerDesc.getProperties().get(HdfsFsManager.USER_NAME_KEY);
                if (user != null) {
                    if (Strings.isNullOrEmpty(userInBroker)) {
                        brokerDesc.getProperties().put(HdfsFsManager.USER_NAME_KEY, user);
                    } else if (!userInBroker.equals(user) && !"root".equals(ConnectContext.get().getQualifiedUser())) {
                        throw new SemanticException("username in broker should be equal to current logged in user: " +
                                ConnectContext.get().getQualifiedUser());
                    }
                }
            }

            statement.checkType(table);

            // check properties
            try {
                statement.checkProperties(statement.getProperties());
            } catch (AnalysisException e) {
                throw new SemanticException(e.getMessage());
            }
            return null;
        }

        @Override
        public Void visitCancelExportStatement(CancelExportStmt statement, ConnectContext context) {
            // analyze dbName
            statement.setDbName(analyzeDbName(statement.getDbName(), context));
            SemanticException exception = new SemanticException(
                    "Where clause should look like: queryid = \"your_query_id\"");
            Expr whereClause = statement.getWhereClause();
            if (whereClause == null) {
                throw exception;
            }
            // analyze where
            checkPredicateType(whereClause, exception);

            // left child
            if (!(whereClause.getChild(0) instanceof SlotRef)) {
                throw exception;
            }
            if (!((SlotRef) whereClause.getChild(0)).getColumnName().equalsIgnoreCase("queryid")) {
                throw exception;
            }
            // right child with query id
            statement.setQueryId(analyzeQueryID(whereClause, exception));
            return null;
        }

        protected Set<String> getAcceptableWhereKeys() {
            return ACCEPTABLE_WHERE_KEYS;
        }

        protected Void visitShowExportStatement(ShowExportStmt statement, ConnectContext context,
                                             SemanticException exception) {
            // analyze dbName
            statement.setDbName(analyzeDbName(statement.getDbName(), context));
            // analyze where clause if not null
            Expr whereExpr = statement.getWhereClause();
            if (whereExpr != null) {
                boolean hasJobId = false;
                boolean hasState = false;
                boolean hasQueryId = false;
                boolean hasTableName = false;
                checkPredicateType(whereExpr, exception);

                // left child
                if (!(whereExpr.getChild(0) instanceof SlotRef)) {
                    throw exception;
                }
                String leftKey = ((SlotRef) whereExpr.getChild(0)).getColumnName();
                if (!getAcceptableWhereKeys().contains(leftKey.toLowerCase())) {
                    throw exception;
                }
                if (leftKey.equalsIgnoreCase("id")) {
                    hasJobId = true;
                } else if (leftKey.equalsIgnoreCase("state")) {
                    hasState = true;
                } else if (leftKey.equalsIgnoreCase("queryid")) {
                    hasQueryId = true;
                } else if (leftKey.equalsIgnoreCase("tableName")) {
                    hasTableName = true;
                }

                // right child
                if (hasState) {
                    if (!(whereExpr.getChild(1) instanceof StringLiteral)) {
                        throw exception;
                    }

                    String value = ((StringLiteral) whereExpr.getChild(1)).getStringValue();
                    if (Strings.isNullOrEmpty(value)) {
                        throw exception;
                    }

                    statement.setStateValue(value.toUpperCase());

                    try {
                        setJobState(statement, value.toUpperCase());
                    } catch (IllegalArgumentException e) {
                        throw exception;
                    }
                } else if (hasJobId) {
                    if (!(whereExpr.getChild(1) instanceof IntLiteral)) {
                        throw exception;
                    }
                    statement.setJobId(((IntLiteral) whereExpr.getChild(1)).getLongValue());
                } else if (hasQueryId) {
                    statement.setQueryId(analyzeQueryID(whereExpr, exception));
                } else if (hasTableName) {
                    if (!(whereExpr.getChild(1) instanceof StringLiteral)) {
                        throw exception;
                    }
                    String value = ((StringLiteral) whereExpr.getChild(1)).getStringValue();
                    if (Strings.isNullOrEmpty(value)) {
                        throw exception;
                    }
                    statement.setTableName(value);
                }
            }

            // order by
            List<OrderByElement> orderByElements = statement.getOrderByElements();
            if (orderByElements != null && !orderByElements.isEmpty()) {
                ArrayList<OrderByPair> orderByPairs = new ArrayList<>();
                for (OrderByElement orderByElement : orderByElements) {
                    if (!(orderByElement.getExpr() instanceof SlotRef)) {
                        ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, "Should order by column");
                    }
                    SlotRef slotRef = (SlotRef) orderByElement.getExpr();
                    int index = 0;
                    try {
                        index = ExportProcNode.analyzeColumn(slotRef.getColumnName());
                    } catch (AnalysisException e) {
                        ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, e.getMessage());
                    }
                    OrderByPair orderByPair = new OrderByPair(index, !orderByElement.getIsAsc());
                    orderByPairs.add(orderByPair);
                }
                statement.setOrderByPairs(orderByPairs);
            }
            return null;
        }

        @Override
        public Void visitShowExportStatement(ShowExportStmt statement, ConnectContext context) {
            SemanticException exception = new SemanticException(
                    "Where clause should look like : queryid = \"your_query_id\" " +
                            "or STATE = \"PENDING|EXPORTING|FINISHED|CANCELLED\" or tableName=\"your_table\"");
            return visitShowExportStatement(statement, context, exception);
        }

        protected void setJobState(ShowExportStmt statement, String stateValue) {
            statement.setJobState(ExportJob.JobState.valueOf(stateValue.toUpperCase()));
        }

        private UUID analyzeQueryID(Expr whereExpr, SemanticException exception) {
            if (!(whereExpr.getChild(1) instanceof StringLiteral)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, exception.getMessage());
            }
            UUID uuid = null;
            String value = ((StringLiteral) whereExpr.getChild(1)).getStringValue();
            try {
                uuid = UUID.fromString(value);
            } catch (IllegalArgumentException e) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                        "Invalid UUID string as queryid: " + value);
            }
            return uuid;
        }

        protected String analyzeDbName(String dbName, ConnectContext context) {
            if (Strings.isNullOrEmpty(dbName)) {
                dbName = context.getDatabase();
                if (Strings.isNullOrEmpty(dbName)) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_NO_DB_ERROR);
                }
            }
            return dbName;
        }

        protected void checkPredicateType(Expr whereExpr, SemanticException exception) throws SemanticException {
            // check predicate type
            if (whereExpr instanceof BinaryPredicate) {
                BinaryPredicate binaryPredicate = (BinaryPredicate) whereExpr;
                if (binaryPredicate.getOp() != BinaryPredicate.Operator.EQ) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, exception.getMessage());
                }
            } else {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, exception.getMessage());
            }
        }
    }
}