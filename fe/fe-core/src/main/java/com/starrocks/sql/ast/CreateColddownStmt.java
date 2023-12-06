// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.BrokerDesc;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.TableRef;
import com.starrocks.analysis.TimestampArithmeticExpr;
import com.starrocks.catalog.Table;
import com.starrocks.common.util.PrintableMap;
import com.starrocks.load.export.ExternalTableExportConfig;
import com.starrocks.sql.analyzer.SemanticException;

import java.util.List;
import java.util.Map;

// Colddown statement, colddown data to dirs by broker.
//
// syntax:
//      CREATE COLDDOWN JOB name on tablename
//          [(col1, col2[, ...])]
//          where col1='x'
//          TO 'colddown_target_path'|EXTERNAL_TABLE
//          [PROPERTIES("key"="value")]
//          WITH BROKER 'broker_name' [( $broker_attrs)]
public class CreateColddownStmt extends ExportStmt {

    private final String jobName;

    public CreateColddownStmt(String jobName, TableRef tableRef, List<String> columnNames, Expr whereExpr,
                              String typeName, Map<String, String> targetProperties, Map<String, String> properties,
                              BrokerDesc brokerDesc) {
        super(tableRef, columnNames, whereExpr, typeName, targetProperties, properties, brokerDesc);
        this.jobName = jobName;
        this.includeQueryId = false;
    }

    public String getJobName() {
        return jobName;
    }

    @Override
    protected void checkExternalTable(ExternalTableExportConfig externalTableExportConfig, Table table) {
        if (table.getType() == Table.TableType.MATERIALIZED_VIEW) {
            String timeUnitValue = getProperties().get(ExternalTableExportConfig.PARTITION_TIME_UNIT);
            if (timeUnitValue == null) {
                throw new SemanticException(ExternalTableExportConfig.PARTITION_TIME_UNIT +
                        " in job should be specified when colddown MATERIALIZED VIEW");
            }
            // verify timeUnitValue is a valid string
            TimestampArithmeticExpr.TimeUnit timeUnit = TimestampArithmeticExpr.TimeUnit.valueOf(timeUnitValue.toUpperCase());
            // partitions in mv are generated automatically, so the prefix can be set by user
            getProperties().put(ExternalTableExportConfig.PARTITION_PREFIX, "p");
        }
        Table externalTable = externalTableExportConfig.verifyExternalTable();
        if (externalTable != null) {
            return;
        }
        externalTableExportConfig.createExternalTable(table);
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE COLDDOWN JOB ");
        sb.append(jobName);
        sb.append(" on ");
        sb.append(getTblName().toSql());
        sb.append("\n");
        if (getWhereExpr() != null) {
            sb.append("Where (");
            sb.append(getWhereExpr().toSql());
            sb.append(")");
        }
        sb.append("\n");

        sb.append(" TO ");
        sb.append(getTypeName());

        if (getTargetProperties() != null && !getTargetProperties().isEmpty()) {
            sb.append("\n(");
            sb.append(new PrintableMap<>(getTargetProperties(), "=", true, false));
            sb.append(")");
        }

        if (getProperties() != null && !getProperties().isEmpty()) {
            sb.append("\nPROPERTIES (");
            sb.append(new PrintableMap<>(getProperties(), "=", true, false));
            sb.append(")");
        }

        if (getBrokerDesc() != null) {
            sb.append("\n WITH BROKER '").append(getBrokerDesc().getName()).append("' (");
            sb.append(new PrintableMap<>(getBrokerDesc().getProperties(), "=", true, false, true));
            sb.append(")");
        }

        return sb.toString();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateColddownStatement(this, context);
    }
}