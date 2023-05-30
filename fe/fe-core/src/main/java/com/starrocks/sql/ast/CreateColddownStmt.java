// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.google.common.base.Strings;
import com.starrocks.analysis.BrokerDesc;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.TableRef;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.UserException;
import com.starrocks.common.util.PrintableMap;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.iceberg.IcebergMetadata;
import com.starrocks.fs.HdfsUtil;
import com.starrocks.fs.hdfs.HdfsFs;
import com.starrocks.load.export.ExternalTableExportConfig;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.SemanticException;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.hadoop.Util;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.starrocks.load.export.ExternalTableExportConfig.EXTERNAL_TABLE;

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
        Table externalTable = externalTableExportConfig.verifyExternalTable();
        if (externalTable != null) {
            return;
        }
        createExternalTable(table);
    }

    private void createExternalTable(Table table) {
        TableName extTableName = AnalyzerUtils.stringToTableName(getTargetProperties().get(EXTERNAL_TABLE));
        String extCatalogName = extTableName.getCatalog();
        String extDbName = extTableName.getDb();
        if (extCatalogName == null) {
            extCatalogName = InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME;
            if (Strings.isNullOrEmpty(extDbName)) {
                extDbName = getTblName().getDb();
            }
        }
        if (!shouldCreateExternalTable(extCatalogName)) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_TABLE_ERROR,
                    getTargetProperties().get(EXTERNAL_TABLE));
            return;
        }
        ConnectorMetadata connectorMetadata = GlobalStateMgr.getCurrentState().getMetadataMgr()
                .getOptionalMetadata(extCatalogName).get();
        try {
            Map<String, String> properties = new HashMap<>();
            getProperties().forEach((key, value) -> {
                if (key.startsWith("colddown_") || key.startsWith("max_file_") || key.equals("timeout")) {
                    return;
                }
                properties.put(key, value);
            });
            if (!Config.enable_check_tdw_pri) {
                properties.put(TableProperties.ENGINE_HIVE_ENABLED, "true");
                properties.put("external.table.purge", "true");
            }
            properties.put("uuid", UUID.randomUUID().toString());

            String tableLocation = ((IcebergMetadata) connectorMetadata).getTableLocation(extDbName,
                    extTableName.getTbl(), properties);
            HdfsFs fileSystem = HdfsUtil.getFileSystem(tableLocation, getBrokerDesc());
            String finalExtDbName = extDbName;
            Util.doAsWithUGI(fileSystem.getUgi(), () -> {
                ((IcebergMetadata) connectorMetadata).createTable(finalExtDbName, extTableName.getTbl(),
                        table.getColumns(), table.getPartitionColumnNames(), properties);
                return null;
            });
        } catch (IOException | UserException e) {
            throw new SemanticException(e.getMessage(), e);
        }
    }

    private boolean shouldCreateExternalTable(String extCatalogName) {
        if (!isAutomaticallyCreateTargetTable() || !CatalogMgr.isExternalCatalog(extCatalogName)) {
            return false;
        }
        String catalogType = GlobalStateMgr.getCurrentState().getCatalogMgr().getCatalogType(extCatalogName);
        if (catalogType == null) {
            throw new SemanticException("catalog " + extCatalogName + " not exist!");
        }
        // currently, try to create table only when it is iceberg catalog
        return catalogType.equals("iceberg");
    }

    private boolean isAutomaticallyCreateTargetTable() {
        if (getTargetProperties().containsKey("automatically_create_target_table")) {
            return Boolean.parseBoolean(getTargetProperties().get("automatically_create_target_table"));
        } else {
            return true;
        }
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