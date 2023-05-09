// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.analysis.BrokerDesc;
import com.starrocks.analysis.Delimiter;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.RedirectStatus;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.TableRef;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.util.PrintableMap;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.load.ExportJob;
import com.starrocks.load.export.ExportTargetType;
import com.starrocks.load.export.ExternalTableExportConfig;
import com.starrocks.load.export.HiveExportConfig;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

// EXPORT statement, export data to dirs by broker.
//
// syntax:
//      EXPORT TABLE tablename [PARTITION (name1[, ...])]
//          [(col1, col2[, ...])]
//          TO 'export_target_path'|EXTERNAL_TABLE
//          [PROPERTIES("key"="value")]
//          WITH BROKER 'broker_name' [( $broker_attrs)]
public class ExportStmt extends StatementBase {

    private static final String FILE_FORMAT = "file_format";
    private static final String INCLUDE_QUERY_ID_PROP = "include_query_id";

    private static final String DEFAULT_COLUMN_SEPARATOR = "\t";
    private static final String DEFAULT_LINE_DELIMITER = "\n";
    private static final String DEFAULT_FILE_NAME_PREFIX = "data_";
    private static final String DEFAULT_FILE_FORMAT = "csv";

    private static final Set<String> VALID_SCHEMES = Sets.newHashSet(
            "afs", "bos", "hdfs", "oss", "s3a", "cosn", "viewfs", "ks3");

    private TableName tblName;
    private List<String> partitions;
    private List<String> columnNames;
    private final Expr whereExpr;
    private String typeName;
    // path should include "/"
    private String path;
    private String fileNamePrefix;
    private final BrokerDesc brokerDesc;
    private Map<String, String> targetProperties = Maps.newHashMap();
    private Map<String, String> properties = Maps.newHashMap();
    private String columnSeparator;
    private String rowDelimiter;
    protected boolean includeQueryId = true;
    private String fileFormat;
    private List<String> exportColumnNames;
    private Map<String, Type> exportTypes;

    // configs
    private ExternalTableExportConfig externalTableExportConfig;
    private HiveExportConfig hiveExportConfig;

    // may catalog.db.table
    private TableRef tableRef;
    private long exportStartTime;

    public ExportStmt(TableRef tableRef, List<String> columnNames, Expr whereExpr, String typeName,
                      Map<String, String> targetProperties, Map<String, String> properties, BrokerDesc brokerDesc) {
        this.tableRef = tableRef;
        this.columnNames = columnNames;
        this.whereExpr = whereExpr;
        this.typeName = typeName.trim();
        this.path = typeName;
        if (properties != null) {
            this.targetProperties = targetProperties;
        }
        if (properties != null) {
            this.properties = properties;
        }
        this.brokerDesc = brokerDesc;
        this.columnSeparator = DEFAULT_COLUMN_SEPARATOR;
        this.rowDelimiter = DEFAULT_LINE_DELIMITER;
        this.includeQueryId = true;
    }

    public long getExportStartTime() {
        return exportStartTime;
    }

    public void setTblName(TableName tblName) {
        this.tblName = tblName;
    }

    public void setPartitions(List<String> partitions) {
        this.partitions = partitions;
    }

    public void setExportStartTime(long exportStartTime) {
        this.exportStartTime = exportStartTime;
    }

    public TableName getTblName() {
        return tblName;
    }

    public TableRef getTableRef() {
        return tableRef;
    }

    public List<String> getPartitions() {
        return partitions;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public Expr getWhereExpr() {
        return whereExpr;
    }

    public String getTypeName() {
        return typeName;
    }

    public String getPath() {
        return path;
    }

    public String getFileNamePrefix() {
        return fileNamePrefix;
    }

    public BrokerDesc getBrokerDesc() {
        return brokerDesc;
    }

    public Map<String, String> getTargetProperties() {
        return targetProperties;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public String getColumnSeparator() {
        return this.columnSeparator;
    }

    public String getRowDelimiter() {
        return this.rowDelimiter;
    }

    public boolean isIncludeQueryId() {
        return includeQueryId;
    }

    public String getFileFormat() {
        return fileFormat == null ? DEFAULT_FILE_FORMAT : fileFormat;
    }

    public List<String> getExportColumnNames() {
        return exportColumnNames;
    }

    public Map<String, Type> getExportTypes() {
        return exportTypes;
    }

    @Override
    public boolean needAuditEncryption() {
        return brokerDesc != null;
    }

    public void checkTable(GlobalStateMgr globalStateMgr) {
        Database db = globalStateMgr.getDb(tblName.getDb());
        if (db == null) {
            throw new SemanticException("Db does not exist. name: " + tblName.getDb());
        }
        db.readLock();
        try {
            Table table = db.getTable(tblName.getTbl());
            if (table == null) {
                throw new SemanticException("Table[" + tblName.getTbl() + "] does not exist");
            }

            Table.TableType tblType = table.getType();
            switch (tblType) {
                case MYSQL:
                case OLAP:
                case LAKE:
                    break;
                case BROKER:
                case SCHEMA:
                case INLINE_VIEW:
                case VIEW:
                default:
                    throw new SemanticException("Table[" + tblName.getTbl() + "] is " + tblType +
                            " type, do not support EXPORT.");
            }

            if (partitions != null) {
                for (String partitionName : partitions) {
                    Partition partition = table.getPartition(partitionName);
                    if (partition == null) {
                        throw new SemanticException("Partition [" + partitionName + "] does not exist.");
                    }
                }
            }

            // check columns
            Set<String> tableColumns = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
            for (Column column : table.getBaseSchema()) {
                tableColumns.add(column.getName());
            }
            if (columnNames != null) {
                if (columnNames.isEmpty()) {
                    throw new SemanticException("Columns is empty.");
                }

                Set<String> uniqColumnNames = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
                for (String columnName : columnNames) {
                    if (!uniqColumnNames.add(columnName)) {
                        throw new SemanticException("Duplicated column [" + columnName + "]");
                    }
                    if (!tableColumns.contains(columnName)) {
                        throw new SemanticException("Column [" + columnName + "] does not exist in table.");
                    }
                }
                exportColumnNames = Lists.newArrayList(columnNames);
            } else {
                exportColumnNames = Lists.newArrayList(tableColumns);
            }
        } finally {
            db.readUnlock();
        }
    }

    public void checkType(Table table) {
        ExportTargetType type;
        try {
            type = ExportTargetType.valueOf(typeName);
        } catch (IllegalArgumentException e) {
            type = ExportTargetType.HDFS;
            typeName = type.name();
            if (path.startsWith("\"")) {
                path = path.substring(1);
            }
            if (path.endsWith("\"")) {
                path = path.trim().substring(0, path.length() - 1);
            }
            targetProperties.put("path", path);
        }
        switch (type) {
            case EXTERNAL_TABLE:
                externalTableExportConfig =
                        new ExternalTableExportConfig(tblName, properties, targetProperties, brokerDesc);
                checkExternalTable(externalTableExportConfig, table);
                break;
            case HDFS:
            case LOCAL:
                path = targetProperties.get("path");
                checkPath();
                break;
            case HIVE:
                hiveExportConfig =
                        new HiveExportConfig(targetProperties);
                hiveExportConfig.analyzeProperties();
                path = hiveExportConfig.getPath();
                checkPath();
                break;
            default:
                break;
        }
    }

    protected void checkExternalTable(ExternalTableExportConfig externalTableExportConfig, Table table) {
        if (partitions == null || partitions.size() != 1) {
            throw new SemanticException("Does not support export multiple partitions to external table now.");
        }
        externalTableExportConfig.analyzeProperties(table, partitions.get(0));
        path = externalTableExportConfig.getPath();
        exportTypes = externalTableExportConfig.getExportTypes();
        exportColumnNames = externalTableExportConfig.reorder(exportColumnNames);
        if (fileFormat == null) {
            switch (externalTableExportConfig.getTargetTableType()) {
                case HIVE:
                    fileFormat = "orc";
                    break;
                case ICEBERG:
                    fileFormat = "parquet";
                    break;
            }
        }
        checkPath();
    }

    public Function<ExportJob, Void> getBeforeFinishFunction() {
        ExportTargetType type = ExportTargetType.valueOf(typeName);
        switch (type) {
            case EXTERNAL_TABLE:
                return externalTableExportConfig.getBeforeFinishFunction();
            case HDFS:
            case LOCAL:
                path = targetProperties.get("path");
                checkPath();
                break;
            case HIVE:
                hiveExportConfig =
                        new HiveExportConfig(targetProperties);
                hiveExportConfig.analyzeProperties();
                path = hiveExportConfig.getPath();
                checkPath();
                break;
            default:
                break;
        }
        return null;
    }

    private void checkPath() {
        if (Strings.isNullOrEmpty(path)) {
            throw new SemanticException("No dest path specified.");
        }

        try {
            URI uri = new URI(path);
            String scheme = uri.getScheme();
            if (scheme == null) {
                throw new SemanticException("Invalid export path. please use valid scheme: " + VALID_SCHEMES);
            }
            path = uri.normalize().toString();
        } catch (URISyntaxException e) {
            throw new SemanticException("Invalid path format. " + e.getMessage());
        }

        if (path.endsWith("/")) {
            fileNamePrefix = DEFAULT_FILE_NAME_PREFIX;
        } else {
            int lastSlashIndex = path.lastIndexOf("/");
            fileNamePrefix = path.substring(lastSlashIndex + 1);
            // path should include "/"
            path = path.substring(0, lastSlashIndex + 1);
        }
    }

    public void checkProperties(Map<String, String> properties) throws AnalysisException {
        this.columnSeparator = PropertyAnalyzer.analyzeColumnSeparator(
                properties, ExportStmt.DEFAULT_COLUMN_SEPARATOR);
        this.columnSeparator = Delimiter.convertDelimiter(this.columnSeparator);
        this.rowDelimiter = PropertyAnalyzer.analyzeRowDelimiter(properties, ExportStmt.DEFAULT_LINE_DELIMITER);
        this.rowDelimiter = Delimiter.convertDelimiter(this.rowDelimiter);
        if (properties.containsKey(LoadStmt.LOAD_MEM_LIMIT)) {
            try {
                Long.parseLong(properties.get(LoadStmt.LOAD_MEM_LIMIT));
            } catch (NumberFormatException e) {
                throw new AnalysisException("Invalid load_mem_limit value: " + e.getMessage());
            }
        } else {
            // use session variables
            properties.put(LoadStmt.LOAD_MEM_LIMIT,
                    String.valueOf(ConnectContext.get().getSessionVariable().getMaxExecMemByte()));
        }
        // timeout
        if (properties.containsKey(LoadStmt.TIMEOUT_PROPERTY)) {
            try {
                Long.parseLong(properties.get(LoadStmt.TIMEOUT_PROPERTY));
            } catch (NumberFormatException e) {
                throw new AnalysisException("Invalid timeout value: " + e.getMessage());
            }
        } else {
            // use session variables
            properties.put(LoadStmt.TIMEOUT_PROPERTY, String.valueOf(Config.export_task_default_timeout_second));
        }

        // include query id
        if (properties.containsKey(INCLUDE_QUERY_ID_PROP)) {
            String includeQueryIdStr = properties.get(INCLUDE_QUERY_ID_PROP);
            if (!includeQueryIdStr.equalsIgnoreCase("true")
                    && !includeQueryIdStr.equalsIgnoreCase("false")) {
                throw new AnalysisException("Invalid include query id value: " + includeQueryIdStr);
            }
            includeQueryId = Boolean.parseBoolean(properties.get(INCLUDE_QUERY_ID_PROP));
        }

        // file format
        if (properties.containsKey(FILE_FORMAT)) {
            String fileFormatStr = properties.get(FILE_FORMAT);
            if (!fileFormatStr.equalsIgnoreCase("csv")
                    && !fileFormatStr.equalsIgnoreCase("parquet")
                    && !fileFormatStr.equalsIgnoreCase("orc")) {
                throw new AnalysisException("Invalid file format: " + fileFormatStr);
            }
            this.fileFormat = fileFormatStr.toLowerCase();
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("EXPORT TABLE ");
        if (tblName == null) {
            sb.append("non-exist");
        } else {
            sb.append(tblName.toSql());
        }
        if (partitions != null && !partitions.isEmpty()) {
            sb.append(" PARTITION (");
            Joiner.on(", ").appendTo(sb, partitions);
            sb.append(")");
        }
        sb.append("\n");
        if (whereExpr != null) {
            sb.append("Where (");
            sb.append(whereExpr.toSql());
            sb.append(")");
        }
        sb.append("\n");

        sb.append(" TO ").append("'");
        sb.append(typeName);
        sb.append("'");

        if (targetProperties != null && !targetProperties.isEmpty()) {
            sb.append("\n(");
            sb.append(new PrintableMap<String, String>(targetProperties, "=", true, false));
            sb.append(")");
        }

        if (properties != null && !properties.isEmpty()) {
            sb.append("\nPROPERTIES (");
            sb.append(new PrintableMap<String, String>(properties, "=", true, false));
            sb.append(")");
        }

        if (brokerDesc != null) {
            sb.append("\n WITH BROKER '").append(brokerDesc.getName()).append("' (");
            sb.append(new PrintableMap<String, String>(brokerDesc.getProperties(), "=", true, false, true));
            sb.append(")");
        }

        return sb.toString();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitExportStatement(this, context);
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