// Licensed to the Apache Software Foundation (ASF) under one

package com.starrocks.load.export;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.starrocks.analysis.BrokerDesc;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.TimestampArithmeticExpr;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DynamicPartitionProperty;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.Config;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.UserException;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.load.ExportJob;
import com.starrocks.load.FsUtil;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.task.ExportExportingTask;
import com.starrocks.utils.TdwUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

public class ExternalTableExportConfig {
    private static final Logger LOG = LogManager.getLogger(ExternalTableExportConfig.class);
    public static final String EXTERNAL_TABLE = "external_table";
    private static final String PARTITION_PREFIX = "partition.prefix";
    private static final String PARTITION_TIME_UNIT = "partition.time_unit";
    private static final String SR_TMP_PATH = "/__sr_tmp";
    private final TableName olapTableName;
    private final Map<String, String> targetProperties;
    private final BrokerDesc brokerDesc;
    private Table.TableType targetTableType;
    private Function<ExportJob, Void> beforeFinishFunction;
    private String path;
    private boolean overwritePartition = true;
    private LinkedHashMap<String, Type> exportTypes;

    public ExternalTableExportConfig(TableName olapTableName, Map<String, String> targetProperties,
                                     BrokerDesc brokerDesc) {
        this.olapTableName = olapTableName;
        this.targetProperties = targetProperties;
        this.brokerDesc = brokerDesc;
    }

    public void analyzeProperties(Table table, String partition) {
        Table externalTable = verifyExternalTable();
        targetTableType = externalTable.getType();
        exportTypes = new LinkedHashMap<>();
        for (Column column : externalTable.getBaseSchema()) {
            String name = column.getName();
            // tdw only
            if (name.startsWith("sys_thive_")) {
                name = name.substring("sys_thive_".length());
            }
            exportTypes.put(name, column.getType());
        }
        String targetPartitionName = getTargetExportPartition(table, externalTable, partition);
        overwritePartition = getOverwritePartition(targetProperties);
        if (externalTable.getType() == Table.TableType.HIVE) {
            prepareForHive((HiveTable) externalTable, targetPartitionName);
        }
    }

    public Table verifyExternalTable() {
        TableName extTableName = AnalyzerUtils.stringToTableName(
                Preconditions.checkNotNull(targetProperties.get(EXTERNAL_TABLE),
                        EXTERNAL_TABLE + " should be specified"));
        String extCatalogName = extTableName.getCatalog();
        String extDbName = extTableName.getDb();
        if (extCatalogName == null) {
            extCatalogName = InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME;
            if (Strings.isNullOrEmpty(extDbName)) {
                extDbName = olapTableName.getDb();
            }
        }
        Table table = GlobalStateMgr.getCurrentState().getMetadataMgr()
                .getTable(extCatalogName, extDbName, extTableName.getTbl());
        if (table == null) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_TABLE_ERROR, extTableName);
        }
        return table;
    }

    private boolean getOverwritePartition(Map<String, String> targetProperties) {
        String value = targetProperties.get("overwrite_partition");
        return Strings.isNullOrEmpty(value) || "true".equalsIgnoreCase(value);
    }

    public String getPath() {
        return path;
    }

    public Map<String, Type> getExportTypes() {
        return exportTypes;
    }

    public List<String> reorder(List<String> exportColumnNames) {
        if (exportTypes == null || exportTypes.isEmpty()) {
            return exportColumnNames;
        }
        List<String> newNames = Lists.newArrayListWithExpectedSize(exportColumnNames.size());
        for (String name : exportTypes.keySet()) {
            if (exportColumnNames.contains(name)) {
                newNames.add(name);
            }
        }
        return newNames;
    }

    public Table.TableType getTargetTableType() {
        return targetTableType;
    }

    public Function<ExportJob, Void> getBeforeFinishFunction() {
        return beforeFinishFunction;
    }

    private boolean isTdwHive(Table table) {
        return Config.enable_check_tdw_pri && table.getType() == Table.TableType.HIVE;
    }

    private String getTargetExportPartition(Table table, Table externalTable, String partition) {
        String targetPartition = targetProperties.get("target_partition");
        if (!Strings.isNullOrEmpty(targetPartition)) {
            return targetPartition;
        }
        Map<String, String> tableProperties = ((OlapTable) table).getTableProperty().getProperties();
        String timeUnitValue = tableProperties.get(DynamicPartitionProperty.TIME_UNIT);
        if (timeUnitValue == null) {
            throw new SemanticException("either " + DynamicPartitionProperty.TIME_UNIT + " in table or "
                    + PARTITION_TIME_UNIT + " in job should be specified");
        }
        String targetPartitionExpr = targetProperties.get("target_partition_expr");
        if (isTdwHive(externalTable)) {
            if (externalTable.isUnPartitioned()) {
                throw new SemanticException("Only partitioned table is supported now");
            }
            if (externalTable.getPartitionColumnNames().size() != 1) {
                throw new SemanticException("Multiple level partitioned table is not supported now");
            }
            String partitionColumn = externalTable.getPartitionColumnNames().get(0);
            Type partitionColumnType = exportTypes.get(partitionColumn);
            if (partitionColumnType == null) {
                throw new SemanticException("Partition column " + partitionColumn + " is not in export column list");
            }
            PrimitiveType primitiveType = partitionColumnType.getPrimitiveType();
            targetPartitionExpr = "%P" + getPartitionFormat(primitiveType, TimestampArithmeticExpr.TimeUnit.DAY);
        } else if (Strings.isNullOrEmpty(targetPartitionExpr)) {
            targetPartitionExpr = guessPartitionExpr(externalTable,
                    TimestampArithmeticExpr.TimeUnit.valueOf(timeUnitValue.toUpperCase()));
        }
        if (tableProperties.containsKey(DynamicPartitionProperty.PREFIX)) {
            String prefixValue = tableProperties.get(DynamicPartitionProperty.PREFIX);
            partition = partition.substring(prefixValue.length());
        }
        String tdwPartitionPrefix = "";
        if (isTdwHive(externalTable)) {
            tdwPartitionPrefix = tableProperties.get("tdw_partition_prefix");
            if (Strings.isNullOrEmpty(tdwPartitionPrefix)) {
                tdwPartitionPrefix = TdwUtil.TDW_PARTITION_DEFAULT_PREFIX;
            }
            // separator for tdw partition prefix which will be used to split partition value later in TdwUtil
            tdwPartitionPrefix += TdwUtil.TDW_PARTITION_PREFIX_SEPARATOR;
        }
        String year = "";
        String month = "";
        String day = "";
        String hour = "";
        String minute = "";
        switch (TimestampArithmeticExpr.TimeUnit.valueOf(timeUnitValue.toUpperCase())) {
            case MINUTE:
                year = partition.substring(0, 4);
                month = partition.substring(4, 6);
                day = partition.substring(6, 8);
                hour = partition.substring(8, 10);
                minute = partition.substring(10, 12);
                break;
            case HOUR:
                year = partition.substring(0, 4);
                month = partition.substring(4, 6);
                day = partition.substring(6, 8);
                hour = partition.substring(8, 10);
                break;
            case DAY:
                year = partition.substring(0, 4);
                month = partition.substring(4, 6);
                day = partition.substring(6, 8);
                break;
            case MONTH:
                year = partition.substring(0, 4);
                month = partition.substring(4, 6);
                break;
            default:
                throw new SemanticException("TimeUnit " + timeUnitValue + " is not supported in export now");
        }
        String exportPartition = targetPartitionExpr
                .replaceAll("%Y", year)
                .replaceAll("%m", month)
                .replaceAll("%d", day)
                .replaceAll("%H", hour)
                .replaceAll("%i", minute)
                .replaceAll("%P", tdwPartitionPrefix);
        if (!exportPartition.startsWith("/")) {
            exportPartition = "/" + exportPartition;
        }
        if (!exportPartition.endsWith("/")) {
            exportPartition = exportPartition + "/";
        }
        return exportPartition;
    }

    private String guessPartitionExpr(Table table, TimestampArithmeticExpr.TimeUnit timeUnit) {
        if (!table.isUnPartitioned() || table.getType() == Table.TableType.ICEBERG) {
            // try best to build a targetPartitionExpr
            List<String> partitionColumnNames = table.getPartitionColumnNames();
            if (partitionColumnNames.size() == 1) {
                String partitionColumn = table.getPartitionColumnNames().get(0);
                Type partitionColumnType = exportTypes.get(partitionColumn);
                PrimitiveType primitiveType = partitionColumnType.getPrimitiveType();
                String partitionFormat = getPartitionFormat(primitiveType, timeUnit);
                if (partitionFormat != null) {
                    return partitionColumn + "=" + partitionFormat;
                }
            }
        }
        throw new SemanticException("either target_partition or target_partition_expr should be specified");
    }

    private String getPartitionFormat(PrimitiveType primitiveType, TimestampArithmeticExpr.TimeUnit timeUnit) {
        switch (timeUnit) {
            case MINUTE:
                return primitiveType.isIntegerType() ? "%Y%m%d%H%i" : "%Y-%m-%d-%H-%i";
            case HOUR:
                return primitiveType.isIntegerType() ? "%Y%m%d%H" : "%Y-%m-%d-%H";
            case DAY:
                return primitiveType.isIntegerType() ? "%Y%m%d" : "%Y-%m-%d";
            case MONTH:
                return primitiveType.isIntegerType() ? "%Y%m" : "%Y-%m";
            default:
                return null;
        }
    }

    private void prepareForHive(HiveTable hiveTable, String originalTargetPartitionName) {
        String targetPartitionName = originalTargetPartitionName;
        if (Config.enable_check_tdw_pri) {
            targetPartitionName = targetPartitionName.replaceAll(TdwUtil.TDW_PARTITION_PREFIX_SEPARATOR, "")
                    .replaceAll("-", "");
        }
        String tmpTargetPartitionName = targetPartitionName;
        if (overwritePartition) {
            tmpTargetPartitionName = SR_TMP_PATH + "/tmp" + targetPartitionName;
        }
        path = hiveTable.getTableLocation() + tmpTargetPartitionName;
        Optional<ConnectorMetadata> connectorMetadataOpt = GlobalStateMgr.getCurrentState().getMetadataMgr()
                .getOptionalMetadata(hiveTable.getCatalogName());
        if (connectorMetadataOpt.isPresent()) {
            ConnectorMetadata connectorMetadata = connectorMetadataOpt.get();
            String finalTargetPartitionName = targetPartitionName;
            String tdwUser = TdwUtil.getCurrentTdwUserName();
            beforeFinishFunction = job -> {
                try {
                    TdwUtil.setCurrentTdwUserName(tdwUser);
                    Set<String> files = job.getExportedFiles();
                    if (files.isEmpty()) {
                        throw new StarRocksConnectorException("no data in partition: %s", finalTargetPartitionName);
                    }
                    // must use originalTargetPartitionName here
                    connectorMetadata.addPartition(hiveTable, originalTargetPartitionName);
                    if (!overwritePartition) {
                        return null;
                    }
                    String target = hiveTable.getTableLocation() + finalTargetPartitionName;
                    String backup = hiveTable.getTableLocation() + SR_TMP_PATH + finalTargetPartitionName;
                    // check export file exist
                    boolean targetExist;
                    try {
                        targetExist = FsUtil.checkPathExist(target, job.getBrokerDesc());
                    } catch (UserException e) {
                        throw new StarRocksConnectorException(e.getMessage(), e);
                    }
                    // 1. move target partition path to the backup path
                    if (targetExist) {
                        String failMsg = ExportExportingTask.moveFile(job, target, backup);
                        if (failMsg != null) {
                            throw new StarRocksConnectorException(failMsg);
                        }
                    }
                    // 2. move export path to partition path
                    job.removeExportTempPath();
                    String failMsg = ExportExportingTask.moveFile(job, path, target);
                    if (failMsg != null) {
                        // 3. restore target partition path from the backup path
                        String failMsg2 = ExportExportingTask.moveFile(job, backup, target);
                        if (failMsg2 != null) {
                            throw new StarRocksConnectorException(failMsg2);
                        }
                        throw new StarRocksConnectorException(failMsg);
                    }
                    // 3. delete the backup path
                    if (targetExist) {
                        try {
                            FsUtil.deletePath(backup, job.getBrokerDesc());
                        } catch (UserException e) {
                            throw new StarRocksConnectorException(e.getMessage(), e);
                        }
                    }
                    try {
                        // only the partitionValue after '=' matters
                        List<String> partitionNames;
                        if (Config.enable_check_tdw_pri) {
                            // /p_;2023-03-24/ -> [p_=2023-03-24]
                            partitionNames = Collections.singletonList(
                                    originalTargetPartitionName.replaceAll(TdwUtil.TDW_PARTITION_PREFIX_SEPARATOR,
                                            "="));
                        } else {
                            // /year=2023/month=03/day=24/ -> [year=2023, month=03, day=24]
                            partitionNames = Arrays.asList(removeSlash(finalTargetPartitionName).split("/"));
                        }
                        connectorMetadata.refreshTable(olapTableName.getDb(), hiveTable, partitionNames, true);
                    } catch (Exception e) {
                        LOG.warn("refresh partition + " + finalTargetPartitionName + " failed, need manual refresh", e);
                    }
                    return null;
                } finally {
                    TdwUtil.removeCurrentTdwUserName();
                }
            };
        } else {
            ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_CATALOG_AND_DB_ERROR, hiveTable.getCatalogName());
        }
    }

    private String removeSlash(String path) {
        if (path.startsWith("/")) {
            path = path.substring(1);
        }
        if (path.endsWith("/")) {
            path = path.substring(0, path.length() - 1);
        }
        return path;
    }
}
