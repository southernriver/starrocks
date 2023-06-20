// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.common.DdlException;
import com.starrocks.common.util.Util;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.RemoteFileDesc;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.Statistics;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.types.Types;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.starrocks.catalog.IcebergTable.ICEBERG_CATALOG_TYPE;
import static com.starrocks.catalog.IcebergTable.ICEBERG_IMPL;
import static com.starrocks.catalog.IcebergTable.ICEBERG_METASTORE_URIS;
import static com.starrocks.connector.iceberg.IcebergUtil.getIcebergCustomCatalog;
import static com.starrocks.connector.iceberg.IcebergUtil.getIcebergGlueCatalog;
import static com.starrocks.connector.iceberg.IcebergUtil.getIcebergHiveCatalog;

public class IcebergMetadata implements ConnectorMetadata {

    private static final Logger LOG = LogManager.getLogger(IcebergMetadata.class);
    private String metastoreURI;
    private String catalogType;
    private String catalogImpl;
    private final String catalogName;
    private IcebergCatalog icebergCatalog;
    private Map<String, String> customProperties;
    private final Map<IcebergFilter, List<FileScanTask>> tasks = new ConcurrentHashMap<>();

    public IcebergMetadata(String catalogName, Map<String, String> properties, HdfsEnvironment hdfsEnvironment) {
        this.catalogName = catalogName;

        if (IcebergCatalogType.HIVE_CATALOG == IcebergCatalogType.fromString(properties.get(ICEBERG_CATALOG_TYPE))) {
            catalogType = properties.get(ICEBERG_CATALOG_TYPE);
            metastoreURI = properties.get(ICEBERG_METASTORE_URIS);
            Util.validateMetastoreUris(metastoreURI);
            icebergCatalog = getIcebergHiveCatalog(metastoreURI, properties, hdfsEnvironment);
        } else if (IcebergCatalogType.CUSTOM_CATALOG ==
                IcebergCatalogType.fromString(properties.get(ICEBERG_CATALOG_TYPE))) {
            catalogType = properties.get(ICEBERG_CATALOG_TYPE);
            catalogImpl = properties.get(ICEBERG_IMPL);
            icebergCatalog = getIcebergCustomCatalog(catalogImpl, properties, hdfsEnvironment);
            properties.remove(ICEBERG_CATALOG_TYPE);
            properties.remove(ICEBERG_IMPL);
            customProperties = properties;
        } else if (IcebergCatalogType.GLUE_CATALOG ==
                IcebergCatalogType.fromString(properties.get(ICEBERG_CATALOG_TYPE))) {
            catalogType = properties.get(ICEBERG_CATALOG_TYPE);
            icebergCatalog = getIcebergGlueCatalog(catalogName, properties, hdfsEnvironment);
        } else {
            throw new RuntimeException(String.format("Property %s is missing or not supported now.",
                    ICEBERG_CATALOG_TYPE));
        }
    }

    @Override
    public List<String> listDbNames() {
        return icebergCatalog.listAllDatabases();
    }

    @Override
    public Database getDb(String dbName) {
        try {
            return icebergCatalog.getDB(dbName);
        } catch (InterruptedException | TException e) {
            LOG.error("Failed to get iceberg database " + dbName, e);
            return null;
        }
    }

    @Override
    public List<String> listTableNames(String dbName) {
        List<TableIdentifier> tableIdentifiers = icebergCatalog.listTables(Namespace.of(dbName));
        return tableIdentifiers.stream().map(TableIdentifier::name).collect(Collectors.toCollection(ArrayList::new));
    }

    @Override
    public Table getTable(String dbName, String tblName) {
        try {
            org.apache.iceberg.Table icebergTable = getIcebergTable(dbName, tblName);
            // Submit a future task for refreshing
            GlobalStateMgr.getCurrentState().getIcebergRepository().refreshTable(icebergTable);
            if (IcebergCatalogType.fromString(catalogType).equals(IcebergCatalogType.CUSTOM_CATALOG)) {
                return IcebergUtil.convertCustomCatalogToSRTable(icebergTable, catalogImpl, catalogName, dbName,
                        tblName, customProperties);
            } else if (IcebergCatalogType.fromString(catalogType).equals(IcebergCatalogType.GLUE_CATALOG)) {
                return IcebergUtil.convertGlueCatalogToSRTable(icebergTable, catalogName, dbName, tblName);
            } else {
                return IcebergUtil.convertHiveCatalogToSRTable(icebergTable, metastoreURI, catalogName, dbName,
                        tblName);
            }
        } catch (DdlException e) {
            LOG.error("Failed to get iceberg table " + IcebergUtil.getIcebergTableIdentifier(dbName, tblName), e);
            return null;
        }
    }

    @Override
    public List<String> listPartitionNames(String dbName, String tblName) {
        org.apache.iceberg.Table icebergTable
                = icebergCatalog.loadTable(IcebergUtil.getIcebergTableIdentifier(dbName, tblName));
        if (!IcebergCatalogType.fromString(catalogType).equals(IcebergCatalogType.HIVE_CATALOG)
                && !IcebergCatalogType.fromString(catalogType).equals(IcebergCatalogType.GLUE_CATALOG)) {
            throw new StarRocksIcebergException(
                    "Do not support get partitions from catalog type: " + catalogType);
        }
        if (icebergTable.spec().fields().stream()
                .anyMatch(partitionField -> !partitionField.transform().isIdentity())) {
            throw new StarRocksIcebergException(
                    "Do not support get partitions from No-Identity partition transform now");
        }

        return IcebergUtil.getIdentityPartitionNames(icebergTable);
    }

    public org.apache.iceberg.Table getIcebergTable(String dbName, String tblName) {
        return icebergCatalog.loadTable(IcebergUtil.getIcebergTableIdentifier(dbName, tblName));
    }

    @Override
    public List<RemoteFileInfo> getRemoteFileInfos(Table table, List<PartitionKey> partitionKeys,
                                                   long snapshotId, ScalarOperator predicate) {
        return getRemoteFileInfos((IcebergTable) table, snapshotId, predicate);
    }

    private List<RemoteFileInfo> getRemoteFileInfos(IcebergTable table, long snapshotId, ScalarOperator predicate) {
        RemoteFileInfo remoteFileInfo = new RemoteFileInfo();
        IcebergFilter key = IcebergFilter.of(table.getRemoteDbName(), table.getRemoteTableName(), snapshotId, predicate);

        if (!tasks.containsKey(key)) {
            List<ScalarOperator> scalarOperators = Utils.extractConjuncts(predicate);
            org.apache.iceberg.Table nativeTbl = table.getNativeTable();
            Types.StructType schema = nativeTbl.schema().asStruct();
            ScalarOperatorToIcebergExpr.IcebergContext icebergContext = new ScalarOperatorToIcebergExpr.IcebergContext(schema);
            Expression icebergPredicate = new ScalarOperatorToIcebergExpr().convert(scalarOperators, icebergContext);

            ImmutableList.Builder<FileScanTask> builder = ImmutableList.builder();
            org.apache.iceberg.Table nativeTable = table.getNativeTable();
            TableScan scan = nativeTable.newScan().useSnapshot(snapshotId).filter(icebergPredicate);

            for (CombinedScanTask combinedScanTask : scan.planTasks()) {
                for (FileScanTask fileScanTask : combinedScanTask.files()) {
                    builder.add(fileScanTask);
                }
            }
            tasks.put(key, builder.build());
        }

        List<RemoteFileDesc> remoteFileDescs = ImmutableList.of(new RemoteFileDesc(tasks.get(key)));
        remoteFileInfo.setFiles(remoteFileDescs);

        return Lists.newArrayList(remoteFileInfo);
    }

    @Override
    public Statistics getTableStatistics(OptimizerContext session,
                                         Table table,
                                         Map<ColumnRefOperator, Column> columns,
                                         List<PartitionKey> partitionKeys,
                                         ScalarOperator predicate) {
        return Statistics.builder().build();
    }
}
