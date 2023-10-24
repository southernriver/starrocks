// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector.iceberg;

import com.starrocks.analysis.BrokerDesc;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergResource;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.Resource;
import com.starrocks.catalog.ResourceMgr;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.UserException;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.hive.IcebergHiveCatalog;
import com.starrocks.fs.HdfsUtil;
import com.starrocks.fs.hdfs.HdfsFs;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.Util;

import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static com.starrocks.server.CatalogMgr.ResourceMappingCatalog.getResourceMappingCatalogName;

public class IcebergUtil {
    private static final ConcurrentHashMap<String, IcebergHiveCatalog> METASTORE_URI_TO_CATALOG =
            new ConcurrentHashMap<>();

    private static synchronized IcebergHiveCatalog getIcebergHiveCatalogInstance(String uri,
                                                                                 Map<String, String> properties,
                                                                                 HdfsEnvironment hdfsEnvironment) {
        if (!METASTORE_URI_TO_CATALOG.containsKey(uri)) {
            properties.put(CatalogProperties.URI, uri);
            METASTORE_URI_TO_CATALOG.put(uri, (IcebergHiveCatalog) CatalogLoader.hive(String.format("hive-%s", uri),
                    hdfsEnvironment.getConfiguration(), properties).loadCatalog());
        }
        return METASTORE_URI_TO_CATALOG.get(uri);
    }

    public static org.apache.iceberg.Table getTableFromHiveMetastore(String metastoreUris, String dbName,
                                                                     String tblName) throws StarRocksConnectorException {
        IcebergHiveCatalog catalog =
                getIcebergHiveCatalogInstance(metastoreUris, new HashMap<>(), new HdfsEnvironment());
        return catalog.loadTable(TableIdentifier.of(dbName, tblName));
    }

    public static Table getTableFromResource(String resourceName, String dbName, String tblName)
            throws StarRocksConnectorException, AnalysisException {
        ResourceMgr resourceMgr = GlobalStateMgr.getCurrentState().getResourceMgr();
        Resource resource = resourceMgr.getResource(resourceName);
        if (resource == null) {
            throw new AnalysisException("Resource " + resourceName + " not exists");
        }
        if (!(resource instanceof IcebergResource)) {
            throw new AnalysisException("Resource " + resourceName + " is not iceberg resource");
        }

        String type = resource.getType().name().toLowerCase(Locale.ROOT);

        String catalogName = getResourceMappingCatalogName(resource.getName(), type);
        return getTableFromCatalog(catalogName, dbName, tblName);
    }

    public static Table getTableFromCatalog(String catalogName, String dbName, String tblName)
            throws StarRocksConnectorException, AnalysisException {
        MetadataMgr metadataMgr = GlobalStateMgr.getCurrentState().getMetadataMgr();
        IcebergTable resTable = (IcebergTable) metadataMgr.getTable(catalogName, dbName, tblName);
        if (resTable == null) {
            throw new StarRocksConnectorException(catalogName + "." + dbName + "." + tblName + " not exists");
        } else {
            return resTable.getNativeTable();
        }
    }

    public static void createTable(com.starrocks.catalog.Table table, TableName tableName, Map<String, String> prop,
                                   BrokerDesc brokerDesc) throws UserException, IOException {
        String catalogName = tableName.getCatalog();
        String dbName = tableName.getDb();
        ConnectorMetadata connectorMetadata = GlobalStateMgr.getCurrentState().getMetadataMgr()
                .getOptionalMetadata(catalogName).get();
        Map<String, String> properties = new HashMap<>(prop);
        if (!Config.is_tdw_hive) {
            properties.put(TableProperties.ENGINE_HIVE_ENABLED, "true");
            properties.put("external.table.purge", "true");
        }
        properties.put("uuid", UUID.randomUUID().toString());

        String tableLocation = ((IcebergMetadata) connectorMetadata).getTableLocation(dbName,
                tableName.getTbl(), properties);
        HdfsFs fileSystem = HdfsUtil.getFileSystem(tableLocation, brokerDesc);
        Util.doAsWithUGI(fileSystem.getUgi(), () -> {
            ((IcebergMetadata) connectorMetadata).createTable(dbName, tableName.getTbl(),
                    table.getFullSchema(), table.getPartitionColumnNames(), properties);
            return null;
        });
    }

    public static com.starrocks.catalog.Table applySchemaChange(com.starrocks.catalog.Table table,
                                                                IcebergTable icebergTable, BrokerDesc brokerDesc) {
        Map<String, Column> columnsInTargetTable = new HashMap<>();
        for (Column column : icebergTable.getBaseSchema()) {
            columnsInTargetTable.put(column.getName(), column);
        }
        String location = icebergTable.getTableLocation();
        try {
            HdfsFs fileSystem = HdfsUtil.getFileSystem(location, brokerDesc);
            Table iceTbl = icebergTable.getNativeTableWithUgi(fileSystem.getUgi());
            boolean needUpdate = false;
            UpdateSchema updateSchema = iceTbl.updateSchema();
            for (Column column : table.getBaseSchema()) {
                Column oldColumn = columnsInTargetTable.remove(column.getName());
                if (oldColumn == null) {
                    updateSchema.addColumn(column.getName(), IcebergApiConverter.toIcebergColumnType(column.getType()),
                            column.getComment());
                    needUpdate = true;
                } else if (!isColumnEqual(oldColumn, column)) {
                    updateSchema.updateColumn(column.getName(),
                            IcebergApiConverter.toIcebergColumnType(column.getType()).asPrimitiveType(),
                            column.getComment());
                    needUpdate = true;
                }
            }
            for (Column oldColumn : columnsInTargetTable.values()) {
                updateSchema.deleteColumn(oldColumn.getName());
                needUpdate = true;
            }
            if (!needUpdate) {
                return icebergTable;
            }
            Util.doAsWithUGI(fileSystem.getUgi(), () -> {
                updateSchema.commit();
                return null;
            });
            return getNewTable(icebergTable);
        } catch (UserException e) {
            String msg = String.format("failed to get FileSystem from path %s", location);
            throw new StarRocksConnectorException(msg, e);
        } catch (IOException e) {
            String msg = String.format("failed to add column to iceberg table %s", icebergTable.getTableIdentifier());
            throw new StarRocksConnectorException(msg, e);
        }
    }

    private static boolean isColumnEqual(Column left, Column right) {
        return Objects.equals(left.getName(), right.getName())
                && Objects.equals(IcebergApiConverter.toIcebergColumnType(left.getType()),
                IcebergApiConverter.toIcebergColumnType(right.getType()))
                && Objects.equals(left.getComment(), right.getComment());
    }

    private static com.starrocks.catalog.Table getNewTable(IcebergTable table) {
        // get the updated table instance
        return GlobalStateMgr.getCurrentState().getConnectorMgr()
                .getConnector(table.getCatalogName()).getMetadata()
                .getTable(table.getRemoteDbName(), table.getRemoteTableName());
    }
}