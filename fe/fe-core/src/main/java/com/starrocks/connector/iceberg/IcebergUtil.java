// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector.iceberg;

import com.starrocks.catalog.IcebergResource;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.Resource;
import com.starrocks.catalog.ResourceMgr;
import com.starrocks.common.AnalysisException;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.hive.IcebergHiveCatalog;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
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
}