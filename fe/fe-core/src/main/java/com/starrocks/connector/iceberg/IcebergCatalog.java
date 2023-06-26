// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector.iceberg;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.connector.exception.StarRocksConnectorException;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.thrift.TException;

import java.util.List;
import java.util.Map;

public interface IcebergCatalog {

    IcebergCatalogType getIcebergCatalogType();

    Table loadTable(IcebergTable table) throws StarRocksConnectorException;

    Table loadTable(TableIdentifier tableIdentifier) throws StarRocksConnectorException;

    default Table loadTableWithUgi(TableIdentifier tableIdentifier, UserGroupInformation ugi) {
        throw new NotImplementedException();
    }

    Table loadTable(TableIdentifier tableId,
                    String tableLocation,
                    Map<String, String> properties) throws StarRocksConnectorException;

    List<String> listAllDatabases();

    Database getDB(String dbName) throws InterruptedException, TException;

    List<TableIdentifier> listTables(Namespace of);

    default Transaction newCreateTableTransaction(
            String dbName,
            String tableName,
            Schema schema,
            PartitionSpec partitionSpec,
            String location,
            Map<String, String> properties) {
        throw new StarRocksConnectorException("This catalog doesn't support creating tables");
    }

    default String defaultTableLocation(String dbName, String tblName) {
        return "";
    }

}
