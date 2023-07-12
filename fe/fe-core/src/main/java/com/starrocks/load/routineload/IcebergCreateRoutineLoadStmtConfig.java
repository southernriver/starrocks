// Licensed to the Apache Software Foundation (ASF) under one

package com.starrocks.load.routineload;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.starrocks.analysis.BrokerDesc;
import com.starrocks.analysis.Expr;
import com.starrocks.common.AnalysisException;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.ast.CreateRoutineLoadStmt;
import com.starrocks.sql.parser.SqlParser;

import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

public class IcebergCreateRoutineLoadStmtConfig {
    private static final String DEFAULT_ICEBERG_CATALOG_NAME = "iceberg";
    // iceberg type properties
    private static final String ICEBERG_CATALOG_TYPE = "iceberg_catalog_type";
    private static final String ICEBERG_CATALOG_HIVE_METASTORE_URIS = "iceberg_catalog_hive_metastore_uris";
    private static final String ICEBERG_RESOURCE_NAME = "iceberg_resource_name";
    private static final String ICEBERG_CATALOG_NAME = "iceberg_catalog_name";
    private static final String ICEBERG_DATABASE = "iceberg_database";
    private static final String ICEBERG_TABLE = "iceberg_table";
    // optional
    private static final String ICEBERG_CONSUME_POSITION = "iceberg_consume_position";
    public static final String ICEBERG_WHERE_EXPR = "iceberg_where_expr";
    private static final String READ_ICEBERG_SNAPSHOTS_AFTER_TIMESTAMP = "read_iceberg_snapshots_after_timestamp";
    private static final String ICEBERG_PLAN_SPLIT_SIZE = "plan_split_size";

    private static final String ICEBERG_FROM_EARLIEST = "FROM_EARLIEST";
    private static final String ICEBERG_FROM_LATEST = "FROM_LATEST";

    private static final ImmutableSet<String> ICEBERG_PROPERTIES_SET = new ImmutableSet.Builder<String>()
            .add(ICEBERG_CATALOG_TYPE)
            .add(ICEBERG_CATALOG_HIVE_METASTORE_URIS)
            .add(ICEBERG_RESOURCE_NAME)
            .add(ICEBERG_CATALOG_NAME)
            .add(ICEBERG_DATABASE)
            .add(ICEBERG_TABLE)
            .add(ICEBERG_CONSUME_POSITION)
            .add(ICEBERG_WHERE_EXPR)
            .build();

    // iceberg related properties
    private String icebergCatalogType;
    private String icebergCatalogHiveMetastoreUris;
    private String icebergResourceName;
    private String icebergCatalogName;
    private String icebergDatabase;
    private String icebergTable = null;
    private String icebergConsumePosition = null;
    private Expr icebergWhereExpr = null;
    private final Map<String, String> dataSourceProperties;
    // custom iceberg property map<key, value>
    private Map<String, String> customIcebergProperties = Maps.newHashMap();
    private BrokerDesc brokerDesc;

    public IcebergCreateRoutineLoadStmtConfig(Map<String, String> dataSourceProperties, BrokerDesc brokerDesc) {
        this.dataSourceProperties = dataSourceProperties;
        this.brokerDesc = brokerDesc;
    }

    public String getIcebergCatalogType() {
        return icebergCatalogType;
    }

    public static boolean isHiveCatalogType(String icebergCatalogType) {
        return "HIVE".equalsIgnoreCase(icebergCatalogType);
    }

    public static boolean isResourceCatalogType(String icebergCatalogType) {
        return "RESOURCE".equalsIgnoreCase(icebergCatalogType);
    }

    public static boolean isExternalCatalogType(String icebergCatalogType) {
        return "EXTERNAL_CATALOG".equalsIgnoreCase(icebergCatalogType);
    }

    public String getIcebergCatalogHiveMetastoreUris() {
        return icebergCatalogHiveMetastoreUris;
    }

    public String getIcebergResourceName() {
        return icebergResourceName;
    }

    public String getIcebergCatalogName() {
        return icebergCatalogName;
    }

    public String getIcebergDatabase() {
        return icebergDatabase;
    }

    public String getIcebergTable() {
        return icebergTable;
    }

    public String getIcebergConsumePosition() {
        return icebergConsumePosition;
    }

    public Map<String, String> getCustomIcebergProperties() {
        return customIcebergProperties;
    }

    public BrokerDesc getBrokerDesc() {
        return brokerDesc;
    }

    public Expr getIcebergWhereExpr() {
        return icebergWhereExpr;
    }

    public void checkIcebergProperties() throws AnalysisException {
        Optional<String> optional = dataSourceProperties.keySet().stream()
                .filter(entity -> !ICEBERG_PROPERTIES_SET.contains(entity))
                .filter(entity -> !entity.startsWith("property.")).findFirst();
        if (optional.isPresent()) {
            throw new AnalysisException(optional.get() + " is invalid iceberg custom property");
        }

        icebergDatabase = getDataSourceProperty(ICEBERG_DATABASE);
        icebergTable = getDataSourceProperty(ICEBERG_TABLE);
        icebergCatalogType = getDataSourceProperty(ICEBERG_CATALOG_TYPE);
        if (isHiveCatalogType(icebergCatalogType)) {
            icebergCatalogHiveMetastoreUris = getDataSourceProperty(ICEBERG_CATALOG_HIVE_METASTORE_URIS);

            if (!Pattern.matches(CreateRoutineLoadStmt.ENDPOINT_REGEX, icebergCatalogHiveMetastoreUris)) {
                throw new AnalysisException(ICEBERG_CATALOG_HIVE_METASTORE_URIS + ":" + icebergCatalogHiveMetastoreUris
                        + " not match pattern " + CreateRoutineLoadStmt.ENDPOINT_REGEX);
            }
        } else if (isResourceCatalogType(icebergCatalogType)) {
            icebergResourceName = getIcebergResourceName(ICEBERG_RESOURCE_NAME);
        } else if (isExternalCatalogType(icebergCatalogType)) {
            icebergCatalogName = getIcebergCatalogName(ICEBERG_CATALOG_NAME);
        } else {
            throw new AnalysisException(ICEBERG_CATALOG_TYPE + ":" + icebergCatalogType + " not supported");
        }

        // check positions
        String consumePosition = dataSourceProperties.get(ICEBERG_CONSUME_POSITION);
        if (consumePosition != null) {
            icebergConsumePosition = getIcebergConsumePosition(consumePosition);
        } else {
            icebergConsumePosition = ICEBERG_FROM_LATEST;
        }
        String whereString = Strings.emptyToNull(dataSourceProperties.get(ICEBERG_WHERE_EXPR));
        if (whereString != null) {
            icebergWhereExpr = SqlParser.parseSqlToExpr(whereString, SqlModeHelper.MODE_DEFAULT);
            customIcebergProperties.put(ICEBERG_WHERE_EXPR, whereString);
        }
        analyzeIcebergCustomProperties(dataSourceProperties, customIcebergProperties);
    }

    public static Expr getIcebergWhereExprFromCustomIcebergProperties(Map<String, String> customIcebergProperties) {
        String whereString = customIcebergProperties.get(ICEBERG_WHERE_EXPR);
        return whereString != null ? SqlParser.parseSqlToExpr(whereString, SqlModeHelper.MODE_DEFAULT) : null;
    }

    private String getDataSourceProperty(String key) throws AnalysisException {
        String value = Strings.nullToEmpty(dataSourceProperties.get(key)).trim();
        if (Strings.isNullOrEmpty(value)) {
            throw new AnalysisException(key + " is a required property");
        }
        return value;
    }

    private String getIcebergResourceName(String key) throws AnalysisException {
        String value = Strings.nullToEmpty(dataSourceProperties.get(key)).trim();
        if (Strings.isNullOrEmpty(value)) {
            throw new AnalysisException(key + " is a required property");
        }
        return value;
    }

    private String getIcebergCatalogName(String key) {
        String value = Strings.nullToEmpty(dataSourceProperties.get(key)).trim();
        if (Strings.isNullOrEmpty(value)) {
            return DEFAULT_ICEBERG_CATALOG_NAME;
        }
        return value;
    }

    private String getIcebergConsumePosition(String consumePositionStr) throws AnalysisException {
        if (consumePositionStr.equalsIgnoreCase(ICEBERG_FROM_EARLIEST)) {
            return ICEBERG_FROM_EARLIEST;
        } else if (consumePositionStr.equalsIgnoreCase(ICEBERG_FROM_LATEST)) {
            return ICEBERG_FROM_LATEST;
        } else {
            throw new AnalysisException("Only FROM_EARLIEST/FROM_LATEST can be specified");
        }
    }

    public static void analyzeIcebergCustomProperties(Map<String, String> dataSourceProperties,
                                                      Map<String, String> customIcebergProperties)
            throws AnalysisException {
        CreateRoutineLoadStmt.setCustomProperties(dataSourceProperties, customIcebergProperties);
    }

    public static Long getReadIcebergSnapshotsAfterTimestamp(Map<String, String> customIcebergProperties) {
        return getLong(customIcebergProperties, READ_ICEBERG_SNAPSHOTS_AFTER_TIMESTAMP);
    }

    public static Long getIcebergPlanSplitSize(Map<String, String> customIcebergProperties) {
        return getLong(customIcebergProperties, ICEBERG_PLAN_SPLIT_SIZE);
    }

    private static Long getLong(Map<String, String> properties, String key) {
        String value = properties.get(key);
        if (value == null || value.isEmpty()) {
            return null;
        }
        try {
            return Long.valueOf(value);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    public static boolean isFromEarliest(String consumePosition) {
        return ICEBERG_FROM_EARLIEST.equalsIgnoreCase(consumePosition);
    }
}
