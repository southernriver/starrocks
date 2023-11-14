// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.metric;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.starrocks.common.Config;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public final class TableMetricsRegistry {
    private static final Logger LOG = LogManager.getLogger(TableMetricsRegistry.class);

    private final Cache<Long, TableMetricsEntity> idToTableMetrics;
    private static final TableMetricsRegistry INSTANCE = new TableMetricsRegistry();

    private TableMetricsRegistry() {
        idToTableMetrics = CacheBuilder.newBuilder()
                .maximumSize(Config.max_table_metrics_num)
                .removalListener((RemovalListener<Long, TableMetricsEntity>) removalNotification -> {
                    MetricVisitor visitor = new SimpleCoreMetricVisitor("starrocks_fe");
                    LOG.info("Removed table metrics from table [" + removalNotification.getKey() + "] " +
                            removalNotification.getValue().getMetrics().stream().map(m -> {
                                visitor.visit(m);
                                return visitor.build();
                            }).collect(Collectors.joining(", ")));
                }).build();
    }

    public static TableMetricsRegistry getInstance() {
        return INSTANCE;
    }

    public synchronized void renameMetricsEntity(long tableId, String newTableName) {
        TableMetricsEntity metricsEntity = getMetricsEntity(tableId);
        if (metricsEntity == null) {
            return;
        }
        Optional.ofNullable(metricsEntity.getMetrics())
                .ifPresent(metrics -> metrics.forEach(metric -> metric.getLabels().stream().forEach(label -> {
                    if (label instanceof MetricLabel && ((MetricLabel) label).getKey().equalsIgnoreCase("tbl_name")) {
                        ((MetricLabel) label).setValue(newTableName);
                    }
                })));
    }

    public synchronized TableMetricsEntity getMetricsEntity(long tableId) {
        try {
            return idToTableMetrics.get(tableId, TableMetricsEntity::new);
        } catch (ExecutionException e) {
            LOG.error("Failed to obtain table [" + tableId + "] metrics!");
        }
        // Here suppose to be unreachable.
        return null;
    }
}

