// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.sql.optimizer.rewrite;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.HiveMetaStoreTable;
import com.starrocks.catalog.HivePartitionKey;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.connector.hive.THiveConstants;
import com.starrocks.connector.hive.THiveUtils;
import com.starrocks.planner.PartitionPruner;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.ScanOperatorPredicates;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.transformation.ListPartitionPruner;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class OptThivePartitionPruner {
    private static final Logger LOG = LogManager.getLogger(OptThivePartitionPruner.class);

    public static void thivePrunePartitions(LogicalScanOperator operator, OptimizerContext context)
            throws AnalysisException {
        Table table = operator.getTable();
        HiveMetaStoreTable hmsTable = (HiveMetaStoreTable) table;
        List<Column> partitionColumns = hmsTable.getPartitionColumns();

        String thivePartitionColumns =
                ((HiveTable) table).getHiveProperties().get(THiveConstants.THIVE_PARTITION_COLUMNS);
        String thivePartitionTypesStr =
                ((HiveTable) table).getHiveProperties().get(THiveConstants.THIVE_PARTITION_TYPES);

        List<String> thivePartitionTypes = new ArrayList<>();
        if (StringUtils.isEmpty(thivePartitionColumns)) {
            partitionColumns = Lists.newArrayList();
        } else {
            String[] splits = thivePartitionColumns.split(THiveConstants.SEPARATOR);
            partitionColumns = new ArrayList<>(splits.length);
            for (String split : splits) {
                partitionColumns.add(table.getColumn(split));
            }
            for (String split : thivePartitionTypesStr.split(THiveConstants.SEPARATOR)) {
                thivePartitionTypes.add(split);
            }
        }

        if (hmsTable.isUnPartitioned()) {
            long partitionId = 0;
            PartitionKey key = new PartitionKey();
            operator.getScanOperatorPredicates().getIdToPartitionKey().put(partitionId, key);

            ScanOperatorPredicates scanOperatorPredicates = operator.getScanOperatorPredicates();
            scanOperatorPredicates.setSelectedPartitionIds(scanOperatorPredicates.getIdToPartitionKey().keySet());
            OptExternalPartitionPruner.computeMinMaxConjuncts(operator, context);
            addConjunctsForThive(operator);
        } else {
            // one level
            if (partitionColumns.size() == 1) {
                if (thivePartitionTypes.get(0).equalsIgnoreCase(THiveConstants.LIST)) {
                    oneLevelListPrunePartitions(operator, context, hmsTable, partitionColumns);
                } else if (thivePartitionTypes.get(0).equalsIgnoreCase(THiveConstants.RANGE)) {
                    oneLevelRangePrunePartitions(operator, context, hmsTable, partitionColumns);
                } else {
                    oneLevelHashPrunePartitions(operator, context, hmsTable, partitionColumns);
                }
            } else if (partitionColumns.size() == 2) {
                // two level
                String level1ThivePartitionType = thivePartitionTypes.get(0);
                String level2ThivePartitionType = thivePartitionTypes.get(1);
                if (level1ThivePartitionType.equalsIgnoreCase(THiveConstants.LIST)) {
                    if (level2ThivePartitionType.equalsIgnoreCase(THiveConstants.LIST)) {
                        twoLevelListListPrunePartitions(operator, context, hmsTable, partitionColumns);
                    } else if (level2ThivePartitionType.equalsIgnoreCase(THiveConstants.RANGE)) {
                        twoLevelListRangePrunePartitions(operator, context, hmsTable, partitionColumns);
                    } else if (level2ThivePartitionType.equalsIgnoreCase(THiveConstants.HASH)) {
                        twoLevelListHashPrunePartitions(operator, context, hmsTable, partitionColumns);
                    }
                } else if (level1ThivePartitionType.equalsIgnoreCase(THiveConstants.RANGE)) {
                    if (level2ThivePartitionType.equalsIgnoreCase(THiveConstants.LIST)) {
                        twoLevelRangeListPrunePartitions(operator, context, hmsTable, partitionColumns);
                    } else if (level2ThivePartitionType.equalsIgnoreCase(THiveConstants.RANGE)) {
                        twoLevelRangeRangePrunePartitions(operator, context, hmsTable, partitionColumns);
                    } else if (level2ThivePartitionType.equalsIgnoreCase(THiveConstants.HASH)) {
                        twoLevelRangeHashPrunePartitions(operator, context, hmsTable, partitionColumns);
                    }
                } else if (level1ThivePartitionType.equalsIgnoreCase(THiveConstants.HASH)) {
                    String errorMessage = "This is not possible, only the second partition can be hash type " + table;
                    LOG.warn(errorMessage);
                    throw new StarRocksPlannerException(errorMessage, ErrorType.UNSUPPORTED);
                }
            }
        }
    }

    /**
     CREATE TABLE test_thive_list_partition_table(
     id INT,
     dt STRING,
     name STRING
     )
     PARTITION BY LIST( id )
     (
     PARTITION p_2001 VALUES IN ( 2000, 2001 ),
     PARTITION p_2011 VALUES IN ( 2010, 2011),
     PARTITION p_2021 VALUES IN ( 2020, 2021),
     PARTITION default
     )
     STORED AS ORCFILE;

     partition location is
     hdfs://ss-teg/user/tdw/warehouse/test.db/test_thive_list_partition_table/default
     hdfs://ss-teg/user/tdw/warehouse/test.db/test_thive_list_partition_table/p_2001
     hdfs://ss-teg/user/tdw/warehouse/test.db/test_thive_list_partition_table/p_2011
     hdfs://ss-teg/user/tdw/warehouse/test.db/test_thive_list_partition_table/p_2021
     */
    public static void oneLevelListPrunePartitions(LogicalScanOperator operator, OptimizerContext context,
                                                   HiveMetaStoreTable hmsTable,
                                                   List<Column> partitionColumns)
            throws AnalysisException {
        // only one partition key
        Column partitionColumn = partitionColumns.get(0);
        Map<String, List<String>> partitionNameToPartitionValues = getPartitionValues(hmsTable, partitionColumn);

        Map<PartitionKey, Long> partitionKeys = Maps.newHashMap();
        // partitionColumnName -> (LiteralExpr -> partition ids)
        Map<ColumnRefOperator, TreeMap<LiteralExpr, Set<Long>>> columnToPartitionValuesMap = Maps.newHashMap();
        // partitionColumnName -> null partitionIds
        Map<ColumnRefOperator, Set<Long>> columnToNullPartitions = Maps.newHashMap();
        List<ColumnRefOperator> partitionColumnRefOperators = new ArrayList<>();
        for (Column column : partitionColumns) {
            ColumnRefOperator partitionColumnRefOperator = operator.getColumnReference(column);
            columnToPartitionValuesMap.put(partitionColumnRefOperator, new TreeMap<>());
            columnToNullPartitions.put(partitionColumnRefOperator, Sets.newHashSet());
            partitionColumnRefOperators.add(partitionColumnRefOperator);
        }

        long partitionId = 0;
        Set<Long> defaultPartitionIds = new HashSet<>();
        for (Map.Entry<String, List<String>> entry : partitionNameToPartitionValues.entrySet()) {
            PartitionKey partitionKey = new HivePartitionKey();
            partitionKey.pushColumn(LiteralExpr.create(entry.getKey(), Type.STRING), PrimitiveType.VARCHAR);
            partitionKeys.put(partitionKey, partitionId);

            ColumnRefOperator columnRefOperator = partitionColumnRefOperators.get(0);

            // exclude default partition
            if (!entry.getKey().equalsIgnoreCase(THiveConstants.DEFAULT)) {
                for (String rawValue : entry.getValue()) {
                    LiteralExpr literal = LiteralExpr.create(rawValue, partitionColumn.getType());
                    Set<Long> partitions = columnToPartitionValuesMap.get(columnRefOperator)
                            .computeIfAbsent(literal, k -> Sets.newHashSet());
                    partitions.add(partitionId);
                }
            } else {
                defaultPartitionIds.add(partitionId);
            }
            operator.getScanOperatorPredicates().getIdToPartitionKey().put(partitionId, partitionKey);
            partitionId++;
        }

        thiveClassifyConjuncts(operator, columnToPartitionValuesMap);
        thiveComputePartitionInfo(operator, columnToPartitionValuesMap, columnToNullPartitions, defaultPartitionIds);
        OptExternalPartitionPruner.computeMinMaxConjuncts(operator, context);
        addConjunctsForThive(operator);
    }

    /**
     CREATE TABLE test_thive_range_partition_table(
     id INT,
     name STRING,
     cdouble DOUBLE
     )
     PARTITION BY RANGE( id )
     (
     PARTITION default,
     PARTITION p_5 VALUES LESS THAN ( 5 ),
     PARTITION p_10 VALUES LESS THAN ( 10 ),
     PARTITION p_15 VALUES LESS THAN ( 15 )
     );

     p_5: (-∞, 5),  p_10: [5, 10),  p_15: [10, 15), default: [15, +∞)

     partition location is
     hdfs://ss-teg/user/tdw/warehouse/test.db/test_thive_range_partition_table/default
     hdfs://ss-teg/user/tdw/warehouse/test.db/test_thive_range_partition_table/p_5
     hdfs://ss-teg/user/tdw/warehouse/test.db/test_thive_range_partition_table/p_10
     hdfs://ss-teg/user/tdw/warehouse/test.db/test_thive_range_partition_table/p_15
     */
    public static void oneLevelRangePrunePartitions(LogicalScanOperator operator, OptimizerContext context,
                                                    HiveMetaStoreTable hmsTable,
                                                    List<Column> partitionColumns)
            throws AnalysisException {
        // only one partition column
        Column partitionColumn = partitionColumns.get(0);

        Map<String, List<String>> partitionNameToPartitionValues = getPartitionValues(hmsTable, partitionColumn);

        List<LiteralExpr> values = new ArrayList<>();
        for (Map.Entry<String, List<String>> entry : partitionNameToPartitionValues.entrySet()) {
            // exclude default partition
            if (!entry.getKey().equalsIgnoreCase(THiveConstants.DEFAULT)) {
                // only one value
                String rawValue = entry.getValue().get(0);
                values.add(LiteralExpr.create(rawValue, partitionColumn.getType()));
            }
        }
        Collections.sort(values);

        Map<Long, Range<PartitionKey>> keyRangeById = Maps.newHashMap();
        long partitionId = 0;
        for (Map.Entry<String, List<String>> entry : partitionNameToPartitionValues.entrySet()) {
            PartitionKey partitionKey = new HivePartitionKey();
            partitionKey.pushColumn(LiteralExpr.create(entry.getKey(), Type.STRING), PrimitiveType.VARCHAR);

            if (!entry.getKey().equalsIgnoreCase(THiveConstants.DEFAULT)) {
                // only one value
                String rawValue = entry.getValue().get(0);
                LiteralExpr literal = LiteralExpr.create(rawValue, partitionColumn.getType());
                int index = values.indexOf(literal);
                if (index == 0) {
                    PartitionKey upperBound = new PartitionKey();
                    upperBound.pushColumn(values.get(0), partitionColumn.getType().getPrimitiveType());
                    keyRangeById.put(partitionId, Range.lessThan(upperBound));
                } else {
                    PartitionKey lowerBound = new PartitionKey();
                    lowerBound.pushColumn(values.get(index - 1), partitionColumn.getType().getPrimitiveType());
                    PartitionKey upperBound = new PartitionKey();
                    upperBound.pushColumn(values.get(index), partitionColumn.getType().getPrimitiveType());
                    keyRangeById.put(partitionId, Range.closedOpen(lowerBound, upperBound));
                }
            } else {
                if (values.size() > 1) {
                    PartitionKey lowerBound = new PartitionKey();
                    lowerBound.pushColumn(values.get(values.size() - 1), partitionColumn.getType().getPrimitiveType());
                    keyRangeById.put(partitionId, Range.atLeast(lowerBound));
                } else {
                    // only one partition and it is default.
                    keyRangeById.put(partitionId, Range.all());
                }
            }
            operator.getScanOperatorPredicates().getIdToPartitionKey().put(partitionId, partitionKey);
            partitionId++;
        }

        ThiveRangePartitionPruner partitionPruner = new ThiveRangePartitionPruner(keyRangeById,
                partitionColumns, operator.getColumnFilters());

        ScanOperatorPredicates scanOperatorPredicates = operator.getScanOperatorPredicates();
        Collection<Long> selectedPartitionIds = partitionPruner.prune();
        if (selectedPartitionIds == null) {
            selectedPartitionIds = scanOperatorPredicates.getIdToPartitionKey().keySet();
        }
        scanOperatorPredicates.setSelectedPartitionIds(selectedPartitionIds);
        addConjunctsForThive(operator);
    }

    /**
     create table test_thive_hash_partition_table (id int, name STRING) partition by hashkey(id);

     partition location is
     hdfs://ss-teg/user/tdw/warehouse/test.db/test_thive_hash_partition_table/Hash_0000
     hdfs://ss-teg/user/tdw/warehouse/test.db/test_thive_hash_partition_table/Hash_0001
     xxx
     hdfs://ss-teg/user/tdw/warehouse/test.db/test_thive_hash_partition_table/Hash_0498
     hdfs://ss-teg/user/tdw/warehouse/test.db/test_thive_hash_partition_table/Hash_0499
     */
    public static void oneLevelHashPrunePartitions(LogicalScanOperator operator, OptimizerContext context,
                                                   HiveMetaStoreTable hmsTable,
                                                   List<Column> partitionColumns)
            throws AnalysisException {
        Column partitionColumn = partitionColumns.get(0);
        Map<String, List<String>> partitionNameToPartitionValues = getPartitionValues(hmsTable, partitionColumn);

        long partitionId = 0;
        for (Map.Entry<String, List<String>> entry : partitionNameToPartitionValues.entrySet()) {
            PartitionKey partitionKey = new HivePartitionKey();
            partitionKey.pushColumn(LiteralExpr.create(entry.getKey(), Type.STRING), PrimitiveType.VARCHAR);

            operator.getScanOperatorPredicates().getIdToPartitionKey().put(partitionId, partitionKey);
            partitionId++;
        }
        ScanOperatorPredicates scanOperatorPredicates = operator.getScanOperatorPredicates();
        scanOperatorPredicates.setSelectedPartitionIds(
                operator.getScanOperatorPredicates().getIdToPartitionKey().keySet());
        addConjunctsForThive(operator);
    }

    public static void addConjunctsForThive(LogicalScanOperator operator) {
        // add all Conjuncts to nonPartitionConjuncts for thive
        for (ScalarOperator scalarOperator : Utils.extractConjuncts(operator.getPredicate())) {
            operator.getScanOperatorPredicates().getNonPartitionConjuncts().add(scalarOperator);
        }
    }

    /**
     * Get all values of the specified partition field
     */
    public static Map<String, List<String>> getPartitionValues(HiveMetaStoreTable hmsTable, Column partitionColumn) {
        return GlobalStateMgr.getCurrentState().getMetadataMgr().getPartitionValues(
                hmsTable.getCatalogName(), hmsTable.getDbName(), hmsTable.getTableName(),
                THiveUtils.toTHivePartitionKey(partitionColumn.getName()));
    }

    public static void thiveClassifyConjuncts(LogicalScanOperator operator,
                                              Map<ColumnRefOperator, TreeMap<LiteralExpr, Set<Long>>> columnToPartitionValuesMap)
            throws AnalysisException {
        for (ScalarOperator scalarOperator : Utils.extractConjuncts(operator.getPredicate())) {
            List<ColumnRefOperator> columnRefOperatorList = Utils.extractColumnRef(scalarOperator);
            if (!columnRefOperatorList.retainAll(columnToPartitionValuesMap.keySet())) {
                operator.getScanOperatorPredicates().getPartitionConjuncts().add(scalarOperator);
            }
        }
    }

    private static void thiveComputePartitionInfo(LogicalScanOperator operator,
                                                  Map<ColumnRefOperator, TreeMap<LiteralExpr, Set<Long>>> columnToPartitionValues,
                                                  Map<ColumnRefOperator, Set<Long>> columnToNullPartitions,
                                                  Set<Long> defaultPartitionIds) throws AnalysisException {
        ScanOperatorPredicates scanOperatorPredicates = operator.getScanOperatorPredicates();
        ListPartitionPruner partitionPruner = new ListPartitionPruner(columnToPartitionValues, columnToNullPartitions,
                scanOperatorPredicates.getPartitionConjuncts());
        Collection<Long> selectedPartitionIds = partitionPruner.prune();
        if (selectedPartitionIds == null) {
            selectedPartitionIds = scanOperatorPredicates.getIdToPartitionKey().keySet();
        }
        if (defaultPartitionIds != null && defaultPartitionIds.size() > 0) {
            Set<Long> allPartitionIds = new HashSet<>();
            allPartitionIds.addAll(selectedPartitionIds);
            allPartitionIds.addAll(defaultPartitionIds);
            scanOperatorPredicates.setSelectedPartitionIds(allPartitionIds);
        } else {
            scanOperatorPredicates.setSelectedPartitionIds(selectedPartitionIds);
        }
        //scanOperatorPredicates.getNoEvalPartitionConjuncts().addAll(partitionPruner.getNoEvalConjuncts());
    }

    public static void twoLevelListListPrunePartitions(LogicalScanOperator operator, OptimizerContext context,
                                                       HiveMetaStoreTable hmsTable,
                                                       List<Column> partitionColumns)
            throws AnalysisException {
        Map<String, List<String>> level1PartitionNameToPartitionValues =
                getPartitionValues(hmsTable, partitionColumns.get(0));

        Map<String, List<String>> level2PartitionNameToPartitionValues =
                getPartitionValues(hmsTable, partitionColumns.get(1));

        Map<PartitionKey, Long> partitionKeys = Maps.newHashMap();
        // partitionColumnName -> (LiteralExpr -> partition ids)
        Map<ColumnRefOperator, TreeMap<LiteralExpr, Set<Long>>> columnToPartitionValuesMap = Maps.newHashMap();
        // partitionColumnName -> null partitionIds
        Map<ColumnRefOperator, Set<Long>> columnToNullPartitions = Maps.newHashMap();
        List<ColumnRefOperator> partitionColumnRefOperators = new ArrayList<>();
        for (Column column : partitionColumns) {
            ColumnRefOperator partitionColumnRefOperator = operator.getColumnReference(column);
            columnToPartitionValuesMap.put(partitionColumnRefOperator, new TreeMap<>());
            columnToNullPartitions.put(partitionColumnRefOperator, Sets.newHashSet());
            partitionColumnRefOperators.add(partitionColumnRefOperator);
        }

        long partitionId = 0;
        Set<Long> defaultPartitionIds = new HashSet<>();
        for (Map.Entry<String, List<String>> level1 : level1PartitionNameToPartitionValues.entrySet()) {
            for (Map.Entry<String, List<String>> level2 : level2PartitionNameToPartitionValues.entrySet()) {
                PartitionKey partitionKey = new HivePartitionKey();
                partitionKey.pushColumn(LiteralExpr.create(level1.getKey(), Type.STRING), PrimitiveType.VARCHAR);
                partitionKey.pushColumn(LiteralExpr.create(level2.getKey(), Type.STRING), PrimitiveType.VARCHAR);
                partitionKeys.put(partitionKey, partitionId);
                LOG.debug(hmsTable.getTableName() + " partitionId = " + partitionId + ", level1 = " + level1 +
                        ", level2 = " + level2);

                if (!level1.getKey().equalsIgnoreCase(THiveConstants.DEFAULT)) {
                    ColumnRefOperator columnRefOperator = partitionColumnRefOperators.get(0);
                    for (String rawValue : level1.getValue()) {
                        LiteralExpr literal = LiteralExpr.create(rawValue, partitionColumns.get(0).getType());
                        Set<Long> partitions = columnToPartitionValuesMap.get(columnRefOperator)
                                .computeIfAbsent(literal, k -> Sets.newHashSet());
                        partitions.add(partitionId);
                    }
                } else {
                    defaultPartitionIds.add(partitionId);
                }

                if (!level2.getKey().equalsIgnoreCase(THiveConstants.DEFAULT)) {
                    ColumnRefOperator columnRefOperator = partitionColumnRefOperators.get(1);
                    for (String rawValue : level2.getValue()) {
                        LiteralExpr literal = LiteralExpr.create(rawValue, partitionColumns.get(1).getType());
                        Set<Long> partitions = columnToPartitionValuesMap.get(columnRefOperator)
                                .computeIfAbsent(literal, k -> Sets.newHashSet());
                        partitions.add(partitionId);
                    }
                } else {
                    defaultPartitionIds.add(partitionId);
                }
                operator.getScanOperatorPredicates().getIdToPartitionKey().put(partitionId, partitionKey);
                partitionId++;
            }
        }

        thiveClassifyConjuncts(operator, columnToPartitionValuesMap);
        thiveComputePartitionInfo(operator, columnToPartitionValuesMap, columnToNullPartitions, defaultPartitionIds);
        addConjunctsForThive(operator);
    }

    public static void twoLevelListRangePrunePartitions(LogicalScanOperator operator, OptimizerContext context,
                                                        HiveMetaStoreTable hmsTable,
                                                        List<Column> partitionColumns)
            throws AnalysisException {
        Map<String, List<String>> level1PartitionNameToPartitionValues =
                getPartitionValues(hmsTable, partitionColumns.get(0));

        Map<String, List<String>> level2PartitionNameToPartitionValues =
                getPartitionValues(hmsTable, partitionColumns.get(1));

        Map<PartitionKey, Long> partitionKeys = Maps.newHashMap();
        // partitionColumnName -> (LiteralExpr -> partition ids)
        Map<ColumnRefOperator, TreeMap<LiteralExpr, Set<Long>>> columnToPartitionValuesMap = Maps.newHashMap();
        // partitionColumnName -> null partitionIds
        Map<ColumnRefOperator, Set<Long>> columnToNullPartitions = Maps.newHashMap();
        List<ColumnRefOperator> partitionColumnRefOperators = new ArrayList<>();

        Column listPartitionColumn = partitionColumns.get(0);
        ColumnRefOperator partitionColumnRefOperator = operator.getColumnReference(listPartitionColumn);
        columnToPartitionValuesMap.put(partitionColumnRefOperator, new TreeMap<>());
        columnToNullPartitions.put(partitionColumnRefOperator, Sets.newHashSet());
        partitionColumnRefOperators.add(partitionColumnRefOperator);

        long partitionId = 0;
        Set<Long> level1ListDefaultPartitionIds = new HashSet<>();
        Map<Long, String> level2RangePartitionNamesById = Maps.newHashMap();
        for (Map.Entry<String, List<String>> level1 : level1PartitionNameToPartitionValues.entrySet()) {
            for (Map.Entry<String, List<String>> level2 : level2PartitionNameToPartitionValues.entrySet()) {
                PartitionKey partitionKey = new HivePartitionKey();
                partitionKey.pushColumn(LiteralExpr.create(level1.getKey(), Type.STRING), PrimitiveType.VARCHAR);
                partitionKey.pushColumn(LiteralExpr.create(level2.getKey(), Type.STRING), PrimitiveType.VARCHAR);
                partitionKeys.put(partitionKey, partitionId);
                level2RangePartitionNamesById.put(partitionId, level2.getKey());
                LOG.debug(hmsTable.getTableName() + " partitionId = " + partitionId + ", level1 = " + level1 +
                        ", level2 = " + level2);

                if (!level1.getKey().equalsIgnoreCase(THiveConstants.DEFAULT)) {
                    ColumnRefOperator columnRefOperator = partitionColumnRefOperators.get(0);
                    for (String rawValue : level1.getValue()) {
                        LiteralExpr literal = LiteralExpr.create(rawValue, listPartitionColumn.getType());
                        Set<Long> partitions = columnToPartitionValuesMap.get(columnRefOperator)
                                .computeIfAbsent(literal, k -> Sets.newHashSet());
                        partitions.add(partitionId);
                    }
                } else {
                    level1ListDefaultPartitionIds.add(partitionId);
                }
                operator.getScanOperatorPredicates().getIdToPartitionKey().put(partitionId, partitionKey);
                partitionId++;
            }
        }

        thiveClassifyConjuncts(operator, columnToPartitionValuesMap);

        ScanOperatorPredicates scanOperatorPredicates = operator.getScanOperatorPredicates();
        ListPartitionPruner partitionPruner =
                new ListPartitionPruner(columnToPartitionValuesMap, columnToNullPartitions,
                        scanOperatorPredicates.getPartitionConjuncts());
        Collection<Long> selectedPartitionIds = partitionPruner.prune();
        if (selectedPartitionIds == null) {
            selectedPartitionIds = scanOperatorPredicates.getIdToPartitionKey().keySet();
        }
        LOG.debug(hmsTable.getTableName() + " level1 listPartitionPruner selectedPartitionIds = " +
                selectedPartitionIds);

        Set<String> selectLevel2RangePartitionNames = rangePrunePartitions(operator, context, hmsTable,
                level2PartitionNameToPartitionValues, partitionColumns.get(1));
        LOG.debug(hmsTable.getTableName() + " level2 rangePrune select partitionNames = " +
                selectLevel2RangePartitionNames);

        List<Long> matches = new ArrayList<>();
        for (Long entry : selectedPartitionIds) {
            if (selectLevel2RangePartitionNames.contains(level2RangePartitionNamesById.get(entry))) {
                matches.add(entry);
            }
        }

        // add default
        Set<Long> allPartitions = new HashSet<>();
        allPartitions.addAll(matches);
        allPartitions.addAll(level1ListDefaultPartitionIds);
        LOG.debug(hmsTable.getTableName() + " twoLevelListRangePrunePartitions selectedPartitionIds = " + matches +
                ", level1ListDefaultPartitionIds = " + level1ListDefaultPartitionIds);
        scanOperatorPredicates.setSelectedPartitionIds(allPartitions);

        addConjunctsForThive(operator);
    }

    public static void twoLevelListHashPrunePartitions(LogicalScanOperator operator, OptimizerContext context,
                                                       HiveMetaStoreTable hmsTable,
                                                       List<Column> partitionColumns)
            throws AnalysisException {
        Map<String, List<String>> level1PartitionNameToPartitionValues =
                getPartitionValues(hmsTable, partitionColumns.get(0));

        Map<String, List<String>> level2PartitionNameToPartitionValues =
                getPartitionValues(hmsTable, partitionColumns.get(1));

        Map<PartitionKey, Long> partitionKeys = Maps.newHashMap();
        // partitionColumnName -> (LiteralExpr -> partition ids)
        Map<ColumnRefOperator, TreeMap<LiteralExpr, Set<Long>>> columnToPartitionValuesMap = Maps.newHashMap();
        // partitionColumnName -> null partitionIds
        Map<ColumnRefOperator, Set<Long>> columnToNullPartitions = Maps.newHashMap();
        List<ColumnRefOperator> partitionColumnRefOperators = new ArrayList<>();

        Column listPartitionColumn = partitionColumns.get(0);
        ColumnRefOperator partitionColumnRefOperator = operator.getColumnReference(listPartitionColumn);
        columnToPartitionValuesMap.put(partitionColumnRefOperator, new TreeMap<>());
        columnToNullPartitions.put(partitionColumnRefOperator, Sets.newHashSet());
        partitionColumnRefOperators.add(partitionColumnRefOperator);

        long partitionId = 0;
        Set<Long> level1ListDefaultPartitionIds = new HashSet<>();
        Map<Long, String> level2RangePartitionNamesById = Maps.newHashMap();
        for (Map.Entry<String, List<String>> level1 : level1PartitionNameToPartitionValues.entrySet()) {
            for (Map.Entry<String, List<String>> level2 : level2PartitionNameToPartitionValues.entrySet()) {
                PartitionKey partitionKey = new HivePartitionKey();
                partitionKey.pushColumn(LiteralExpr.create(level1.getKey(), Type.STRING), PrimitiveType.VARCHAR);
                partitionKey.pushColumn(LiteralExpr.create(level2.getKey(), Type.STRING), PrimitiveType.VARCHAR);
                partitionKeys.put(partitionKey, partitionId);
                level2RangePartitionNamesById.put(partitionId, level2.getKey());
                LOG.debug(hmsTable.getTableName() + " partitionId = " + partitionId + ", level1 = " + level1 +
                        ", level2 = " + level2);

                if (!level1.getKey().equalsIgnoreCase(THiveConstants.DEFAULT)) {
                    ColumnRefOperator columnRefOperator = partitionColumnRefOperators.get(0);
                    for (String rawValue : level1.getValue()) {
                        LiteralExpr literal = LiteralExpr.create(rawValue, listPartitionColumn.getType());
                        Set<Long> partitions = columnToPartitionValuesMap.get(columnRefOperator)
                                .computeIfAbsent(literal, k -> Sets.newHashSet());
                        partitions.add(partitionId);
                    }
                } else {
                    level1ListDefaultPartitionIds.add(partitionId);
                }
                operator.getScanOperatorPredicates().getIdToPartitionKey().put(partitionId, partitionKey);
                partitionId++;
            }
        }

        thiveClassifyConjuncts(operator, columnToPartitionValuesMap);

        ScanOperatorPredicates scanOperatorPredicates = operator.getScanOperatorPredicates();
        ListPartitionPruner partitionPruner =
                new ListPartitionPruner(columnToPartitionValuesMap, columnToNullPartitions,
                        scanOperatorPredicates.getPartitionConjuncts());
        Collection<Long> selectedPartitionIds = partitionPruner.prune();
        if (selectedPartitionIds == null) {
            selectedPartitionIds = scanOperatorPredicates.getIdToPartitionKey().keySet();
        }
        LOG.debug(hmsTable.getTableName() + " level1 ListPartitionPruner selectedPartitionIds = " + selectedPartitionIds);
        // add default
        Set<Long> allPartitions = new HashSet<>();
        allPartitions.addAll(selectedPartitionIds);
        allPartitions.addAll(level1ListDefaultPartitionIds);
        LOG.debug(hmsTable.getTableName() + " twoLevelListHashPrunePartitions selectedPartitionIds = " +
                selectedPartitionIds + ", level1ListDefaultPartitionIds = " + level1ListDefaultPartitionIds);
        scanOperatorPredicates.setSelectedPartitionIds(allPartitions);

        addConjunctsForThive(operator);
    }

    public static Set<String> rangePrunePartitions(LogicalScanOperator operator, OptimizerContext context,
                                                   HiveMetaStoreTable hmsTable,
                                                   Map<String, List<String>> partitionNameToPartitionValues,
                                                   Column rangePartitionColumn)
            throws AnalysisException {
        // range prune
        List<LiteralExpr> values = new ArrayList<>();
        for (Map.Entry<String, List<String>> entry : partitionNameToPartitionValues.entrySet()) {
            // exclude default partition
            if (!entry.getKey().equalsIgnoreCase(THiveConstants.DEFAULT)) {
                // only one value
                String rawValue = entry.getValue().get(0);
                values.add(LiteralExpr.create(rawValue, rangePartitionColumn.getType()));
            }
        }
        Collections.sort(values);

        Map<Long, Range<PartitionKey>> keyRangeById = Maps.newHashMap();
        Map<Long, String> partitionNameById = Maps.newHashMap();
        long partitionId = 0;

        for (Map.Entry<String, List<String>> entry : partitionNameToPartitionValues.entrySet()) {
            PartitionKey partitionKey = new HivePartitionKey();
            partitionKey.pushColumn(LiteralExpr.create(entry.getKey(), Type.STRING), PrimitiveType.VARCHAR);
            partitionNameById.put(partitionId, entry.getKey());

            if (!entry.getKey().equalsIgnoreCase(THiveConstants.DEFAULT)) {
                // only one value
                String rawValue = entry.getValue().get(0);
                LiteralExpr literal = LiteralExpr.create(rawValue, rangePartitionColumn.getType());
                int index = values.indexOf(literal);
                if (index == 0) {
                    PartitionKey upperBound = new PartitionKey();
                    upperBound.pushColumn(values.get(0), rangePartitionColumn.getType().getPrimitiveType());
                    keyRangeById.put(partitionId, Range.lessThan(upperBound));
                } else {
                    PartitionKey lowerBound = new PartitionKey();
                    lowerBound.pushColumn(values.get(index - 1), rangePartitionColumn.getType().getPrimitiveType());
                    PartitionKey upperBound = new PartitionKey();
                    upperBound.pushColumn(values.get(index), rangePartitionColumn.getType().getPrimitiveType());
                    keyRangeById.put(partitionId, Range.closedOpen(lowerBound, upperBound));
                }
            } else {
                if (values.size() > 1) {
                    PartitionKey lowerBound = new PartitionKey();
                    lowerBound.pushColumn(values.get(values.size() - 1),
                            rangePartitionColumn.getType().getPrimitiveType());
                    keyRangeById.put(partitionId, Range.atLeast(lowerBound));
                } else {
                    // only one partition and it is default.
                    keyRangeById.put(partitionId, Range.all());
                }
            }
            //operator.getScanOperatorPredicates().getIdToPartitionKey().put(partitionId, partitionKey);
            partitionId++;
        }
        LOG.debug("keyRangeById = " + keyRangeById);

        List<Column> partitionColumns = new ArrayList<>();
        partitionColumns.add(rangePartitionColumn);
        PartitionPruner partitionPruner = new ThiveRangePartitionPruner(keyRangeById,
                partitionColumns, operator.getColumnFilters());

        Collection<Long> selectedPartitionIds = partitionPruner.prune();
        LOG.debug("rangePrunePartitions selectedPartitionIds = " + selectedPartitionIds);
        if (selectedPartitionIds == null) {
            return new HashSet<>(partitionNameById.values());
        } else {
            Set set = new HashSet<>();
            for (Long entry : selectedPartitionIds) {
                set.add(partitionNameById.get(entry));
            }
            return set;
        }
    }

    public static void twoLevelRangeListPrunePartitions(LogicalScanOperator operator, OptimizerContext context,
                                                        HiveMetaStoreTable hmsTable,
                                                        List<Column> partitionColumns)
            throws AnalysisException {
        Map<String, List<String>> level1PartitionNameToPartitionValues =
                getPartitionValues(hmsTable, partitionColumns.get(0));

        Map<String, List<String>> level2PartitionNameToPartitionValues =
                getPartitionValues(hmsTable, partitionColumns.get(1));

        Map<PartitionKey, Long> partitionKeys = Maps.newHashMap();
        // partitionColumnName -> (LiteralExpr -> partition ids)
        Map<ColumnRefOperator, TreeMap<LiteralExpr, Set<Long>>> columnToPartitionValuesMap = Maps.newHashMap();
        // partitionColumnName -> null partitionIds
        Map<ColumnRefOperator, Set<Long>> columnToNullPartitions = Maps.newHashMap();
        List<ColumnRefOperator> partitionColumnRefOperators = new ArrayList<>();

        Column listPartitionColumn = partitionColumns.get(1);
        ColumnRefOperator partitionColumnRefOperator = operator.getColumnReference(listPartitionColumn);
        columnToPartitionValuesMap.put(partitionColumnRefOperator, new TreeMap<>());
        columnToNullPartitions.put(partitionColumnRefOperator, Sets.newHashSet());
        partitionColumnRefOperators.add(partitionColumnRefOperator);

        long partitionId = 0;
        Set<Long> level2ListDefaultPartitionIds = new HashSet<>();
        Map<Long, String> level1RangePartitionNamesById = Maps.newHashMap();
        for (Map.Entry<String, List<String>> level1 : level1PartitionNameToPartitionValues.entrySet()) {
            for (Map.Entry<String, List<String>> level2 : level2PartitionNameToPartitionValues.entrySet()) {
                PartitionKey partitionKey = new HivePartitionKey();
                partitionKey.pushColumn(LiteralExpr.create(level1.getKey(), Type.STRING), PrimitiveType.VARCHAR);
                partitionKey.pushColumn(LiteralExpr.create(level2.getKey(), Type.STRING), PrimitiveType.VARCHAR);
                partitionKeys.put(partitionKey, partitionId);
                level1RangePartitionNamesById.put(partitionId, level1.getKey());
                LOG.debug(hmsTable.getTableName() + " partitionId = " + partitionId + ", level1 = " + level1 +
                        ", level2 = " + level2);

                if (!level2.getKey().equalsIgnoreCase(THiveConstants.DEFAULT)) {
                    ColumnRefOperator columnRefOperator = partitionColumnRefOperators.get(0);
                    for (String rawValue : level2.getValue()) {
                        LiteralExpr literal = LiteralExpr.create(rawValue, listPartitionColumn.getType());
                        Set<Long> partitions = columnToPartitionValuesMap.get(columnRefOperator)
                                .computeIfAbsent(literal, k -> Sets.newHashSet());
                        partitions.add(partitionId);
                    }
                } else {
                    level2ListDefaultPartitionIds.add(partitionId);
                }
                operator.getScanOperatorPredicates().getIdToPartitionKey().put(partitionId, partitionKey);
                partitionId++;
            }
        }

        thiveClassifyConjuncts(operator, columnToPartitionValuesMap);

        ScanOperatorPredicates scanOperatorPredicates = operator.getScanOperatorPredicates();
        ListPartitionPruner partitionPruner =
                new ListPartitionPruner(columnToPartitionValuesMap, columnToNullPartitions,
                        scanOperatorPredicates.getPartitionConjuncts());
        Collection<Long> selectedPartitionIds = partitionPruner.prune();
        if (selectedPartitionIds == null) {
            selectedPartitionIds = scanOperatorPredicates.getIdToPartitionKey().keySet();
        }
        LOG.debug(hmsTable.getTableName() + " level2 ListPartitionPruner selectedPartitionIds = " +
                selectedPartitionIds);

        Set<String> selectLevel1RangePartitionNames =
                rangePrunePartitions(operator, context, hmsTable, level1PartitionNameToPartitionValues,
                        partitionColumns.get(0));
        LOG.debug(hmsTable.getTableName() + " level1 selectRangePartitionNames = " +
                selectLevel1RangePartitionNames);

        List<Long> matches = new ArrayList<>();
        for (Long entry : selectedPartitionIds) {
            if (selectLevel1RangePartitionNames.contains(level1RangePartitionNamesById.get(entry))) {
                matches.add(entry);
            }
        }

        // add default
        Set<Long> allPartitions = new HashSet<>();
        allPartitions.addAll(matches);
        allPartitions.addAll(level2ListDefaultPartitionIds);
        LOG.debug(hmsTable.getTableName() + " twoLevelRangeListPrunePartitions selectedPartitionIds = " +
                allPartitions);
        scanOperatorPredicates.setSelectedPartitionIds(allPartitions);

        addConjunctsForThive(operator);
    }

    public static void twoLevelRangeRangePrunePartitions(LogicalScanOperator operator, OptimizerContext context,
                                                         HiveMetaStoreTable hmsTable,
                                                         List<Column> partitionColumns)
            throws AnalysisException {
        Map<String, List<String>> level1PartitionNameToPartitionValues =
                getPartitionValues(hmsTable, partitionColumns.get(0));

        Map<String, List<String>> level2PartitionNameToPartitionValues =
                getPartitionValues(hmsTable, partitionColumns.get(1));

        long partitionId = 0;
        Map<Long, String> level1RangePartitionNamesById = Maps.newHashMap();
        Map<Long, String> level2RangePartitionNamesById = Maps.newHashMap();
        for (Map.Entry<String, List<String>> level1 : level1PartitionNameToPartitionValues.entrySet()) {
            for (Map.Entry<String, List<String>> level2 : level2PartitionNameToPartitionValues.entrySet()) {
                PartitionKey partitionKey = new HivePartitionKey();
                partitionKey.pushColumn(LiteralExpr.create(level1.getKey(), Type.STRING), PrimitiveType.VARCHAR);
                partitionKey.pushColumn(LiteralExpr.create(level2.getKey(), Type.STRING), PrimitiveType.VARCHAR);
                level1RangePartitionNamesById.put(partitionId, level1.getKey());
                level2RangePartitionNamesById.put(partitionId, level2.getKey());
                LOG.debug(hmsTable.getTableName() + " partitionId = " + partitionId + ", level1 = " + level1 +
                        ", level2 = " + level2);

                operator.getScanOperatorPredicates().getIdToPartitionKey().put(partitionId, partitionKey);
                partitionId++;
            }
        }
        Set<String> selectLevel1RangePartitionNames =
                rangePrunePartitions(operator, context, hmsTable, level1PartitionNameToPartitionValues,
                        partitionColumns.get(0));
        LOG.debug(hmsTable.getTableName() + " level1 selectRangePartitionNames = " + selectLevel1RangePartitionNames);

        Set<String> selectLevel2RangePartitionNames =
                rangePrunePartitions(operator, context, hmsTable, level2PartitionNameToPartitionValues,
                        partitionColumns.get(1));
        LOG.debug(hmsTable.getTableName() + " level2 selectRangePartitionNames = " + selectLevel2RangePartitionNames);

        List<Long> matches = new ArrayList<>();
        for (Long entry : level1RangePartitionNamesById.keySet()) {
            if (selectLevel1RangePartitionNames.contains(level1RangePartitionNamesById.get(entry)) &&
                    selectLevel2RangePartitionNames.contains(level2RangePartitionNamesById.get(entry))) {
                matches.add(entry);
            }
        }
        LOG.debug(hmsTable.getTableName() + " twoLevelRangeRangePrunePartitions selectedPartitionIds = " + matches);
        ScanOperatorPredicates scanOperatorPredicates = operator.getScanOperatorPredicates();
        scanOperatorPredicates.setSelectedPartitionIds(matches);

        addConjunctsForThive(operator);
    }

    public static void twoLevelRangeHashPrunePartitions(LogicalScanOperator operator, OptimizerContext context,
                                                        HiveMetaStoreTable hmsTable,
                                                        List<Column> partitionColumns)
            throws AnalysisException {
        Map<String, List<String>> level1PartitionNameToPartitionValues =
                getPartitionValues(hmsTable, partitionColumns.get(0));

        Map<String, List<String>> level2PartitionNameToPartitionValues =
                getPartitionValues(hmsTable, partitionColumns.get(1));

        long partitionId = 0;
        Map<Long, String> level1RangePartitionNamesById = Maps.newHashMap();
        for (Map.Entry<String, List<String>> level1 : level1PartitionNameToPartitionValues.entrySet()) {
            for (Map.Entry<String, List<String>> level2 : level2PartitionNameToPartitionValues.entrySet()) {
                PartitionKey partitionKey = new HivePartitionKey();
                partitionKey.pushColumn(LiteralExpr.create(level1.getKey(), Type.STRING), PrimitiveType.VARCHAR);
                partitionKey.pushColumn(LiteralExpr.create(level2.getKey(), Type.STRING), PrimitiveType.VARCHAR);
                level1RangePartitionNamesById.put(partitionId, level1.getKey());
                LOG.debug(hmsTable.getTableName() + " partitionId = " + partitionId + ", level1 = " + level1 +
                        ", level2 = " + level2);

                operator.getScanOperatorPredicates().getIdToPartitionKey().put(partitionId, partitionKey);
                partitionId++;
            }
        }

        Set<String> selectLevel1RangePartitionNames =
                rangePrunePartitions(operator, context, hmsTable, level1PartitionNameToPartitionValues,
                        partitionColumns.get(0));
        LOG.debug(hmsTable.getTableName() + " level1 selectRangePartitionNames = " + selectLevel1RangePartitionNames);

        List<Long> matches = new ArrayList<>();
        for (Long entry : level1RangePartitionNamesById.keySet()) {
            if (selectLevel1RangePartitionNames.contains(level1RangePartitionNamesById.get(entry))) {
                matches.add(entry);
            }
        }
        LOG.debug(hmsTable.getTableName() + " selectedPartitionIds = " + matches);
        ScanOperatorPredicates scanOperatorPredicates = operator.getScanOperatorPredicates();
        scanOperatorPredicates.setSelectedPartitionIds(matches);

        addConjunctsForThive(operator);
    }
}
