// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.starrocks.analysis.DateLiteral;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.HiveMetaStoreTable;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.ListPartitionInfo;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.metric.TableMetricsEntity;
import com.starrocks.metric.TableMetricsRegistry;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.ScanOperatorPredicates;
import com.starrocks.sql.optimizer.operator.logical.LogicalHiveScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ArrayOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.CollectionElementOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.PredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.OptExternalPartitionPruner;
import com.starrocks.sql.optimizer.rewrite.OptOlapPartitionPruner;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.task.TaskContext;
import com.starrocks.sql.plan.PlanFragmentBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class SplitOlapScanIntoHybridScanRule extends TransformationRule {
    private static final Logger LOG = LogManager.getLogger(PlanFragmentBuilder.class);

    public SplitOlapScanIntoHybridScanRule() {
        super(RuleType.TF_SPLIT_OLAP_SCAN_INTO_HYBRID_SCAN, Pattern.create(OperatorType.LOGICAL_OLAP_SCAN));
    }

    public static class CustomComparator implements Comparator<ColumnRefOperator> {
        @Override
        public int compare(ColumnRefOperator p1, ColumnRefOperator p2) {
            if (p1.getId() > p2.getId()) {
                return 1;
            } else if (p1.getId() < p2.getId()) {
                return -1;
            }
            return 0;
        }
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalScanOperator scanOperator = (LogicalScanOperator) input.getOp();
        if (!(scanOperator instanceof LogicalOlapScanOperator)) {
            return Collections.emptyList();
        }

        LogicalOlapScanOperator olapScanOperator = (LogicalOlapScanOperator) scanOperator;
        // skip check olap scan operator.
        // case 1: select * from table partition(p20220527)
        // case 2: select * from table tablet(94065)
        if (olapScanOperator.getHintsTabletIds().size() != 0 || olapScanOperator.getPartitionNames() != null) {
            return Collections.emptyList();
        }

        Table coldTable = getColdTable(olapScanOperator, context);
        if (scanOperator.isSplited() || coldTable == null || !supportSplitWithColdScanNode(olapScanOperator)) {
            return Collections.emptyList();
        }

        // check origin partition condition all in olap node.
        if (isPredicateAllInOlapNode(olapScanOperator)) {
            return Collections.emptyList();
        }

        HashMap<ColumnRefOperator, Column> originRefToCol =
                new LinkedHashMap<>(olapScanOperator.getColRefToColumnMetaMap());
        Map<Column, ColumnRefOperator> originColToRef = olapScanOperator.getColumnMetaToColRefMap();

        // TODO: Set right column references
        // check origin partition condition all in cold node.
        // if (getOlapPartitionTimeRangeInfo(olapScanOperator).size() == 0) {
        //     LogicalScanOperator coldScanOperator = generateColdScanOperator(coldTable, originRefToCol,
        //             originColToRef, olapScanOperator.getLimit(),
        //             olapScanOperator.getPredicate(), null, context.getTaskContext());
        //     Preconditions.checkState(coldScanOperator != null);
        //     return Lists.newArrayList(new OptExpression(coldScanOperator));
        // }

        // construct new olap node.
        String partitionColumnName = getPartitionName(olapScanOperator);
        Preconditions.checkState(partitionColumnName != null);
        for (Map.Entry<Column, ColumnRefOperator> entry : originColToRef.entrySet()) {
            if (entry.getKey().getName().equals(partitionColumnName)) {
                //we must add partitionColumn, because we add predicate for it.
                originRefToCol.put(entry.getValue(), entry.getKey());
            }
        }

        // generate hot slot references
        Map<ColumnRefOperator, Column> hotRefToCol = new TreeMap<>(new CustomComparator());
        Map<Column, ColumnRefOperator> hotColToRef = new HashMap<>();
        generateNewColAndRefInfo(olapScanOperator.getTable(), coldTable, originColToRef, originRefToCol,
                hotColToRef, hotRefToCol, context.getColumnRefFactory(), false);
        // generate hot scan operator
        LogicalOlapScanOperator hotScanOperator = new LogicalOlapScanOperator(
                olapScanOperator.getTable(),
                hotRefToCol,
                hotColToRef,
                olapScanOperator.getDistributionSpec(),
                olapScanOperator.getLimit(),
                olapScanOperator.getPredicate(),
                //constructOlapPredicate(olapScanOperator, hotOlapColToRef),
                olapScanOperator.getSelectedIndexId(),
                olapScanOperator.getSelectedPartitionId(),
                olapScanOperator.getPartitionNames(),
                olapScanOperator.getSelectedTabletId(),
                olapScanOperator.getHintsTabletIds());

        // generate cold slot references
        Map<ColumnRefOperator, Column> coldRefToCol = new TreeMap<>(new CustomComparator());
        Map<Column, ColumnRefOperator> coldColToRef = new HashMap<>();
        generateNewColAndRefInfo(olapScanOperator.getTable(), coldTable, originColToRef, originRefToCol,
                coldColToRef, coldRefToCol, context.getColumnRefFactory(), true);

        // generate cold predicate
        ScalarOperator coldPredicate = constructColdPredicate(olapScanOperator, hotColToRef, coldColToRef);

        // generate cold scan operator
        LogicalScanOperator coldScanOperator =
                generateColdScanOperator(olapScanOperator.getTable(), coldTable, coldRefToCol, coldColToRef,
                        olapScanOperator.getLimit(), coldPredicate, context.getTaskContext());
        Preconditions.checkState(coldScanOperator != null);

        Long hotScanPartitionNum = getHotScanPartitionNum(olapScanOperator);
        Long coldScanPartitionNum = getColdScanPartitionNum(coldScanOperator, context);
        TableMetricsEntity entity =
                TableMetricsRegistry.getInstance().getMetricsEntity(scanOperator.getTable().getId());
        entity.counterHotScanPartitionsTotal.increase(hotScanPartitionNum);
        entity.counterColdScanPartitionsTotal.increase(coldScanPartitionNum);

        List<List<ColumnRefOperator>> childOutputColumns = Lists.newArrayList();
        childOutputColumns.add(hotScanOperator.getOutputColumns());
        childOutputColumns.add(
                generateColdOutputColumns(olapScanOperator.getTable(), hotScanOperator, coldScanOperator));

        List<OptExpression> newInputs = Lists.newArrayList();
        // set flag for this olap scan node to avoid next split it.
        hotScanOperator.setSplit(true);
        newInputs.add(new OptExpression(hotScanOperator));
        newInputs.add(new OptExpression(coldScanOperator));
        // NOTE: use origin outputColumns for union node.
        LogicalUnionOperator unionOperator = new LogicalUnionOperator(
                hotScanOperator.getOutputColumns(),
                childOutputColumns,
                true);
        return Lists.newArrayList(OptExpression.create(unionOperator, newInputs));
    }

    private Long getHotScanPartitionNum(LogicalOlapScanOperator olapScanOperator) {
        OlapTable table = (OlapTable) olapScanOperator.getTable();
        PartitionInfo partitionInfo = table.getPartitionInfo();
        List<Long> selectedPartitionIds = null;
        if (partitionInfo.getType() == PartitionType.RANGE) {
            selectedPartitionIds = OptOlapPartitionPruner.rangePartitionPrune(table, (RangePartitionInfo) partitionInfo,
                    olapScanOperator);
        } else if (partitionInfo.getType() == PartitionType.LIST) {
            selectedPartitionIds =
                    OptOlapPartitionPruner.listPartitionPrune((ListPartitionInfo) partitionInfo, olapScanOperator);
        }

        if (selectedPartitionIds == null) {
            selectedPartitionIds =
                    table.getPartitions().stream().filter(Partition::hasData).map(Partition::getId).collect(
                            Collectors.toList());
            // some test cases need to perceive partitions pruned, so we can not filter empty partitions.
        } else {
            selectedPartitionIds = selectedPartitionIds.stream()
                    .filter(id -> table.getPartition(id).hasData()).collect(Collectors.toList());
        }

        return Long.valueOf(selectedPartitionIds.size());
    }

    private Long getColdScanPartitionNum(LogicalScanOperator logicalScanOperator, OptimizerContext context) {
        Long selectedPartitionNum = 0L;
        // partitionColumnName -> (LiteralExpr -> partition ids)
        // no null partitions in this map, used by ListPartitionPruner
        Map<ColumnRefOperator, TreeMap<LiteralExpr, Set<Long>>> columnToPartitionValuesMap = Maps.newHashMap();
        // Store partitions with null partition values separately, used by ListPartitionPruner
        // partitionColumnName -> null partitionIds
        Map<ColumnRefOperator, Set<Long>> columnToNullPartitions = Maps.newHashMap();
        try {
            OptExternalPartitionPruner.initPartitionInfo(logicalScanOperator, context, columnToPartitionValuesMap,
                    columnToNullPartitions);
            OptExternalPartitionPruner.classifyConjuncts(logicalScanOperator, columnToPartitionValuesMap);

            Table table = logicalScanOperator.getTable();
            if (table instanceof HiveMetaStoreTable) {
                ScanOperatorPredicates scanOperatorPredicates = logicalScanOperator.getScanOperatorPredicates();
                ListPartitionPruner partitionPruner =
                        new ListPartitionPruner(columnToPartitionValuesMap, columnToNullPartitions,
                                scanOperatorPredicates.getPartitionConjuncts());
                Collection<Long> selectedPartitionIds = partitionPruner.prune();
                if (selectedPartitionIds == null) {
                    selectedPartitionIds = scanOperatorPredicates.getIdToPartitionKey().keySet();
                }
                selectedPartitionNum = Long.valueOf(selectedPartitionIds.size());
            }
        } catch (Exception e) {
            LOG.warn("HMS table partition prune failed : ", e);
            throw new StarRocksPlannerException(e.getMessage(), ErrorType.INTERNAL_ERROR);
        }
        return selectedPartitionNum;
    }

    private List<ColumnRefOperator> generateColdOutputColumns(Table hotTable, LogicalScanOperator hotScanOperator,
                                                              LogicalScanOperator coldScanOperator) {
        List<ColumnRefOperator> coldOutputColumns = new ArrayList<>();
        Map<String, String> colMap = ((OlapTable) hotTable).getHotColdColumnMap();
        List<ColumnRefOperator> hotOutputColumns = hotScanOperator.getOutputColumns();
        List<ColumnRefOperator> tmpColdOutputColumns = coldScanOperator.getOutputColumns();
        for (ColumnRefOperator hotColRef : hotScanOperator.getOutputColumns()) {
            for (ColumnRefOperator coldColRef : tmpColdOutputColumns) {
                if (hotColRef.getName().equals(coldColRef.getName()) ||
                        (colMap.containsKey(hotColRef.getName()) &&
                                colMap.get(hotColRef.getName()).equals(coldColRef.getName()))) {
                    coldOutputColumns.add(coldColRef);
                }
            }
        }
        Preconditions.checkState(hotOutputColumns.size() == coldOutputColumns.size());
        return coldOutputColumns;
    }

    private Projection generateProjectionForColdScanOperator(Table hotTable, Set<ColumnRefOperator> hotRefs,
                                                             Set<ColumnRefOperator> coldRefs,
                                                             ColumnRefFactory columnRefFactory) {
        Projection projection = null;
        boolean shouldGenerateProjection = false;

        Map<String, String> colMap = ((OlapTable) hotTable).getHotColdColumnMap();
        Set<ColumnRefOperator> copyColdRefs = coldRefs.stream().collect(Collectors.toSet());
        Map<ColumnRefOperator, ScalarOperator> columnRefMap = new TreeMap<>(new CustomComparator());

        for (ColumnRefOperator hotColRef : hotRefs) {
            for (ColumnRefOperator coldColRef : copyColdRefs) {
                if (hotColRef.getName().equals(coldColRef.getName()) ||
                        (colMap.containsKey(hotColRef.getName()) &&
                                colMap.get(hotColRef.getName()).equals(coldColRef.getName()))) {
                    Type inputType = coldColRef.getType();
                    Type outputType = hotColRef.getType();
                    if (!outputType.matchesType(inputType)) {
                        if (!Type.canCastTo(inputType, outputType)) {
                            String errorMessage =
                                    "Can't do type cast from " + coldColRef.getName() + "(" + inputType +
                                            ") to " + hotColRef.getName() + "(" + outputType +
                                            "). Please disable lake-house query";
                            throw new IllegalStateException(errorMessage);
                        }
                        shouldGenerateProjection = true;
                        columnRefMap.put(coldColRef, new CastOperator(hotColRef.getType(), coldColRef));
                        // ColumnRefOperator c = columnRefFactory.create("cast", outputType, true);
                        // ScalarOperator expr = new CastOperator(outputType, coldColRef, true);
                        // columnRefMap.put(c, expr);
                    } else {
                        columnRefMap.put(coldColRef, coldColRef);
                    }
                    copyColdRefs.remove(coldColRef);
                    break;
                }
            }
        }

        if (shouldGenerateProjection) {
            projection = new Projection(columnRefMap);
        }
        return projection;
    }

    private LogicalScanOperator generateColdScanOperator(Table hotTable,
                                                         Table coldTable,
                                                         Map<ColumnRefOperator, Column> coldRefToColumn,
                                                         Map<Column, ColumnRefOperator> coldColumnToRef,
                                                         Long coldLimit,
                                                         ScalarOperator coldPredicate,
                                                         TaskContext ctx) {
        Preconditions.checkState(coldTable instanceof HiveTable || coldTable instanceof IcebergTable);

        Set<ColumnRefOperator> scanColumns = new HashSet<>();
        scanColumns.addAll(coldRefToColumn.keySet());
        scanColumns.addAll(Utils.extractColumnRef(coldPredicate));

        if (!containsMaterializedColumn(coldTable, scanColumns)) {
            List<ColumnRefOperator> preOutputColumns =
                    new ArrayList<>(coldColumnToRef.values());
            List<ColumnRefOperator> outputColumns = preOutputColumns.stream()
                    .filter(column -> !column.getType().getPrimitiveType().equals(PrimitiveType.UNKNOWN_TYPE))
                    .collect(Collectors.toList());

            int smallestIndex = -1;
            int smallestColumnLength = Integer.MAX_VALUE;
            for (int index = 0; index < outputColumns.size(); ++index) {
                if (isPartitionColumn(coldTable, outputColumns.get(index).getName())) {
                    continue;
                }

                if (smallestIndex == -1) {
                    smallestIndex = index;
                }
                Type columnType = outputColumns.get(index).getType();
                if (columnType.isScalarType() && columnType.isSupported()) {
                    int columnLength = columnType.getTypeSize();
                    if (columnLength < smallestColumnLength) {
                        smallestIndex = index;
                        smallestColumnLength = columnLength;
                    }
                }
            }
            Preconditions.checkArgument(smallestIndex != -1);
            scanColumns.add(outputColumns.get(smallestIndex));
        }

        Map<ColumnRefOperator, Column> newColumnRefMap = new HashMap<>();
        for (ColumnRefOperator ref : scanColumns) {
            for (Map.Entry<Column, ColumnRefOperator> entry : coldColumnToRef.entrySet()) {
                if (ref == entry.getValue()) {
                    newColumnRefMap.put(ref, entry.getKey());
                }
            }
        }

        LogicalScanOperator coldScanOperator;
        if (coldTable instanceof HiveTable) {
            coldScanOperator =
                    new LogicalHiveScanOperator(coldTable, newColumnRefMap, coldColumnToRef, coldLimit, coldPredicate);
        } else {
            coldScanOperator =
                    new LogicalIcebergScanOperator(coldTable, newColumnRefMap, coldColumnToRef, coldLimit,
                            coldPredicate);
            ((LogicalIcebergScanOperator) coldScanOperator).setHybridScanTable(hotTable);
        }

        coldScanOperator.setHybridScan(true);
        ctx.setIsHybridScanIncluded(true);
        return coldScanOperator;
    }

    private boolean containsMaterializedColumn(Table coldTable, Set<ColumnRefOperator> scanColumns) {
        return scanColumns.size() != 0 && !coldTable.getPartitionColumnNames().containsAll(
                scanColumns.stream().map(ColumnRefOperator::getName).collect(Collectors.toList()));
    }

    private boolean isPartitionColumn(Table coldTable, String columnName) {
        // Hive/Hudi partition columns is not materialized column, so except partition columns
        return coldTable.getPartitionColumnNames().contains(columnName);
    }

    public boolean supportSplitWithColdScanNode(LogicalOlapScanOperator olapScanOperator) {
        OlapTable olapTable = (OlapTable) olapScanOperator.getTable();
        // none partitioned table is not supported
        if (!(olapTable.getPartitionInfo() instanceof RangePartitionInfo)) {
            return false;
        }
        RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) olapTable.getPartitionInfo();
        // TODO: use partitionColumn to check this condition.
        Map<Long, Range<PartitionKey>> idToRange = rangePartitionInfo.getIdToRange(false);
        for (Long id : idToRange.keySet()) {
            PartitionKey key = idToRange.get(id).lowerEndpoint();
            List<LiteralExpr> keys = key.getKeys();
            // now only support partition have one column.
            if (keys.size() > 1) {
                return false;
            }
            // now only support rangePartitionInfo with date/datetime column
            if (!(keys.get(0) instanceof DateLiteral)) {
                return false;
            }
        }
        return true;
    }

    public Table getColdTable(LogicalOlapScanOperator olapScanOperator, OptimizerContext context) {
        Table coldTable = null;
        List<String> coldTableInfo = ((OlapTable) olapScanOperator.getTable()).getColdTableInfo();
        if (coldTableInfo.isEmpty()) {
            return coldTable;
        }

        String coldCatalog = coldTableInfo.get(0);
        String coldDb = coldTableInfo.get(1);
        String coldTbl = coldTableInfo.get(2);

        coldTable = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(coldCatalog, coldDb, coldTbl);
        if (coldTable != null) {
            if (coldTable instanceof HiveTable || coldTable instanceof IcebergTable) {
                context.getDumpInfo().addTable(coldDb, coldTable);
            } else {
                throw new IllegalStateException("Only Hive and Iceberg are supported as cold table.");
            }
        } else {
            throw new IllegalStateException(
                    "Cold table " + coldTable.getName() + " doesn't exist or no access permission for this table.");
        }
        return coldTable;
    }

    /**
     * Get olap's partition column name, now only support column's type is datetime.
     */
    public String getPartitionName(LogicalOlapScanOperator olapScanOperator) {
        OlapTable olapTable = (OlapTable) olapScanOperator.getTable();
        List<Column> partitionColumns = ((RangePartitionInfo) olapTable.getPartitionInfo()).getPartitionColumns();
        for (int i = 0; i < partitionColumns.size(); i++) {
            if (partitionColumns.get(i).getType().isDatetime() ||
                    partitionColumns.get(i).getType().isDateType()) {
                return partitionColumns.get(i).getName();
            }
        }
        return null;
    }

    public Type getPartitionType(String columnName, Map<Column, ColumnRefOperator> olapColumnMeta) {
        for (Column column : olapColumnMeta.keySet()) {
            if (column.getName().equals(columnName)) {
                return column.getType();
            }
        }
        return null;
    }

    /**
     * Construct cold predicate and add time predicate out of partition range.
     * for example:
     * p20220420 VALUES [('2022-04-20 00:00:00'), ('2022-04-21 00:00:00'))
     * p20220425 VALUES [('2022-04-25 00:00:00'), ('2022-04-26 00:00:00'))
     * p20220427 VALUES [('2022-04-27 00:00:00'), ('2022-04-28 00:00:00'))
     * cold predicate must out of this range.
     */
    public ScalarOperator constructColdPredicate(LogicalOlapScanOperator olapScanOperator,
                                                 Map<Column, ColumnRefOperator> hotColumnToRef,
                                                 Map<Column, ColumnRefOperator> coldColumnToRef) {
        // get partition column name, now only support datetime partition.
        String partitionColumnName = getPartitionName(olapScanOperator);
        Preconditions.checkState(partitionColumnName != null);
        Type partitionColumnType = getPartitionType(partitionColumnName, hotColumnToRef);
        Preconditions.checkState(partitionColumnType != null);
        int columnId = getColumnId(partitionColumnName, hotColumnToRef);
        // NOTE: here to get partitions which not synced to remote.
        List<TimeSequence> timeSequenceList = getOlapPartitionTimeRangeInfo(olapScanOperator);
        List<ScalarOperator> predicateList = new ArrayList<ScalarOperator>();

        for (int i = 0; i < timeSequenceList.size(); ) {
            if (i == 0) {
                BinaryPredicateOperator leftPredicate =
                        new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.LT,
                                getPredicateArguments(columnId, partitionColumnName, timeSequenceList.get(i).start,
                                        partitionColumnType));
                predicateList.add(leftPredicate);
            }
            DateLiteral end = timeSequenceList.get(i).end;
            for (i++; i < timeSequenceList.size(); ) {
                DateLiteral nextStart = timeSequenceList.get(i).start;
                if (end.compareTo(nextStart) >= 0) {
                    end = timeSequenceList.get(i).end;
                    i++;
                    continue;
                }
                predicateList.add(getAndPredicate(end, nextStart, columnId, partitionColumnName, partitionColumnType));
                break;
            }
            if (i == timeSequenceList.size()) {
                BinaryPredicateOperator leftPredicate =
                        new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.GE,
                                getPredicateArguments(columnId, partitionColumnName, end, partitionColumnType));
                predicateList.add(leftPredicate);
            }
        }

        ScalarOperator partitionPredicate = Utils.compoundOr(predicateList);
        LOG.debug("cold partition predicates: {}", partitionPredicate.toString());

        ScalarOperator copyPredicate = null;
        if (olapScanOperator.getPredicate() != null) {
            copyPredicate = ((PredicateOperator) olapScanOperator.getPredicate()).clone();
            LOG.debug("hot predicates: {}", copyPredicate.toString());
        }

        ScalarOperator coldPredicate = Utils.compoundAnd(partitionPredicate, copyPredicate);
        LOG.debug("cold compound predicates: {}", coldPredicate.toString());

        Map<String, String> colMap = ((OlapTable) olapScanOperator.getTable()).getHotColdColumnMap();
        modifyColumnRefForColdPredicate(coldPredicate, coldColumnToRef, colMap);
        LOG.debug("cold predicates after column ref modify: {}", coldPredicate.toString());

        modifyColumnTypeForColdPredicate(olapScanOperator.getTable(), coldPredicate);
        LOG.debug("cold predicates after type modify: {}", coldPredicate.toString());

        return coldPredicate;
    }

    public void modifyColumnTypeForColdPredicate(Table table, ScalarOperator coldPredicate) {
        if (coldPredicate == null) {
            return;
        }

        for (int i = 0; i < coldPredicate.getChildren().size(); i++) {
            ScalarOperator child = coldPredicate.getChild(i);
            if (child instanceof BinaryPredicateOperator) {
                handleBinaryPredicate(table, (BinaryPredicateOperator) child);
            } else if (child instanceof PredicateOperator) {
                modifyColumnTypeForColdPredicate(table, child);
            } else if (child instanceof CallOperator) {
                modifyColumnTypeForColdPredicate(table, child);
            } else if (child instanceof CollectionElementOperator) {
                modifyColumnTypeForColdPredicate(table, child);
            } else if (child instanceof ArrayOperator) {
                modifyColumnTypeForColdPredicate(table, child);
            }
        }
    }

    public void handleBinaryPredicate(Table table, BinaryPredicateOperator coldBinaryPredicate) {
        ScalarOperator leftChild = coldBinaryPredicate.getChild(0);
        ScalarOperator rightChild = coldBinaryPredicate.getChild(1);
        ColumnRefOperator colRef;
        ConstantOperator constOp;
        int constIdx;
        if (leftChild instanceof ColumnRefOperator) {
            constIdx = 1;
            colRef = (ColumnRefOperator) leftChild;
            constOp = (ConstantOperator) rightChild;
        } else {
            constIdx = 0;
            colRef = (ColumnRefOperator) rightChild;
            constOp = (ConstantOperator) leftChild;
        }
        Preconditions.checkState(colRef instanceof ColumnRefOperator);
        Preconditions.checkState(constOp instanceof ConstantOperator);

        if (colRef.getType() != constOp.getType()) {
            ConstantOperator coldConstOp;
            Type colType = colRef.getType();
            try {
                coldConstOp = constantTypeCast(table, constOp, colType);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            coldBinaryPredicate.setChild(constIdx, coldConstOp);
        }
    }

    private ConstantOperator constantTypeCast(Table table, ConstantOperator constOp, Type targetType) throws Exception {
        // Date to string or int
        if (constOp.getType().isDateType()) {
            String partitionFormat = ((OlapTable) table).getColdTablePartitionFormat();
            return constOp.castDateTo(partitionFormat, targetType);
        } else {
            // Other type cast
            return constOp.castTo(targetType);
        }
    }

    /*
     * Construct olap predicate and add time predicate in of partition range which not synced to
     * remote storage.
     */
    public ScalarOperator constructOlapPredicate(LogicalOlapScanOperator olapScanOperator,
                                                 Map<Column, ColumnRefOperator> olapColumnMeta) {
        // get partition column name, now only support datetime partition.
        String partitionColumnName = getPartitionName(olapScanOperator);
        Preconditions.checkState(partitionColumnName != null);
        Type partitionColumnType = getPartitionType(partitionColumnName, olapColumnMeta);
        int columnId = getColumnId(partitionColumnName, olapColumnMeta);
        List<ScalarOperator> predicateList = new ArrayList<ScalarOperator>();

        List<TimeSequence> timeSequenceList = getOlapPartitionTimeRangeInfo(olapScanOperator);
        // this case must process in isPredicateAllInColdNode:
        // 1. timeSequenceList size = 0, means: all partitions have been synced to remote;
        for (int i = 0; i < timeSequenceList.size(); ) {
            BinaryPredicateOperator leftPredicate = new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.GE,
                    getPredicateArguments(columnId, partitionColumnName, timeSequenceList.get(i).start,
                            partitionColumnType));

            DateLiteral end = timeSequenceList.get(i).end;
            for (i++; i < timeSequenceList.size(); ) {
                DateLiteral nextStart = timeSequenceList.get(i).start;
                if (end.compareTo(nextStart) >= 0) {
                    end = timeSequenceList.get(i).end;
                    i++;
                    continue;
                }
                BinaryPredicateOperator rightPredicate =
                        new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.LT,
                                getPredicateArguments(columnId, partitionColumnName, end, partitionColumnType));

                predicateList.add(Utils.compoundAnd(leftPredicate, rightPredicate));
                break;
            }
            if (i == timeSequenceList.size()) {
                BinaryPredicateOperator rightPredicate =
                        new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.LT,
                                getPredicateArguments(columnId, partitionColumnName, end, partitionColumnType));
                predicateList.add(Utils.compoundAnd(leftPredicate, rightPredicate));
            }
        }
        // connect this predicates.
        ScalarOperator partitionOrPredicate = Utils.compoundOr(predicateList);
        ScalarOperator predicates = Utils.compoundAnd(partitionOrPredicate, olapScanOperator.getPredicate());
        return predicates;
    }

    public ScalarOperator getAndPredicate(DateLiteral left,
                                          DateLiteral right,
                                          int columnId,
                                          String partitionColumnName,
                                          Type partitionColumnType) {
        BinaryPredicateOperator leftPredicate = new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.GE,
                getPredicateArguments(columnId, partitionColumnName, left, partitionColumnType));
        BinaryPredicateOperator rightPredicate = new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.LT,
                getPredicateArguments(columnId, partitionColumnName, right, partitionColumnType));
        return Utils.compoundAnd(leftPredicate, rightPredicate);
    }

    public List<ScalarOperator> getPredicateArguments(int columnId,
                                                      String partitionColumnName,
                                                      DateLiteral value,
                                                      Type partitionColumnType) {
        List<ScalarOperator> newArguments = new ArrayList<ScalarOperator>();
        newArguments.add(new ColumnRefOperator(columnId, partitionColumnType, partitionColumnName, false));
        newArguments.add(ConstantOperator.createDatetime(value.toLocalDateTime()));
        return newArguments;
    }

    public DateLiteral getDatetimeFromPartitionInfo(PartitionKey key) {
        List<LiteralExpr> keys = key.getKeys();
        DateLiteral time = null;
        for (int i = 0; i < keys.size(); i++) {
            if (keys.get(i) instanceof DateLiteral) {
                time = ((DateLiteral) keys.get(i));
                break;
            }
        }
        Preconditions.checkState(time != null);
        return time;
    }

    public class TimeSequence implements Comparable<TimeSequence> {
        public DateLiteral start;
        public DateLiteral end;

        // the flag hint this range is leftmost value of olap partitons
        public boolean isMinValue;

        public TimeSequence(DateLiteral start, DateLiteral end) {
            this.start = start;
            this.end = end;
        }

        public void setMinValue() {
            this.isMinValue = true;
        }

        public boolean hasMinValue() {
            return isMinValue;
        }

        @Override
        public int compareTo(TimeSequence timeSequence) {
            if (this.start.compareTo(timeSequence.start) == 0) {
                return this.end.compareTo(timeSequence.end);
            }
            return this.start.compareTo(timeSequence.start);
        }
    }

    public void generateNewColAndRefInfo(
            Table hotTable, Table coldTable,
            Map<Column, ColumnRefOperator> originColToRef,
            Map<ColumnRefOperator, Column> originRefToCol,
            Map<Column, ColumnRefOperator> newColToRef,
            Map<ColumnRefOperator, Column> newRefToCol,
            ColumnRefFactory columnRefFactory,
            boolean forColdOperator) {
        // keep ColumnMeta variable have all column info, because the other rule is also like this.
        Map<String, String> colMap = ((OlapTable) hotTable).getHotColdColumnMap();
        Set<String> colSet = new HashSet<>();

        for (Map.Entry<Column, ColumnRefOperator> entry : originColToRef.entrySet()) {
            Column col = entry.getKey();
            ColumnRefOperator ref = entry.getValue();
            String colName = ref.getName();
            if (forColdOperator) {
                if (colMap.containsKey(colName)) {
                    colName = colMap.get(colName);
                }
                col = coldTable.getColumn(colName);
                if (col != null && !colSet.contains(colName)) {
                    colSet.add(colName);
                    newColToRef.put(col, columnRefFactory.create(colName, col.getType(), col.isAllowNull()));
                }
            } else {
                newColToRef.put(col, new ColumnRefOperator(ref.getId(), col.getType(), colName, col.isAllowNull()));
            }
        }

        for (Column column : originRefToCol.values()) {
            Column col = column;
            if (forColdOperator) {
                String colName = colMap.containsKey(col.getName()) ? colMap.get(col.getName()) : col.getName();
                col = coldTable.getColumn(colName);
                if (col == null) {
                    String errorMessage = "No column " + colName + " in cold table " + coldTable.getName() +
                            ". Please check column mapping between " + hotTable.getName() + " and " +
                            coldTable.getName();
                    throw new IllegalStateException(errorMessage);
                }
            }
            ColumnRefOperator ref = newColToRef.get(col);
            Preconditions.checkState(ref != null);
            newRefToCol.put(newColToRef.get(col), col);
        }
    }

    public void generateNewColAndRefForColdTable(
            Table hotTable, Table coldTable,
            Map<Column, ColumnRefOperator> originColToRef,
            Map<ColumnRefOperator, Column> originRefToCol,
            Map<Column, ColumnRefOperator> newColToRef,
            Map<ColumnRefOperator, Column> newRefToCol) {
        AtomicInteger refId = new AtomicInteger(originColToRef.size() + 1);
        coldTable.getColumns().forEach(coldCol -> newColToRef.put(coldCol,
                new ColumnRefOperator(refId.getAndIncrement(), coldCol.getType(), coldCol.getName(),
                        coldCol.isAllowNull())));

        Map<String, String> colMap = ((OlapTable) hotTable).getHotColdColumnMap();
        for (Column column : originRefToCol.values()) {
            String colName = colMap.containsKey(column.getName()) ? colMap.get(column.getName()) : column.getName();
            Column targetCol = coldTable.getColumn(colName);
            if (targetCol == null) {
                String errorMessage = "No column " + colName + " in cold table " + coldTable.getName() +
                        ". Please check column mapping between " + hotTable.getName() + " and " +
                        coldTable.getName();
                throw new IllegalStateException(errorMessage);
            }
            ColumnRefOperator ref = newColToRef.get(targetCol);
            Preconditions.checkState(ref != null);
            newRefToCol.put(ref, targetCol);
        }
    }

    public void modifyColumnRefForColdPredicate(ScalarOperator coldPredicate,
                                                Map<Column, ColumnRefOperator> coldColToRef,
                                                Map<String, String> colMap) {
        if (coldPredicate == null) {
            return;
        }

        for (int i = 0; i < coldPredicate.getChildren().size(); i++) {
            ScalarOperator child = coldPredicate.getChild(i);
            if (child instanceof ColumnRefOperator) {
                ColumnRefOperator hotColRef = (ColumnRefOperator) child;
                String coldColName = colMap.getOrDefault(hotColRef.getName(), hotColRef.getName());
                ColumnRefOperator coldColRef = getColumnRef(coldColName, coldColToRef);
                coldPredicate.setChild(i,
                        new ColumnRefOperator(coldColRef.getId(), coldColRef.getType(), coldColName,
                                coldColRef.isNullable()));
            } else if (child instanceof PredicateOperator) {
                modifyColumnRefForColdPredicate(child, coldColToRef, colMap);
            } else if (child instanceof CallOperator) {
                modifyColumnRefForColdPredicate(child, coldColToRef, colMap);
            } else if (child instanceof CollectionElementOperator) {
                modifyColumnRefForColdPredicate(child, coldColToRef, colMap);
            } else if (child instanceof ArrayOperator) {
                modifyColumnRefForColdPredicate(child, coldColToRef, colMap);
            }
        }
    }

    public ColumnRefOperator getColumnRef(String colName, Map<Column, ColumnRefOperator> colToRef) {
        ColumnRefOperator colRef = null;
        for (Map.Entry<Column, ColumnRefOperator> entry : colToRef.entrySet()) {
            if (colName.equals(entry.getKey().getName())) {
                colRef = entry.getValue();
            }
        }
        Preconditions.checkState(colRef != null);
        return colRef;
    }

    /**
     * Get new column id from columnMeta map depend on column name.
     * because column id in new scan node must difference with children of a common parent.
     */
    public int getColumnId(String colName, Map<Column, ColumnRefOperator> colToRef) {
        int columnId = 0;
        for (Map.Entry<Column, ColumnRefOperator> entry : colToRef.entrySet()) {
            if (colName.equals(entry.getKey().getName())) {
                return entry.getValue().getId();
            }
        }
        Preconditions.checkState(columnId > 0);
        return columnId;
    }

    public List<TimeSequence> getOlapPartitionTimeRangeInfo(LogicalOlapScanOperator olapScanOperator) {
        OlapTable olapTable = (OlapTable) olapScanOperator.getTable();
        Map<Long, Range<PartitionKey>> idToRange = ((RangePartitionInfo) olapTable.getPartitionInfo())
                .getIdToRange(false);
        List<TimeSequence> timeSequenceList = new ArrayList<TimeSequence>();

        for (Long id : idToRange.keySet()) {
            timeSequenceList.add(new TimeSequence(
                    getDatetimeFromPartitionInfo(idToRange.get(id).lowerEndpoint()),
                    getDatetimeFromPartitionInfo(idToRange.get(id).upperEndpoint())));
        }
        return mergeTimeSequenceList(timeSequenceList);
    }

    public List<TimeSequence> mergeTimeSequenceList(List<TimeSequence> timeSequenceList) {
        Collections.sort(timeSequenceList);
        List<TimeSequence> mergeTimeSequenceList = new ArrayList<TimeSequence>();
        // merge time sequence list
        for (int i = 0; i < timeSequenceList.size(); ) {
            DateLiteral start = timeSequenceList.get(i).start;
            DateLiteral end = timeSequenceList.get(i).end;
            for (i++; i < timeSequenceList.size(); ) {
                DateLiteral nextStart = timeSequenceList.get(i).start;
                if (end.compareTo(nextStart) >= 0) {
                    end = timeSequenceList.get(i).end;
                    i++;
                    continue;
                }
                mergeTimeSequenceList.add(new TimeSequence(start, end));
                break;
            }
            if (i == timeSequenceList.size()) {
                mergeTimeSequenceList.add(new TimeSequence(start, end));
            }
        }
        return mergeTimeSequenceList;
    }

    public boolean isPredicateAllInOlapNode(LogicalOlapScanOperator olapScanOperator) {
        // predicate is null , query all data.
        if (olapScanOperator.getPredicate() == null) {
            return false;
        }
        // if preferComputeNode is true: get partitions which not synced to remote.
        // if preferComputeNode is false: get all partitions.
        List<TimeSequence> timeSequenceList = getOlapPartitionTimeRangeInfo(olapScanOperator);
        List<ScalarOperator> orPredicates = changeToORPredicate(olapScanOperator.getPredicate());
        String partitionName = getPartitionName(olapScanOperator);
        for (ScalarOperator orPredicate : orPredicates) {
            // no partition condition in this predicate
            if (!Utils.containColumnRef(orPredicate, partitionName)) {
                return false;
            }
            // get all and predicate and every predicate must meet partition's range.
            List<ScalarOperator> andPredicates = Utils.extractConjuncts(orPredicate);
            if (!isBinaryPredicateInTimeSequenceRange(andPredicates, partitionName, timeSequenceList, false)) {
                return false;
            }
        }
        return true;
    }

    public boolean isBinaryPredicateInTimeSequenceRange(List<ScalarOperator> predicates, String partitionName,
                                                        List<TimeSequence> timeSequenceList, boolean isColdNode) {
        DateLiteral left = null;
        DateLiteral right = null;
        boolean rightInclusive = false;
        // predicate like:  a not in (x, y, z) and b > xxx and b < yyy
        for (ScalarOperator andPredicate : predicates) {
            if (!Utils.containColumnRef(andPredicate, partitionName)) {
                continue;
            }
            // NOTE: now only support resolve BinaryPredicateOperator
            if (!(andPredicate instanceof BinaryPredicateOperator)) {
                return false;
            }

            BinaryPredicateOperator predicate = (BinaryPredicateOperator) andPredicate;
            // NOTE: this predicate is partition's condition but can not resolve this condition's value now.
            if (!(predicate.getChild(1) instanceof ConstantOperator)) {
                return false;
            }
            DateLiteral target = null;
            try {
                if (!(predicate.getChild(0) instanceof ColumnRefOperator)) {
                    return false;
                }
                ConstantOperator child = (ConstantOperator) predicate.getChild(1);
                target = new DateLiteral(child.getDatetime(), child.getType());
            } catch (Exception e) {
                return false;
            }
            if (predicate.getBinaryType() == BinaryPredicateOperator.BinaryType.GT ||
                    predicate.getBinaryType() == BinaryPredicateOperator.BinaryType.GE) {
                if (left == null || left.compareTo(target) < 0) {
                    left = target;
                }
            } else if (predicate.getBinaryType() == BinaryPredicateOperator.BinaryType.LE) {
                if (right == null || right.compareTo(target) > 0) {
                    right = target;
                    rightInclusive = true;
                }
            } else if (predicate.getBinaryType() == BinaryPredicateOperator.BinaryType.LT) {
                if (right == null || right.compareTo(target) > 0) {
                    right = target;
                }
            } else if (predicate.getBinaryType() == BinaryPredicateOperator.BinaryType.EQ) {
                if (left == null || left.compareTo(target) < 0) {
                    left = target;
                }
                if (right == null || right.compareTo(target) > 0) {
                    right = target;
                    rightInclusive = true;
                }
            }
        }

        // check predicate in remote partitions
        if (isColdNode) {
            // only have large than, like: (x > y1, no x < y2);
            if (right == null && left != null) {
                return false;
            } else if (right != null && left == null && timeSequenceList.get(0).hasMinValue()) {
                // only have less than, like: x < y1, no x > y2.
                return isLessThanTimeSequence(timeSequenceList, right, rightInclusive);
            } else {
                return isInTimeSequenceAndIgnoreLeft(timeSequenceList, left, right, rightInclusive);
            }
        }

        // only have less than, like: x < y1, no x > y2.
        if (right != null && left == null) {
            return false;
        } else if (right == null && left != null) {
            // only have greater than, like x > y1 no x < y2.
            return false;
        } else {
            return isInTimeSequence(timeSequenceList, left, right, rightInclusive);
        }
    }

    public List<ScalarOperator> changeToORPredicate(ScalarOperator predicate) {
        // null predicate
        if (predicate == null) {
            return Lists.newArrayList();
        }
        List<ScalarOperator> list = new LinkedList<>();
        // change in predidate to equal list or predicate.
        if (predicate instanceof InPredicateOperator) {
            InPredicateOperator inPredicate = (InPredicateOperator) predicate;
            if (!inPredicate.isNotIn()) {
                for (int i = 1; i < inPredicate.getChildren().size(); i++) {
                    list.add(new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ,
                            inPredicate.getChild(0), inPredicate.getChild(i)));
                }
                return list;
            }
        }

        if (!OperatorType.COMPOUND.equals(predicate.getOpType())) {
            list.add(predicate);
            return list;
        }

        CompoundPredicateOperator cpo = (CompoundPredicateOperator) predicate;
        if (cpo.isOr()) {
            list.addAll(changeToORPredicate(cpo.getChild(0)));
            list.addAll(changeToORPredicate(cpo.getChild(1)));
        }
        if (cpo.isAnd()) {
            List<ScalarOperator> left = changeToORPredicate(cpo.getChild(0));
            List<ScalarOperator> right = changeToORPredicate(cpo.getChild(1));
            for (ScalarOperator operator : left) {
                for (ScalarOperator scalarOperator : right) {
                    list.add(Utils.compoundAnd(operator, scalarOperator));
                }
            }
        }
        return list;
    }

    // for cold node predicat, if right predicate less first timesequence is ok.
    public boolean isInTimeSequenceAndIgnoreLeft(List<TimeSequence> timeSequenceList, DateLiteral left,
                                                 DateLiteral right, boolean rightInclusive) {
        if (timeSequenceList.size() < 1) {
            return false;
        }
        try {
            for (int i = 0; i < timeSequenceList.size(); i++) {
                DateLiteral start = timeSequenceList.get(i).start;
                DateLiteral end = timeSequenceList.get(i).end;
                // less than first partition' end, can ignore left value.
                if (i == 0 && timeSequenceList.get(i).hasMinValue()) {
                    if (end.compareTo(right) > 0 || (end.compareTo(right) == 0 && !rightInclusive)) {
                        return true;
                    }
                }
                if (start.compareTo(left) <= 0 && end.compareTo(right) >= 0 && !rightInclusive) {
                    return true;
                }
                // right inclusive, but partition's right is open interval.
                if (start.compareTo(left) <= 0 && end.compareTo(right) > 0 && rightInclusive) {
                    return true;
                }
            }
        } catch (Exception e) {
            // do nothing
        }
        return false;
    }

    /*
     *  target range in partition range.
     */
    public boolean isInTimeSequence(List<TimeSequence> timeSequenceList,
                                    DateLiteral left, DateLiteral right, boolean rightInclusive) {
        if (timeSequenceList.size() < 1) {
            return false;
        }
        try {
            for (TimeSequence timeSequence : timeSequenceList) {
                DateLiteral start = timeSequence.start;
                DateLiteral end = timeSequence.end;

                if (start.compareTo(left) <= 0 && end.compareTo(right) >= 0 && !rightInclusive) {
                    return true;
                }
                // right inclusive, but partition's right is open interval.
                if (start.compareTo(left) <= 0 && end.compareTo(right) > 0 && rightInclusive) {
                    return true;
                }
            }
        } catch (Exception e) {
            // do nothing
        }
        return false;
    }

    /*
     *  target less than first partition range.
     */
    public boolean isLessThanTimeSequence(List<TimeSequence> timeSequenceList, DateLiteral right,
                                          boolean rightInclusive) {
        if (timeSequenceList.size() < 1) {
            return false;
        }
        DateLiteral end = timeSequenceList.get(0).end;
        if (end.compareTo(right) > 0 && rightInclusive) {
            return true;
        }
        if (end.compareTo(right) >= 0 && !rightInclusive) {
            return true;
        }
        return false;
    }
}
