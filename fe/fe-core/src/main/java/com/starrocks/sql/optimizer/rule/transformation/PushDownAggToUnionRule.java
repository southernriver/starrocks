// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.BetweenPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.DictMappingOperator;
import com.starrocks.sql.optimizer.operator.scalar.ExistsPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class PushDownAggToUnionRule extends TransformationRule {
    private static final Logger LOG = LogManager.getLogger(PushDownAggToUnionRule.class);
    /* rewrite this rule.
        agg                    agg
         |                      |
         |                      |
       union      ----->      union
         |                      |
        / \                    / \
       /   \                  /   \
     olap  iceberg          agg   agg
                             |     |
                           olap  iceberg
    */
    public static final PushDownAggToUnionRule HIVE_SCAN = new PushDownAggToUnionRule(OperatorType.LOGICAL_HIVE_SCAN);
    public static final PushDownAggToUnionRule ICEBERG_SCAN =
            new PushDownAggToUnionRule(OperatorType.LOGICAL_ICEBERG_SCAN);

    public PushDownAggToUnionRule(OperatorType logicalOperatorType) {
        super(RuleType.TF_PUSH_DOWN_UNION_AGG, Pattern.create(OperatorType.LOGICAL_AGGR).addChildren(
                Pattern.create(OperatorType.LOGICAL_UNION)
                        .addChildren(Pattern.create(OperatorType.LOGICAL_OLAP_SCAN))
                        .addChildren(Pattern.create(logicalOperatorType))));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalOperator logicalOperator = (LogicalOperator) input.getOp();

        if (!(logicalOperator instanceof LogicalAggregationOperator)) {
            return Collections.emptyList();
        }
        return doWork(input, context.getColumnRefFactory());
    }

    private List<OptExpression> doWork(OptExpression input, ColumnRefFactory columnRefFactory) {
        LogicalAggregationOperator topAggOperator = (LogicalAggregationOperator) input.getOp();
        if (topAggOperator.getType().isLocal() || topAggOperator.getType().isDistinctLocal()) {
            return Collections.emptyList();
        }

        // union node
        OptExpression unionOptExpression = input.getInputs().get(0);
        LogicalUnionOperator oldUnionOperator = (LogicalUnionOperator) unionOptExpression.getOp();
        List<ColumnRefOperator> outputColRefs = oldUnionOperator.getOutputColumnRefOp();
        List<List<ColumnRefOperator>> childOutputColRefs = oldUnionOperator.getChildOutputColumns();

        Map<Integer, ColumnRefOperator> columIdMap1 = Maps.newHashMap();
        for (ColumnRefOperator ref : columnRefFactory.getColumnRefs()) {
            columIdMap1.put(ref.getId(), ref);
        }
        Map<Integer, ColumnRefOperator> columIdMap2 = Maps.newHashMap();
        for (ColumnRefOperator op : childOutputColRefs.get(0)) {
            int index = outputColRefs.indexOf(op);
            Preconditions.checkState(index >= 0);
            columnRefFactory.create(op.getName(), op.getType(), op.isNullable());
            columIdMap2.put(op.getId(), childOutputColRefs.get(1).get(index));
        }

        List<List<ColumnRefOperator>> newChildOutputColRefs = Lists.newArrayList();
        List<ColumnRefOperator> newChildOutputColRefs1 = Lists.newArrayList();
        List<ColumnRefOperator> newChildOutputColRefs2 = Lists.newArrayList();

        // olap, iceberg scan node
        OptExpression oldChildOptExpression1 = unionOptExpression.getInputs().get(0);
        OptExpression oldChildOptExpression2 = unionOptExpression.getInputs().get(1);

        oldChildOptExpression1.getOp().setProjection(getNewProjection(oldUnionOperator.getProjection(),
                columnRefFactory, columIdMap1));
        oldChildOptExpression2.getOp().setProjection(getNewProjection(oldUnionOperator.getProjection(),
                columnRefFactory, columIdMap2));

        // union's input column and input node.
        List<OptExpression> newInputs = Lists.newArrayList();

        // we split it as SplitAggregateRule.java, if meet some bug, we should refer to it.
        LogicalAggregationOperator child1AggOperator = getNewAggregationOperator(topAggOperator,
                columnRefFactory, columIdMap1, newChildOutputColRefs1);
        OptExpression newChildOptExpression1 = OptExpression.create(child1AggOperator, oldChildOptExpression1);

        LogicalAggregationOperator child2AggOperator = getNewAggregationOperator(topAggOperator,
                columnRefFactory, columIdMap2, newChildOutputColRefs2);
        OptExpression newChildOptExpression2 = OptExpression.create(child2AggOperator, oldChildOptExpression2);

        //Preconditions.checkState(unionOutputColumns.size() == newChildOutputColRefs2.size());

        newInputs.add(newChildOptExpression1);
        newInputs.add(newChildOptExpression2);

        // for union input column
        newChildOutputColRefs.add(newChildOutputColRefs1);
        newChildOutputColRefs.add(newChildOutputColRefs2);

        LogicalUnionOperator newUnionOperator = new LogicalUnionOperator(newChildOutputColRefs1,
                newChildOutputColRefs, true);
        OptExpression unionExpression = OptExpression.create(newUnionOperator, newInputs);

        LogicalAggregationOperator agg =
                getTopAggregationOperator(topAggOperator, newChildOutputColRefs1, columIdMap1);
        return Lists.newArrayList(OptExpression.create(agg, unionExpression));
    }

    private ScalarOperator getNewScalarOperator(ScalarOperator oldOpt,
                                                Map<Integer, ColumnRefOperator> columIdMap) {
        if (oldOpt instanceof ColumnRefOperator) {
            ColumnRefOperator ref = (ColumnRefOperator) oldOpt;
            return columIdMap.get(ref.getId());
        }
        if (oldOpt.getChildren().isEmpty()) {
            return oldOpt;
        }
        ArrayList<ScalarOperator> children = new ArrayList<>();
        for (ScalarOperator op : oldOpt.getChildren()) {
            children.add(getNewScalarOperator(op, columIdMap));
        }
        if (oldOpt instanceof CastOperator) {
            return new CastOperator(oldOpt.getType(), children.get(0), ((CastOperator) oldOpt).isImplicit());
        } else if (oldOpt instanceof CaseWhenOperator) {
            CaseWhenOperator cp = (CaseWhenOperator) oldOpt;
            return new CaseWhenOperator(cp, children);
        } else if (oldOpt instanceof CallOperator) {
            CallOperator fn = (CallOperator) oldOpt;
            return new CallOperator(fn.getFnName(), fn.getType(),
                    children,
                    fn.getFunction(),
                    fn.isDistinct());
        } else if (oldOpt instanceof BinaryPredicateOperator) {
            BinaryPredicateOperator bp = (BinaryPredicateOperator) oldOpt;
            return new BinaryPredicateOperator(bp.getBinaryType(), children);
        } else if (oldOpt instanceof BetweenPredicateOperator) {
            BetweenPredicateOperator bp = (BetweenPredicateOperator) oldOpt;
            return new BetweenPredicateOperator(bp.isNotBetween(), children.toArray(new ScalarOperator[0]));
        } else if (oldOpt instanceof CompoundPredicateOperator) {
            CompoundPredicateOperator cp = (CompoundPredicateOperator) oldOpt;
            return new CompoundPredicateOperator(cp.getCompoundType(), children.toArray(new ScalarOperator[0]));
        } else if (oldOpt instanceof DictMappingOperator) {
            DictMappingOperator dp = (DictMappingOperator) oldOpt;
            return new DictMappingOperator((ColumnRefOperator) getNewScalarOperator(dp.getDictColumn(), columIdMap),
                    getNewScalarOperator(dp.getOriginScalaOperator(), columIdMap), dp.getType());
        } else if (oldOpt instanceof ExistsPredicateOperator) {
            ExistsPredicateOperator ep = (ExistsPredicateOperator) oldOpt;
            return new ExistsPredicateOperator(ep.isNotExists(), children);
        } else if (oldOpt instanceof IsNullPredicateOperator) {
            IsNullPredicateOperator ip = (IsNullPredicateOperator) oldOpt;
            return new IsNullPredicateOperator(ip.isNotNull(), children.get(0));
        } else if (oldOpt instanceof InPredicateOperator) {
            InPredicateOperator ip = (InPredicateOperator) oldOpt;
            return new InPredicateOperator(ip.isNotIn(), children);
        } else if (oldOpt instanceof LikePredicateOperator) {
            LikePredicateOperator lp = (LikePredicateOperator) oldOpt;
            return new LikePredicateOperator(lp.getLikeType(), children.toArray(new ScalarOperator[0]));
        } else {
            LOG.warn("Will not transfer operator: " + oldOpt);
            return oldOpt;
        }
    }

    private List<ColumnRefOperator> getNewColumnRefList(List<ColumnRefOperator> oldRefs,
                                                        Map<Integer, ColumnRefOperator> columIdMap) {
        List<ColumnRefOperator> newRefs = Lists.newArrayList();
        for (ColumnRefOperator ref : oldRefs) {
            ColumnRefOperator newColumn = (ColumnRefOperator) getNewScalarOperator(ref, columIdMap);
            newRefs.add(newColumn);
        }
        return newRefs;
    }

    private Map<ColumnRefOperator, ScalarOperator> getNewColumn2ScalarMap(Map<ColumnRefOperator, ScalarOperator> oldMap,
                                                                          ColumnRefFactory columnRefFactory,
                                                                          Map<Integer, ColumnRefOperator> columIdMap) {
        Map<ColumnRefOperator, ScalarOperator> columnRefMap = Maps.newHashMap();
        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : oldMap.entrySet()) {
            if (entry.getValue() instanceof ColumnRefOperator) {
                ColumnRefOperator ref = (ColumnRefOperator) entry.getValue();
                Preconditions.checkState(ref.getId() == entry.getKey().getId());
                ColumnRefOperator newOpt = (ColumnRefOperator) getNewScalarOperator(entry.getValue(), columIdMap);
                columnRefMap.put(newOpt, newOpt);
                continue;
            }
            ColumnRefOperator key = entry.getKey();
            ColumnRefOperator newKey = columnRefFactory.create(key.getName(), key.getType(), key.isNullable());
            columnRefMap.put(newKey, getNewScalarOperator(entry.getValue(), columIdMap));
            columIdMap.put(key.getId(), newKey);
        }
        return columnRefMap;
    }

    private Projection getNewProjection(Projection oldProjection, ColumnRefFactory columnRefFactory,
                                        Map<Integer, ColumnRefOperator> columIdMap) {
        if (oldProjection == null) {
            return null;
        }
        Map<ColumnRefOperator, ScalarOperator> columnRefMap =
                getNewColumn2ScalarMap(oldProjection.getColumnRefMap(), columnRefFactory, columIdMap);

        Map<ColumnRefOperator, ScalarOperator> commonSubOperatorMap =
                getNewColumn2ScalarMap(oldProjection.getCommonSubOperatorMap(), columnRefFactory, columIdMap);

        return new Projection(columnRefMap, commonSubOperatorMap);
    }

    private LogicalAggregationOperator getNewAggregationOperator(LogicalAggregationOperator aggOp,
                                                                 ColumnRefFactory columnRefFactory,
                                                                 Map<Integer, ColumnRefOperator> columIdMap,
                                                                 List<ColumnRefOperator> newChildOutputColRefs) {

        Map<ColumnRefOperator, CallOperator> newAggregations = Maps.newHashMap();

        for (Map.Entry<ColumnRefOperator, CallOperator> entry : aggOp.getAggregations().entrySet()) {
            CallOperator newFn = (CallOperator) getNewScalarOperator(entry.getValue(), columIdMap);
            newFn.setType(getIntermediateType(entry.getValue()));

            ColumnRefOperator newOutputColumn = columnRefFactory.create(
                    entry.getKey().getName(),
                    getIntermediateType(entry.getValue()),
                    entry.getKey().isNullable());
            newAggregations.put(newOutputColumn, newFn);
            newChildOutputColRefs.add(newOutputColumn);
            //columIdMap.put(entry.getKey().getId(), newOutputColumn.getId());
        }

        List<ColumnRefOperator> newGroupBy = getNewColumnRefList(aggOp.getGroupingKeys(), columIdMap);
        newChildOutputColRefs.addAll(newGroupBy);

        AggType type = null;
        if (aggOp.getType().isGlobal()) {
            type = AggType.LOCAL;
        } else if (aggOp.getType().isDistinctGlobal()) {
            type = AggType.DISTINCT_LOCAL;
        }

        return new LogicalAggregationOperator(type,
                newGroupBy,
                getNewColumnRefList(aggOp.getPartitionByColumns(), columIdMap),
                newAggregations,
                aggOp.isSplit(),
                aggOp.getSingleDistinctFunctionPos(),
                aggOp.getLimit(),
                aggOp.getPredicate());
    }

    private LogicalAggregationOperator getTopAggregationOperator(LogicalAggregationOperator aggOp,
                                                                 List<ColumnRefOperator> newChildOutputColRefs,
                                                                 Map<Integer, ColumnRefOperator> columIdMap) {

        Map<ColumnRefOperator, CallOperator> newAggregations = Maps.newHashMap();
        int i = 0;
        for (Map.Entry<ColumnRefOperator, CallOperator> entry : aggOp.getAggregations().entrySet()) {
            CallOperator fn = entry.getValue();
            CallOperator newFn = new CallOperator(fn.getFnName(), fn.getType(),
                    Lists.newArrayList(newChildOutputColRefs.get(i)),
                    fn.getFunction(),
                    fn.isDistinct());
            newAggregations.put(entry.getKey(), newFn);
            i++;
        }

        LogicalAggregationOperator agg = new LogicalAggregationOperator(aggOp.getType(),
                getNewColumnRefList(aggOp.getGroupingKeys(), columIdMap),
                getNewColumnRefList(aggOp.getPartitionByColumns(), columIdMap),
                newAggregations,
                true,
                aggOp.getSingleDistinctFunctionPos(),
                aggOp.getLimit(),
                aggOp.getPredicate());
        Map<ColumnRefOperator, ScalarOperator> columnRefMap = Maps.newHashMap();
        Map<ColumnRefOperator, ScalarOperator> commonSubOperatorMap = Maps.newHashMap();
        if (aggOp.getProjection() != null) {
            Projection oldPrj = aggOp.getProjection();
            for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : oldPrj.getColumnRefMap().entrySet()) {
                columnRefMap.put(entry.getKey(), getNewScalarOperator(entry.getValue(), columIdMap));
            }
            for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : oldPrj.getCommonSubOperatorMap().entrySet()) {
                commonSubOperatorMap.put(entry.getKey(), getNewScalarOperator(entry.getValue(), columIdMap));
            }
        }
        for (Map.Entry<ColumnRefOperator, CallOperator> ref : aggOp.getAggregations().entrySet()) {
            if (columnRefMap.containsKey(ref.getKey())) {
                continue;
            }
            columnRefMap.put(ref.getKey(), ref.getKey());
        }
        for (ColumnRefOperator column : aggOp.getGroupingKeys()) {
            if (columnRefMap.containsKey(column)) {
                continue;
            }
            columnRefMap.put(column, columIdMap.get(column.getId()));
        }
        agg.setProjection(new Projection(columnRefMap, commonSubOperatorMap));
        return agg;
    }

    //copy from SplitAggregateRule.java
    private Type getIntermediateType(CallOperator aggregation) {
        AggregateFunction af = (AggregateFunction) aggregation.getFunction();
        Preconditions.checkState(af != null);
        return af.getIntermediateType() == null ? af.getReturnType() : af.getIntermediateType();
    }
}
