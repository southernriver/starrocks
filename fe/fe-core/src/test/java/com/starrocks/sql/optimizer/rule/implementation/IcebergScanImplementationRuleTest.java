// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer.rule.implementation;

import com.google.common.collect.Maps;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.Table;
import com.starrocks.sql.optimizer.Memo;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.logical.LogicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import mockit.Mocked;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class IcebergScanImplementationRuleTest {

    @Test
    public void transform(@Mocked IcebergTable table) {
        LogicalIcebergScanOperator logicalIcebergScanOperator = new LogicalIcebergScanOperator(table,
                Table.TableType.ICEBERG, Maps.newHashMap(), Maps.newHashMap(), -1, ConstantOperator.createBoolean(true));

        List<OptExpression> output =
                new IcebergScanImplementationRule().transform(new OptExpression(logicalIcebergScanOperator), new OptimizerContext(
                        new Memo(), new ColumnRefFactory()));

        assertEquals(1, output.size());

        PhysicalIcebergScanOperator physical = (PhysicalIcebergScanOperator) output.get(0).getOp();
        assertEquals(ConstantOperator.createBoolean(true), physical.getPredicate());
    }

}