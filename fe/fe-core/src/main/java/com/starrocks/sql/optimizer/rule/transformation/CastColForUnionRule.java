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

import com.google.common.collect.Lists;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CastColForUnionRule extends TransformationRule {
    public CastColForUnionRule() {
        super(RuleType.TF_CAST_COL_FOR_UNION, Pattern.create(OperatorType.LOGICAL_UNION));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalUnionOperator unionOperator = (LogicalUnionOperator) input.getOp();
        if (!(unionOperator instanceof LogicalUnionOperator) || input.getInputs().size() != 2) {
            return Collections.emptyList();
        }

        boolean shouldGenerateProjection = false;
        List<ColumnRefOperator> childOutputColRefs1 = unionOperator.getChildOutputColumns().get(0);
        List<ColumnRefOperator> childOutputColRefs2 = unionOperator.getChildOutputColumns().get(1);
        Map<ColumnRefOperator, ScalarOperator> columnRefMap = new HashMap<>();

        for (int i = 0; i < childOutputColRefs1.size(); i++) {
            ColumnRefOperator childColRef1 = childOutputColRefs1.get(i);
            ColumnRefOperator childColRef2 = childOutputColRefs2.get(i);
            if (childColRef1.getType().matchesType(childColRef2.getType())) {
                columnRefMap.put(childColRef2, childColRef2);
            } else {
                if (!Type.canCastTo(childColRef1.getType(), childColRef2.getType())) {
                    String errorMessage =
                            "Can't do type cast from " + childColRef2.getName() + "(" + childColRef2.getType() +
                                    ") to " + childColRef1.getName() + "(" + childColRef1.getType() +
                                    "). Please disable lake-house query";
                    throw new IllegalStateException(errorMessage);
                }
                shouldGenerateProjection = true;
                columnRefMap.put(childColRef2, new CastOperator(childColRef1.getType(), childColRef2));
            }
        }

        if (shouldGenerateProjection) {
            OptExpression optExp = input.getInputs().get(1);
            optExp.getOp().setProjection(new Projection(columnRefMap));
            return Lists.newArrayList(input);
        } else {
            return Collections.emptyList();
        }
    }
}
