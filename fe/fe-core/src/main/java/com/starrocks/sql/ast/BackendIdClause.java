// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/qe/ConnectProcessor.java

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
package com.starrocks.sql.ast;

import com.starrocks.alter.AlterOpType;
import org.apache.commons.lang.NotImplementedException;

import java.util.List;
import java.util.Map;

public class BackendIdClause extends AlterClause {
    protected List<String> ids;

    protected BackendIdClause(List<String> ids) {
        super(AlterOpType.ALTER_OTHER);
        this.ids = ids;
    }

    public List<String> getBackendIds() {
        return ids;
    }

    @Override
    public Map<String, String> getProperties() {
        throw new NotImplementedException();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitBackendIdClause(this, context);
    }
}
