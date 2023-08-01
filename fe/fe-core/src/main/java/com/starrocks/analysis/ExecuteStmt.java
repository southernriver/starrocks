// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/ExecuteStmt.java

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

package com.starrocks.analysis;

import com.starrocks.sql.ast.StatementBase;

import java.util.List;

public class ExecuteStmt extends StatementBase {
    private long stmtId;
    private List<LiteralExpr> args;

    public ExecuteStmt(long stmtId, List<LiteralExpr> args) {
        this.stmtId = stmtId;
        this.args = args;
    }

    public long getStmtId() {
        return stmtId;
    }

    public List<LiteralExpr> getArgs() {
        return args;
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.NO_FORWARD;
    }


    @Override
    public String toSql() {
        String sql = "EXECUTE(";
        int size = args.size();
        for (int i = 0; i < size; ++i) {
            sql += args.get(i).toSql();
            if (i < size - 1) {
                sql += ", ";
            }
        }
        sql += ")";
        return sql;
    }
}