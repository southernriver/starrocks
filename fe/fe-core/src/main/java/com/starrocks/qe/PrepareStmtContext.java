// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/PrepareStmtContext.java

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
package com.starrocks.qe;

import com.starrocks.analysis.ParamPlaceHolderExpr;
import com.starrocks.analysis.PrepareStatement;
import com.starrocks.analysis.SlotRef;
import com.starrocks.common.Pair;

import java.util.ArrayList;
import java.util.List;

public class PrepareStmtContext {
    private PrepareStatement stmt;
    private final ConnectContext ctx;
    private final long stmtId;

    // Timestamp in millisecond last command starts at
    protected volatile long startTime;
    // select * from tbl where a = ? and b = ?
    // `?` is the placeholder
    private List<Pair<SlotRef, List<ParamPlaceHolderExpr>>> placeholders = new ArrayList<>();

    public PrepareStmtContext(PrepareStatement prepareStatement, ConnectContext ctx, long stmtId) {
        this.stmt = prepareStatement;
        this.ctx = ctx;
        this.stmtId = stmtId;
    }
    public PrepareStmtContext(ConnectContext ctx, long stmtId) {
        this.ctx = ctx;
        this.stmtId = stmtId;
    }

    public void addParamPlaceHolder(Pair<SlotRef, List<ParamPlaceHolderExpr>> holderPair) {
        placeholders.add(holderPair);
    }

    public void setPreparedStmt(PrepareStatement preparedStmt) {
        this.stmt = preparedStmt;
    }

    public void finishPrepare() {
        this.stmt.setPlaceholders(placeholders);
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime() {
        startTime = System.currentTimeMillis();
    }

    public PrepareStatement getStmt() {
        return stmt;
    }

    public long getStmtId() {
        return stmtId;
    }

}