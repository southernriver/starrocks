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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.Pair;
import com.starrocks.common.UserException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.thrift.TDescriptorTable;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import scala.xml.PrettyPrinter.Para;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class PrepareStatement extends StatementBase {
    private static final Logger LOG = LogManager.getLogger(PrepareStatement.class);
    private final StatementBase inner;
    private final long stmtId;
    // select * from tbl where a = ? and b = ?
    // `?` is the placeholder
    protected List<Pair<SlotRef, List<ParamPlaceHolderExpr>>> placeholders = new ArrayList<>();

    ConnectContext context;

    public PrepareStatement(StatementBase stmt, long stmtId) {
        this.inner = stmt;
        this.stmtId = stmtId;
    }

    public void setContext(ConnectContext ctx) {
        this.context = ctx;
    }

    public List<ParamPlaceHolderExpr> placeholders() {
        return this.placeholders.stream().flatMap(p -> p.second.stream())
                .collect(Collectors.toList());
    }

    public List<Pair<SlotRef, List<ParamPlaceHolderExpr>>> getOriginPlaceHolderPairs() {
        return placeholders;
    }

    public int getParamCount() {
        return placeholders.stream().map(p -> p.second.size()).reduce(Integer::sum).orElse(0);
    }

    public List<Expr> getSlotRefOfPlaceHolders() {
        return placeholders.stream().map(p -> p.first).collect(Collectors.toList());
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitPreparedStatement(this, context);
    }

    public List<String> getColLabelsOfPlaceHolders() {
        ArrayList<String> lables = new ArrayList<>();
        Preconditions.checkArgument(inner instanceof QueryStatement, "Only support query preparedStatement now");
        for (Expr slotExpr : getSlotRefOfPlaceHolders()) {
            SlotRef slot = (SlotRef) slotExpr;
            Column c = slot.getColumn();
            if (c != null) {
                lables.add(c.getName());
            } else {
                lables.add("");
            }
        }
        return lables;
    }

    public long getStmtId() {
        return stmtId;
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.NO_FORWARD;
    }

    public StatementBase getInnerStmt() {
        return inner;
    }

    public void assignValues(List<LiteralExpr> values) throws UserException {
        if (values.size() != getParamCount()) {
            throw new UserException("Invalid arguments size "
                                + values.size() + ", expected " + getParamCount());
        }
        List<ParamPlaceHolderExpr> holderExprs = placeholders.stream()
                .flatMap(p -> p.second.stream()).collect(Collectors.toList());
        for (int i = 0; i < values.size(); ++i) {
            holderExprs.get(i).setLiteral(values.get(i));
        }
    }

    public void setPlaceholders(List<Pair<SlotRef, List<ParamPlaceHolderExpr>>> placeholders) {
        this.placeholders = placeholders;
    }
}
