// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.CompoundPredicate;
import com.starrocks.analysis.CompoundPredicate.Operator;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.ExprSubstitutionMap;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.InfoSchemaDb;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.common.Pair;
import com.starrocks.qe.ShowResultSetMetaData;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

// SHOW TABLES
public class ShowTableStmt extends ShowStmt {

    private static final String NAME_COL_PREFIX = "Tables_in_";
    private static final String TYPE_COL = "Table_type";
    private static final String PROPERTIES_COL = "properties";
    private static final String DB_COL = "TABLE_SCHEMA";
    private static final String TABLE_COL = "TABLE_NAME";
    private static final String TABLE_TYPE_COL = "TABLE_TYPE";
    private static final String TABLE_PROPERTIES_REGEXP_PREFIX = "table_properties\\.";
    private static final String TABLE_PROPERTIES_PREFIX = "table_properties.";

    private static final TableName TABLE_NAME = new TableName(InfoSchemaDb.DATABASE_NAME, "tables");
    private static final TableName TABLE_CONFIG_NAME = new TableName(InfoSchemaDb.DATABASE_NAME, "tables_config");
    private static final TableName TMP_TABLE_CONFIG_NAME = new TableName(null, "config_table");
    private static final TableName TMP_TABLE_NAME = new TableName(null, "tables_table");
    private String db;
    private final boolean isVerbose;
    private final String pattern;
    private Expr where;
    private String catalogName;

    public ShowTableStmt(String db, boolean isVerbose, String pattern) {
        this.db = db;
        this.isVerbose = isVerbose;
        this.pattern = pattern;
        this.where = null;
    }

    public ShowTableStmt(String db, boolean isVerbose, String pattern, Expr where) {
        this.db = db;
        this.isVerbose = isVerbose;
        this.pattern = pattern;
        this.where = where;
    }

    public ShowTableStmt(String db, boolean isVerbose, String pattern, String catalogName) {
        this.db = db;
        this.isVerbose = isVerbose;
        this.pattern = pattern;
        this.where = null;
        this.catalogName = catalogName;
    }

    public ShowTableStmt(String db, boolean isVerbose, String pattern, Expr where, String catalogName) {
        this.db = db;
        this.isVerbose = isVerbose;
        this.pattern = pattern;
        this.where = where;
        this.catalogName = catalogName;
    }

    public String getDb() {
        return db;
    }

    public boolean isVerbose() {
        return isVerbose;
    }

    public String getPattern() {
        return pattern;
    }

    public String getCatalogName() {
        return catalogName;
    }

    @Override
    public QueryStatement toSelectStmt() {
        if (where == null) {
            return null;
        }
        // Columns
        SelectList selectList = new SelectList();
        ExprSubstitutionMap aliasMap = new ExprSubstitutionMap(false);
        SelectListItem item = new SelectListItem(new SlotRef(TABLE_NAME, TABLE_COL),
                NAME_COL_PREFIX + db);
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, NAME_COL_PREFIX + db),
                item.getExpr().clone(null));
        if (isVerbose) {
            item = new SelectListItem(new SlotRef(TABLE_NAME, TABLE_TYPE_COL), TYPE_COL);
            selectList.addItem(item);
            aliasMap.put(new SlotRef(null, TYPE_COL), item.getExpr().clone(null));
        }
        where = where.substitute(aliasMap);
        // where databases_name = currentdb
        Expr whereDbEQ = new BinaryPredicate(
                BinaryPredicate.Operator.EQ,
                new SlotRef(TABLE_NAME, DB_COL),
                new StringLiteral(db));
        // old where + and + db where
        Expr finalWhere = new CompoundPredicate(
                CompoundPredicate.Operator.AND,
                whereDbEQ,
                where);
        Pair<Expr, Expr> splitExpr = splitPropertiesExprs(finalWhere);
        if (splitExpr == null) {
            return new QueryStatement(new SelectRelation(selectList, new TableRelation(TABLE_NAME),
                    finalWhere, null, null));
        } else {
            // Join tables_config to filter by properties
            finalWhere = splitExpr.first;
            selectList.addItem(new SelectListItem(new SlotRef(TABLE_NAME, DB_COL), null));
            SelectRelation selectTablesRelation = new SelectRelation(selectList, new TableRelation(TABLE_NAME),
                    finalWhere, null, null);

            Expr configWhere = splitExpr.second;
            SelectList configSelectList = new SelectList();
            configSelectList.addItem(new SelectListItem(new SlotRef(TABLE_CONFIG_NAME, DB_COL), null));
            configSelectList.addItem(new SelectListItem(new SlotRef(TABLE_CONFIG_NAME, TABLE_COL), null));

            SelectRelation selectTablesConfigRelation = new SelectRelation(configSelectList, new TableRelation(TABLE_CONFIG_NAME),
                    configWhere, null, null);

            SubqueryRelation configRelation = new SubqueryRelation(new QueryStatement(selectTablesConfigRelation));
            configRelation.setAlias(TMP_TABLE_CONFIG_NAME);
            SubqueryRelation tableRelation = new SubqueryRelation(new QueryStatement(selectTablesRelation));
            tableRelation.setAlias(TMP_TABLE_NAME);


            BinaryPredicate onPredicateSchema = new BinaryPredicate(
                    BinaryPredicate.Operator.EQ,
                    new SlotRef(TMP_TABLE_NAME, DB_COL),
                    new SlotRef(TMP_TABLE_CONFIG_NAME, DB_COL));

            BinaryPredicate onPredicateTable = new BinaryPredicate(
                    BinaryPredicate.Operator.EQ,
                    new SlotRef(TMP_TABLE_NAME, NAME_COL_PREFIX + db),
                    new SlotRef(TMP_TABLE_CONFIG_NAME, TABLE_COL));

            Expr finalOnPredicate = new CompoundPredicate(Operator.AND, onPredicateSchema, onPredicateTable);

            JoinRelation joinRelation =
                    new JoinRelation(JoinOperator.INNER_JOIN, tableRelation, configRelation, finalOnPredicate, false);

            SelectList finalSelectList = new SelectList(
                    selectList.getItems().stream()
                            .filter(si -> si.getAlias() != null)
                            .map(si -> new SelectListItem(new SlotRef(TMP_TABLE_NAME, si.getAlias()),
                                    si.getAlias())).collect(Collectors.toList()), false);
            return new QueryStatement(new SelectRelation(finalSelectList, joinRelation, null, null, null));
        }
    }

    private List<Expr> flatFilterWithAndOperator(CompoundPredicate root) {
        if (root.getOp() != Operator.AND) {
            return Collections.singletonList(root);
        }
        List<Expr> flatOperators = new ArrayList<>();
        ArrayList<Expr> children = root.getChildren();

        for (Expr child : children) {
            if (child instanceof CompoundPredicate) {
                flatOperators.addAll(flatFilterWithAndOperator((CompoundPredicate) child));
            } else {
                flatOperators.add(child);
            }
        }
        return flatOperators;
    }

    @VisibleForTesting
    public Pair<Expr, Expr> splitPropertiesExprs(Expr where) {
        // where clauses unlikely contain properties filter only
        if (where instanceof CompoundPredicate) {
            Operator op = ((CompoundPredicate) where).getOp();
            if (op != Operator.AND) {
                return null;
            }
            List<Expr> exprs = flatFilterWithAndOperator((CompoundPredicate) where);
            Function<Expr, Boolean> splitPropertiesExpr = expr -> {
                if (expr instanceof BinaryPredicate) {
                    Pair<SlotRef, Expr> extract = ((BinaryPredicate) expr).extract();
                    SlotRef slot = extract.first;
                    return slot.getColumnName().startsWith(TABLE_PROPERTIES_PREFIX) && extract.second instanceof LiteralExpr;
                }
                return false;
            };
            Map<Boolean, List<Expr>> groupByDiff = exprs.stream().collect(Collectors.groupingBy(splitPropertiesExpr));
            Preconditions.checkArgument(groupByDiff.containsKey(false), "Show stmt should contain non-properties filter");
            if (groupByDiff.containsKey(true)) {
                List<Expr> propertiesExpr = groupByDiff.get(true);
                List<Expr> jsonFilterExprs = new ArrayList<>();
                for (Expr pe : propertiesExpr) {
                    Pair<SlotRef, Expr> eqPair = ((BinaryPredicate) pe).extract();
                    String propertyKey = eqPair.first.getColumnName().replaceFirst(TABLE_PROPERTIES_REGEXP_PREFIX, "");
                    String propertyValue = ((LiteralExpr) eqPair.second).getStringValue();
                    Expr jsonQueryExpr =
                            LiteralExpr.create("$." + propertyKey, Type.VARCHAR);
                    LiteralExpr propertyValueExpr = LiteralExpr.create(propertyValue, Type.VARCHAR);
                    Expr propertiesSlot = new SlotRef(TABLE_CONFIG_NAME, PROPERTIES_COL);
                    jsonFilterExprs.add(new BinaryPredicate(BinaryPredicate.Operator.EQ,
                            new FunctionCallExpr("json_query", Arrays.asList(propertiesSlot, jsonQueryExpr)), propertyValueExpr));
                }

                Expr propertiesFinalPredicate =
                        jsonFilterExprs.stream().reduce((e1, e2) -> new CompoundPredicate(Operator.AND, e1, e2)).get();

                Expr newWhere =
                        groupByDiff.get(false).stream().reduce((e1, e2) -> new CompoundPredicate(Operator.AND, e1, e2)).get();
                return Pair.create(newWhere, propertiesFinalPredicate);
            }
        }
        return null;
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        builder.addColumn(
                new Column(NAME_COL_PREFIX + db, ScalarType.createVarchar(20)));
        if (isVerbose) {
            builder.addColumn(new Column(TYPE_COL, ScalarType.createVarchar(20)));
        }
        return builder.build();
    }

    public void setDb(String db) {
        this.db = db;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowTableStatement(this, context);
    }
}
