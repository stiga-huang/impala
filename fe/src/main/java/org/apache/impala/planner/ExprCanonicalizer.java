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

package org.apache.impala.planner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.impala.analysis.BinaryPredicate;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.InPredicate;
import org.apache.impala.analysis.SlotRef;
import org.apache.impala.analysis.ToSqlOptions;
import org.apache.impala.catalog.FeTable;

/**
 * Utility class for canonicalizing expressions for History-Based Optimization (HBO).
 *
 * This class handles expression normalization at different strategy levels:
 * - EXPR_REWRITE: Sorts conjuncts and IN values for deterministic hashing
 * - IGNORE_PARTITION_CONSTANTS: Additionally removes constants from partition column predicates
 *
 * Note: Most expression normalization (e.g., `1 = a` → `a = 1`, `(a=1 OR a=2)` → `a IN (1,2)`)
 * is already performed by ExprRewriter during analysis, so we don't need to redo it here.
 */
public class ExprCanonicalizer {

  // Placeholder string used when removing constants from predicates
  private static final String CONST = "<CONST>";

  /**
   * Canonicalizes a list of expressions according to the specified strategy.
   *
   * @param exprs List of expressions to canonicalize
   * @param table The table being scanned (used to identify partition columns)
   * @param strategy The canonicalization strategy to apply
   * @return A new list of canonicalized expression strings, sorted deterministically
   */
  public static List<String> canonicalizeScanConjuncts(List<Expr> exprs, FeTable table,
      CanonicalizationStrategy strategy) {
    if (exprs == null || exprs.isEmpty()) return Collections.emptyList();
    List<String> result = new ArrayList<>();
    for (Expr expr : exprs) {
      String canonicalizedStr = canonicalizeExpr(expr, table, strategy);
      result.add(canonicalizedStr);
    }
    // Sort for deterministic ordering
    Collections.sort(result);
    return result;
  }

  /**
   * Canonicalizes a single expression according to the strategy.
   */
  private static String canonicalizeExpr(Expr expr, FeTable table,
      CanonicalizationStrategy strategy) {
    // For EXPR_REWRITE, we just need to sort IN values if present
    if (strategy == CanonicalizationStrategy.EXPR_REWRITE) {
      return exprToSqlWithSortedInValues(expr, false);
    }

    // For other strategies, check if we should remove constants
    boolean shouldRemoveConstants = false;

    if (Expr.IS_EQUALITY_PREDICATE.apply(expr)) {
      if (strategy == CanonicalizationStrategy.IGNORE_PARTITION_CONSTANTS) {
        // Remove constants only from partition column equality predicates
        shouldRemoveConstants = table != null && table.referencesPartitionColumn(expr);
      }
    }

    return exprToSqlWithSortedInValues(expr, shouldRemoveConstants);
  }

  /**
   * Converts expression to SQL, sorting IN predicate values if present.
   * When shouldRemoveConstants is true, replaces constants with placeholders in
   * equality predicates (BinaryPredicate and InPredicate).
   */
  private static String exprToSqlWithSortedInValues(
      Expr expr, boolean shouldRemoveConstants) {
    if (expr instanceof InPredicate) {
      InPredicate inPred = (InPredicate) expr;
      List<String> values = new ArrayList<>();
      for (int i = 1; i < inPred.getChildren().size(); i++) {
        Expr child = inPred.getChild(i);
        if (shouldRemoveConstants && child.isConstant()) {
          values.add(CONST);
        } else {
          values.add(child.toSql(ToSqlOptions.FOR_HBO));
        }
      }
      Collections.sort(values);

      StringBuilder sb = new StringBuilder();
      sb.append(inPred.getChild(0).toSql(ToSqlOptions.FOR_HBO));
      sb.append(inPred.isNotIn() ? " NOT IN (" : " IN (");
      for (int i = 0; i < values.size(); i++) {
        if (i > 0) sb.append(", ");
        sb.append(values.get(i));
      }
      sb.append(")");
      return sb.toString();
    }

    if (shouldRemoveConstants && expr instanceof BinaryPredicate) {
      BinaryPredicate binPred = (BinaryPredicate) expr;
      Expr lhs = binPred.getChild(0);
      Expr rhs = binPred.getChild(1);
      // NormalizeBinaryPredicatesRule guarantees that constant is on the right side.
      if ((binPred.getOp() == BinaryPredicate.Operator.EQ ||
          binPred.getOp() == BinaryPredicate.Operator.NOT_DISTINCT)
          && rhs.isConstant() && lhs instanceof SlotRef) {
        return lhs.toSql(ToSqlOptions.FOR_HBO) +
            binPred.getOp().toString() + CONST;
      }
    }
    return expr.toSql(ToSqlOptions.FOR_HBO);
  }
}
