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
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.FeTable;

import com.google.common.base.Preconditions;

/**
 * Utility class for canonicalizing expressions for History-Based Optimization (HBO).
 * 
 * This class handles expression normalization at different strategy levels:
 * - EXPR_REWRITE: Sorts conjuncts and IN values for deterministic hashing
 * - IGNORE_PARTITION_CONSTANTS: Additionally removes constants from partition column predicates
 * - IGNORE_EQUALITY_CONSTANTS: Removes constants from all equality predicates
 *
 * Note: Most expression normalization (e.g., `1 = a` → `a = 1`, `(a=1 OR a=2)` → `a IN (1,2)`)
 * is already performed by ExprRewriter during analysis, so we don't need to redo it here.
 */
public class ExprCanonicalizer {

  // Placeholder string used when removing constants from predicates
  private static final String CONST_PLACEHOLDER = "<CONST>";

  /**
   * Canonicalizes a list of expressions according to the specified strategy.
   *
   * @param exprs List of expressions to canonicalize
   * @param table The table being scanned (used to identify partition columns)
   * @param strategy The canonicalization strategy to apply
   * @return A new list of canonicalized expression strings, sorted deterministically
   */
  public static List<String> canonicalizeExprs(List<Expr> exprs, FeTable table,
      CanonicalizationStrategy strategy) {
    Preconditions.checkNotNull(exprs);
    Preconditions.checkNotNull(strategy);

    List<String> result = new ArrayList<>();
    // TODO: For non-ScanNode expressions, check if the column value comes from a
    // partition column
    int numPartitionCols = (table != null) ? table.getNumClusteringCols() : 0;

    for (Expr expr : exprs) {
      String canonicalizedStr = canonicalizeExpr(expr, numPartitionCols, strategy);
      result.add(canonicalizedStr);
    }

    // Sort for deterministic ordering
    Collections.sort(result);
    return result;
  }

  /**
   * Canonicalizes a list of expressions according to the specified strategy.
   * This overload is for non-scan nodes where table information is not available.
   *
   * @param exprs List of expressions to canonicalize
   * @param strategy The canonicalization strategy to apply
   * @return A new list of canonicalized expression strings, sorted deterministically
   */
  public static List<String> canonicalizeExprs(List<Expr> exprs,
      CanonicalizationStrategy strategy) {
    return canonicalizeExprs(exprs, null, strategy);
  }

  /**
   * Canonicalizes a single expression according to the strategy.
   */
  private static String canonicalizeExpr(Expr expr, int numPartitionCols,
      CanonicalizationStrategy strategy) {
    // For EXPR_REWRITE, we just need to sort IN values if present
    if (strategy == CanonicalizationStrategy.EXPR_REWRITE) {
      return exprToSqlWithSortedInValues(expr);
    }

    // For other strategies, check if we should remove constants
    boolean shouldRemoveConstants = false;

    if (isEqualityPredicate(expr)) {
      if (strategy == CanonicalizationStrategy.IGNORE_EQUALITY_CONSTANTS) {
        shouldRemoveConstants = true;
      } else if (strategy == CanonicalizationStrategy.IGNORE_PARTITION_CONSTANTS) {
        // Remove constants only from partition column equality predicates
        shouldRemoveConstants = referencesPartitionColumn(expr, numPartitionCols);
      }
    }

    if (shouldRemoveConstants) {
      return removeConstantsFromEqualityPredicate(expr);
    }
    return exprToSqlWithSortedInValues(expr);
  }

  /**
   * Converts expression to SQL, sorting IN predicate values if present.
   */
  private static String exprToSqlWithSortedInValues(Expr expr) {
    if (expr instanceof InPredicate) {
      InPredicate inPred = (InPredicate) expr;
      // Create a sorted representation of IN values
      List<String> values = new ArrayList<>();
      // Skip first child (the column reference), process remaining children (values)
      for (int i = 1; i < inPred.getChildren().size(); i++) {
        values.add(inPred.getChild(i).toSql(ToSqlOptions.FOR_HBO));
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

    return expr.toSql(ToSqlOptions.FOR_HBO);
  }

  /**
   * Returns true if the expression is an equality predicate (= or IN).
   * Range predicates (<, >, <=, >=, BETWEEN) are NOT equality predicates.
   * TODO: moving this to static method in Expr.java
   */
  private static boolean isEqualityPredicate(Expr expr) {
    if (expr instanceof BinaryPredicate) {
      BinaryPredicate binPred = (BinaryPredicate) expr;
      // Only EQ is an equality predicate, not <, >, <=, >=, !=
      return binPred.getOp() == BinaryPredicate.Operator.EQ ||
             binPred.getOp() == BinaryPredicate.Operator.NOT_DISTINCT;
    }
    if (expr instanceof InPredicate) {
      // IN predicates are treated as equality predicates
      return true;
    }
    return false;
  }

  /**
   * Returns true if the expression references a partition column.
   * Partition columns are the first numPartitionCols columns in the table.
   * TODO: moving this to ScanNode since this is only true there.
   */
  private static boolean referencesPartitionColumn(Expr expr, int numPartitionCols) {
    if (numPartitionCols == 0) return false;

    // Check all slot references in the expression
    List<SlotRef> slotRefs = new ArrayList<>();
    expr.collect(SlotRef.class, slotRefs);
    
    for (SlotRef slotRef : slotRefs) {
      Column col = slotRef.getDesc().getColumn();
      if (col != null && col.getPosition() < numPartitionCols) {
        return true;
      }
    }
    return false;
  }

  /**
   * Removes constants from an equality predicate, replacing them with placeholders.
   * Only handles BinaryPredicate (=) and InPredicate.
   */
  private static String removeConstantsFromEqualityPredicate(Expr expr) {
    if (expr instanceof BinaryPredicate) {
      BinaryPredicate binPred = (BinaryPredicate) expr;
      Expr lhs = binPred.getChild(0);
      Expr rhs = binPred.getChild(1);
      if (((binPred.getOp() == BinaryPredicate.Operator.EQ ||
          binPred.getOp() == BinaryPredicate.Operator.NOT_DISTINCT))
           && rhs.isConstant() && lhs instanceof SlotRef) {
        // TODO: dealing with expressions like "a + 1 = 2" to "a = <CONST>"
        return lhs.toSql(ToSqlOptions.FOR_HBO) + 
            binPred.getOp().toString() + CONST_PLACEHOLDER;
      }
    } else if (expr instanceof InPredicate) {
      InPredicate inPred = (InPredicate) expr;
      StringBuilder sb = new StringBuilder();
      sb.append(inPred.getChild(0).toSql(ToSqlOptions.FOR_HBO));
      sb.append(inPred.isNotIn() ? " NOT IN (" : " IN (");
      for (int i = 1; i < inPred.getChildren().size(); i++) {
        if (i > 1) sb.append(", ");
        Expr child = inPred.getChild(i);
        if (child.isConstant()) {
          sb.append(CONST_PLACEHOLDER);
        } else {
          sb.append(child.toSql(ToSqlOptions.FOR_HBO));
        }
      }
      sb.append(")");
      return sb.toString();
    }

    // Fallback: shouldn't reach here if isEqualityPredicate returned true
    return expr.toSql(ToSqlOptions.FOR_HBO);
  }
}
