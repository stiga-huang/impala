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

package org.apache.impala.analysis;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.impala.authorization.AuthorizationContext;
import org.apache.impala.authorization.TableMask;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.ColumnStats;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.FeView;
import org.apache.impala.catalog.StructField;
import org.apache.impala.catalog.StructType;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.InternalException;
import org.apache.impala.common.Pair;
import org.apache.impala.rewrite.ExprRewriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * An inline view is a query statement with an alias. Inline views can be parsed directly
 * from a query string or represent a reference to a local or catalog view.
 */
public class InlineViewRef extends TableRef {
  private final static Logger LOG = LoggerFactory.getLogger(InlineViewRef.class);

  // Catalog or local view that is referenced.
  // Null for inline views parsed directly from a query string.
  private final FeView view_;

  // If not null, these will serve as the column labels for the inline view. This provides
  // a layer of separation between column labels visible from outside the inline view
  // and column labels used in the query definition. Either all or none of the column
  // labels must be overridden.
  private List<String> explicitColLabels_;

  /////////////////////////////////////////
  // BEGIN: Members that need to be reset()

  // The select or union statement of the inline view
  protected QueryStmt queryStmt_;

  // queryStmt has its own analysis context
  protected Analyzer inlineViewAnalyzer_;

  // list of tuple ids materialized by queryStmt
  protected final List<TupleId> materializedTupleIds_ = new ArrayList<>();

  // Map inline view's output slots to the corresponding resultExpr of queryStmt.
  protected final ExprSubstitutionMap smap_;

  // Map inline view's output slots to the corresponding baseTblResultExpr of queryStmt.
  protected final ExprSubstitutionMap baseTblSmap_;

  // Whether this is an inline view generated for table masking.
  private boolean isTableMaskingView_ = false;

  // Whether this is an inline view generated for a non-correlated scalar subquery
  // returning at most one value.
  private boolean isNonCorrelatedScalarSubquery_ = false;

  // END: Members that need to be reset()
  /////////////////////////////////////////

  /**
   * C'tor for creating inline views parsed directly from the a query string.
   */
  public InlineViewRef(String alias, QueryStmt queryStmt,
      TableSampleClause sampleParams) {
    super(null, alias, sampleParams);
    Preconditions.checkNotNull(queryStmt);
    queryStmt_ = queryStmt;
    view_ = null;
    smap_ = new ExprSubstitutionMap();
    baseTblSmap_ = new ExprSubstitutionMap();
  }

  public InlineViewRef(String alias, QueryStmt queryStmt, List<String> colLabels) {
    this(alias, queryStmt, (TableSampleClause) null);
    explicitColLabels_ = Lists.newArrayList(colLabels);
  }

  /**
   * C'tor for creating inline views that replace a local or catalog view ref.
   */
  public InlineViewRef(FeView view, TableRef origTblRef) {
    super(view.getTableName().toPath(), origTblRef.getExplicitAlias(),
        origTblRef.getPrivilege(), origTblRef.requireGrantOption());
    queryStmt_ = view.getQueryStmt().clone();
    queryStmt_.reset();
    if (view.isLocalView()) queryStmt_.reset();
    view_ = view;
    smap_ = new ExprSubstitutionMap();
    baseTblSmap_ = new ExprSubstitutionMap();
    setJoinAttrs(origTblRef);
    explicitColLabels_ = view.getColLabels();
    // Set implicit aliases if no explicit one was given.
    if (hasExplicitAlias()) return;
    aliases_ = new String[] {
        view_.getTableName().toString().toLowerCase(), view_.getName().toLowerCase()
    };
    sampleParams_ = origTblRef.getSampleParams();
  }

  /**
   * C'tor for cloning.
   */
  public InlineViewRef(InlineViewRef other) {
    super(other);
    Preconditions.checkNotNull(other.queryStmt_);
    view_ = other.view_;
    queryStmt_ = other.queryStmt_.clone();
    inlineViewAnalyzer_ = other.inlineViewAnalyzer_;
    if (other.explicitColLabels_ != null) {
      explicitColLabels_ = Lists.newArrayList(other.explicitColLabels_);
    }
    materializedTupleIds_.addAll(other.materializedTupleIds_);
    smap_ = other.smap_.clone();
    baseTblSmap_ = other.baseTblSmap_.clone();
    isTableMaskingView_ = other.isTableMaskingView_;
  }

  /**
   * Creates an inline-view doing table masking for column masking and row filtering
   * policies. Callers should replace 'tableRef' with the returned view.
   *
   * @param tableRef original resolved table/view
   * @param tableMask TableMask providing column masking and row filtering policies
   * @param authzCtx AuthorizationContext containing RangerBufferAuditHandler
   */
  static InlineViewRef createTableMaskView(TableRef tableRef, TableMask tableMask,
      AuthorizationContext authzCtx) throws AnalysisException, InternalException {
    Preconditions.checkNotNull(tableRef);
    Preconditions.checkNotNull(authzCtx);
    Preconditions.checkState(tableRef instanceof InlineViewRef
        || tableRef instanceof BaseTableRef);
    List<Column> columns = tableMask.getRequiredColumns();
    List<SelectListItem> items = Lists.newArrayListWithCapacity(columns.size());
    for (Column col: columns) {
      items.add(new SelectListItem(
          tableMask.createColumnMask(col.getName(), col.getType(), authzCtx),
          /*alias*/ col.getName()));
    }
    if (tableMask.hasComplexColumnMask()) {
      throw new AnalysisException("Column masking is not supported for complex types");
    }
    if (columns.isEmpty()) {
      // No columns so use "SELECT 1 FROM tbl" to make a valid statement.
      items.add(new SelectListItem(NumericLiteral.create(1), /*alias*/null));
    }
    SelectList selectList = new SelectList(items);
    FromClause fromClause = new FromClause(Lists.newArrayList(tableRef));
    Expr wherePredicate = tableMask.createRowFilter(authzCtx);
    SelectStmt tableMaskStmt = new SelectStmt(selectList, fromClause, wherePredicate,
        null, null, null, null);

    InlineViewRef viewRef = new InlineViewRef(/*alias*/ null, tableMaskStmt,
        (TableSampleClause) null);
    tableRef.migratePropertiesTo(viewRef);
    viewRef.isTableMaskingView_ = true;
    if (LOG.isTraceEnabled()) {
      LOG.trace("Replacing '{}' with subquery: {}", tableRef.toSql(),
          tableMaskStmt.toSql());
    }
    return viewRef;
  }

  /**
   * Analyzes the inline view query block in a child analyzer of 'analyzer', creates
   * a new tuple descriptor for the inline view and registers auxiliary eq predicates
   * between the slots of that descriptor and the select list exprs of the inline view;
   * then performs join clause analysis.
   */
  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    if (isAnalyzed_) return;

    // Analyze the inline view query statement with its own analyzer
    inlineViewAnalyzer_ = new Analyzer(analyzer);

    // Catalog views refs require special analysis settings for authorization.
    if (isCatalogView()) {
      analyzer.registerAuthAndAuditEvent(view_, priv_, requireGrantOption_);
      if (inlineViewAnalyzer_.isExplain()) {
        // If the user does not have privileges on the view's definition
        // then we report a masked authorization error so as not to reveal
        // privileged information (e.g., the existence of a table).
        inlineViewAnalyzer_.setMaskPrivChecks(
            String.format("User '%s' does not have privileges to " +
            "EXPLAIN this statement.", analyzer.getUser().getName()));
      } else {
        // If this is not an EXPLAIN statement, auth checks for the view
        // definition are still performed in order to determine if the user has access
        // to the runtime profile but don't trigger authorization errors.
        inlineViewAnalyzer_.setMaskPrivChecks(null);
      }
    }

    inlineViewAnalyzer_.setUseHiveColLabels(
        isCatalogView() ? true : analyzer.useHiveColLabels());
    queryStmt_.analyze(inlineViewAnalyzer_);
    correlatedTupleIds_.addAll(queryStmt_.getCorrelatedTupleIds());
    if (explicitColLabels_ != null) {
      Preconditions.checkState(
          explicitColLabels_.size() == queryStmt_.getColLabels().size());
    }

    inlineViewAnalyzer_.setHasLimitOffsetClause(
        queryStmt_.hasLimit() || queryStmt_.hasOffset());
    queryStmt_.getMaterializedTupleIds(materializedTupleIds_);
    desc_ = analyzer.registerTableRef(this);
    desc_.setSourceView(this);
    isAnalyzed_ = true;  // true now that we have assigned desc

    // For constant selects we materialize its exprs into a tuple.
    if (materializedTupleIds_.isEmpty()) {
      Preconditions.checkState(queryStmt_ instanceof SelectStmt);
      Preconditions.checkState(((SelectStmt) queryStmt_).getTableRefs().isEmpty());
      desc_.setIsMaterialized(true);
      materializedTupleIds_.add(desc_.getId());
    }

    // create smap_ and baseTblSmap_ and register auxiliary eq predicates between our
    // tuple descriptor's slots and our *unresolved* select list exprs;
    // we create these auxiliary predicates so that the analyzer can compute the value
    // transfer graph through this inline view correctly (ie, predicates can get
    // propagated through the view);
    // if the view stmt contains analytic functions, we cannot propagate predicates
    // into the view, unless the predicates are compatible with the analytic
    // function's partition by clause, because those extra filters
    // would alter the results of the analytic functions (see IMPALA-1243)
    // TODO: relax this a bit by allowing propagation out of the inline view (but
    // not into it)
    for (int i = 0; i < getColLabels().size(); ++i) {
      String colName = getColLabels().get(i).toLowerCase();
      Expr colExpr = queryStmt_.getResultExprs().get(i);
      Expr baseTableExpr =  queryStmt_.getBaseTblResultExprs().get(i);
      addColumnToSubstitutionMaps(analyzer, colName, colExpr, baseTableExpr);
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("inline view " + getUniqueAlias() + " smap: " + smap_.debugString());
      LOG.trace("inline view " + getUniqueAlias() + " baseTblSmap: " +
          baseTblSmap_.debugString());
      Preconditions.checkState(baseTblSmap_.checkComposedFrom(smap_));
    }

    analyzeTableSample(analyzer);
    analyzeHints(analyzer);
    // Now do the remaining join analysis
    analyzeJoin(analyzer);
  }

  private void addColumnToSubstitutionMaps(
      Analyzer analyzer, String colName, Expr colExpr, Expr baseTableExpr)
      throws AnalysisException {
    Path p = new Path(desc_, Lists.newArrayList(colName));
    Preconditions.checkState(p.resolve());
    SlotDescriptor slotDesc = analyzer.registerSlotRef(p, false);
    slotDesc.setSourceExpr(colExpr);
    slotDesc.setStats(ColumnStats.fromExpr(colExpr));
    SlotRef slotRef = new SlotRef(slotDesc);
    smap_.put(slotRef, colExpr);
    baseTblSmap_.put(slotRef, baseTableExpr);
    if (createAuxPredicate(colExpr)) {
      analyzer.createAuxEqPredicate(new SlotRef(slotDesc), colExpr.clone());
    }

    if (colExpr.getType().isArrayType()) {
      // Calling registerSlotRef() above created a new SlotDescriptor + TupleDescriptor
      // hierarchy for Array types. Walk through this hiararchy and add all slot refs
      // to smap_ and baseTblSmap_.
      // Source must be a SlotRef
      SlotRef srcSlotRef = (SlotRef) colExpr;
      SlotRef baseTableSlotRef = (SlotRef) baseTableExpr;
      SlotDescriptor srcSlotDesc = srcSlotRef.getDesc();
      SlotDescriptor baseTableSlotDesc = baseTableSlotRef.getDesc();
      TupleDescriptor itemTupleDesc = slotDesc.getItemTupleDesc();
      TupleDescriptor srcItemTupleDesc = srcSlotDesc.getItemTupleDesc();
      TupleDescriptor baseTableItemTupleDesc = baseTableSlotDesc.getItemTupleDesc();
      // We don't recurse deeper and only add the immediate item child to the
      // substitution map. This is enough both for collections in select list and in
      // from clause.
      if (itemTupleDesc != null) {
        Preconditions.checkState(srcItemTupleDesc != null);
        Preconditions.checkState(baseTableItemTupleDesc != null);
        // Assume that there is only a single slot as struct in nested in
        // arrays is not yet supported in select lists.
        Preconditions.checkState(itemTupleDesc.getSlots().size() == 1);
        Preconditions.checkState(srcItemTupleDesc.getSlots().size() == 1);
        Preconditions.checkState(baseTableItemTupleDesc.getSlots().size() == 1);
        SlotDescriptor itemSlotDesc = itemTupleDesc.getSlots().get(0);
        SlotDescriptor srcItemSlotDesc = srcItemTupleDesc.getSlots().get(0);
        SlotDescriptor baseTableItemSlotDesc = baseTableItemTupleDesc.getSlots().get(0);
        SlotRef itemSlotRef = new SlotRef(itemSlotDesc);
        SlotRef srcItemSlotRef = new SlotRef(srcItemSlotDesc);
        SlotRef beseTableItemSlotRef = new SlotRef(baseTableItemSlotDesc);
        smap_.put(itemSlotRef, srcItemSlotRef);
        baseTblSmap_.put(itemSlotRef, beseTableItemSlotRef);
        if (createAuxPredicate(colExpr)) {
          analyzer.createAuxEqPredicate(
              new SlotRef(itemSlotDesc), srcItemSlotRef.clone());
        }
      }
    }

    if (colExpr.getType().isStructType()) {
      Preconditions.checkState(colExpr instanceof SlotRef);
      Preconditions.checkState(baseTableExpr instanceof SlotRef);
      putStructMembersIntoSmap(smap_, slotDesc, (SlotRef) colExpr);
      putStructMembersIntoSmap(baseTblSmap_, slotDesc, (SlotRef) baseTableExpr);
      createAuxPredicatesForStructMembers(analyzer, slotDesc, (SlotRef) colExpr);
    }
  }

  // Puts the fields of 'rhsStruct' into 'smap' as right hand side (mapped) values,
  // recursively. The keys (left hand side values) are constructed based on the
  // corresponding slot descriptors in the itemTupleDesc of 'lhsSlotDesc'.
  private void putStructMembersIntoSmap(ExprSubstitutionMap smap,
      SlotDescriptor lhsSlotDesc, SlotRef rhsStruct) {
    for (Pair<SlotDescriptor, SlotRef> pair :
        getStructSlotDescSlotRefPairs(lhsSlotDesc, rhsStruct)) {
      SlotDescriptor lhs = pair.first;
      SlotRef rhs = pair.second;
      smap.put(new SlotRef(lhs), rhs);
    }
  }

  private void createAuxPredicatesForStructMembers(Analyzer analyzer,
      SlotDescriptor structSlotDesc, SlotRef structExpr) {
    for (Pair<SlotDescriptor, SlotRef> pair :
        getStructSlotDescSlotRefPairs(structSlotDesc, structExpr)) {
      SlotDescriptor structMemberSlotDesc = pair.first;
      SlotRef structMemberExpr = pair.second;

      if (createAuxPredicate(structMemberExpr)) {
        analyzer.createAuxEqPredicate(
            new SlotRef(structMemberSlotDesc), structMemberExpr.clone());
      }
    }
  }

  /**
   * Given a slot desc and a SlotRef expression, both referring to the same struct,
   * returns a list of corresponding SlotDescriptor/SlotRef pairs of the struct members,
   * recursively.
   */
  private List<Pair<SlotDescriptor, SlotRef>> getStructSlotDescSlotRefPairs(
      SlotDescriptor structSlotDesc, SlotRef structExpr) {
    Preconditions.checkState(structSlotDesc.getType().isStructType());
    Preconditions.checkState(structExpr.getType().isStructType());
    Preconditions.checkState(structSlotDesc.getType().equals(structExpr.getType()));

    List<Pair<SlotDescriptor, SlotRef>> result = new ArrayList<>();

    TupleDescriptor lhsItemTupleDesc = structSlotDesc.getItemTupleDesc();
    Preconditions.checkNotNull(lhsItemTupleDesc);
    List<SlotDescriptor> lhsChildSlotDescs = lhsItemTupleDesc.getSlots();
    Preconditions.checkState(
        lhsChildSlotDescs.size() == structExpr.getChildren().size());
    for (int i = 0; i < lhsChildSlotDescs.size(); i++) {
      SlotDescriptor lhsChildSlotDesc = lhsChildSlotDescs.get(i);

      Expr rhsChild = structExpr.getChildren().get(i);
      Preconditions.checkState(rhsChild instanceof SlotRef);
      SlotRef rhsChildSlotRef = (SlotRef) rhsChild;

      List<String> lhsRawPath = lhsChildSlotDesc.getPath().getRawPath();
      Path rhsPath = rhsChildSlotRef.getResolvedPath();

      // The path can be null in the case of the sorting tuple.
      if (rhsPath != null) {
        List<String> rhsRawPath = rhsPath.getRawPath();

        // Check that the children come in the same order on both lhs and rhs. If not, the
        // last part of the paths would be different.
        Preconditions.checkState(lhsRawPath.get(lhsRawPath.size() - 1)
            .equals(rhsRawPath.get(rhsRawPath.size() - 1)));

        result.add(new Pair(lhsChildSlotDesc, rhsChildSlotRef));

        if (rhsChildSlotRef.getType().isStructType()) {
          result.addAll(
              getStructSlotDescSlotRefPairs(lhsChildSlotDesc, rhsChildSlotRef));
        }
      }
    }
    return result;
  }

  /**
   * Checks if an auxiliary predicate should be created for an expr. Returns False if the
   * inline view has a SELECT stmt with analytic functions and the expr is not in the
   * common partition exprs of all the analytic functions computed by this inline view.
   */
  private boolean createAuxPredicate(Expr e) {
    if (!(queryStmt_ instanceof SelectStmt)
        || !((SelectStmt) queryStmt_).hasAnalyticInfo()) {
      return true;
    }
    AnalyticInfo analyticInfo = ((SelectStmt) queryStmt_).getAnalyticInfo();
    return analyticInfo.getCommonPartitionExprs().contains(e);
  }

  /**
   * Create and register a non-materialized tuple descriptor for this inline view.
   * This method is called from the analyzer when registering this inline view.
   * Create a non-materialized tuple descriptor for this inline view.
   */
  @Override
  public TupleDescriptor createTupleDescriptor(Analyzer analyzer)
      throws AnalysisException {
    int numColLabels = getColLabels().size();
    Preconditions.checkState(numColLabels > 0);
    Set<String> uniqueColAliases = Sets.newHashSetWithExpectedSize(numColLabels);
    List<StructField> fields = Lists.newArrayListWithCapacity(numColLabels);
    for (int i = 0; i < numColLabels; ++i) {
      // inline view select statement has been analyzed. Col label should be filled.
      Expr selectItemExpr = queryStmt_.getResultExprs().get(i);
      String colAlias = getColLabels().get(i).toLowerCase();

      // inline view col cannot have duplicate name
      if (!uniqueColAliases.add(colAlias)) {
        throw new AnalysisException("duplicated inline view column alias: '" +
            colAlias + "'" + " in inline view " + "'" + getUniqueAlias() + "'");
      }
      fields.add(new StructField(colAlias, selectItemExpr.getType(), null));
    }

    // Create the non-materialized tuple and set its type.
    TupleDescriptor result = analyzer.getDescTbl().createTupleDescriptor(
        getClass().getSimpleName() + " " + getUniqueAlias());
    result.setIsMaterialized(false);
    // If this is a table masking view, the underlying table is wrapped by this so its
    // nested columns are not visible to the original query block, because we can't
    // expose nested columns in the SelectList. However, we can expose nested columns
    // in this view's output type which is a StructType containing fields of all result
    // columns. The output type is used to resolve Paths (see Path#resolve()).
    // By exposing nested columns in the output type, Paths in the original query block
    // can still be recognized and resolved.
    if (isTableMaskingView_) {
      TableRef tblRef = getUnMaskedTableRef();
      if (tblRef instanceof BaseTableRef) {
        BaseTableRef baseTbl = (BaseTableRef) tblRef;
        FeTable tbl = baseTbl.resolvedPath_.getRootTable();
        boolean exposeNestedColumn = false;
        for (Column col : tbl.getColumnsInHiveOrder()) {
          if (!col.getType().isComplexType()) continue;
          if (LOG.isTraceEnabled()) {
            LOG.trace("Add {} (type={}) to output type of table mask view",
                col.getName(), col.getType().toSql());
          }
          fields.add(new StructField(col.getName(), col.getType(), null));
          exposeNestedColumn = true;
        }
        if (exposeNestedColumn) {
          baseTbl.setExposeNestedColumnsByTableMaskView();
        }
        result.setMaskedTable(baseTbl);
      }
    }
    result.setType(new StructType(fields));
    return result;
  }

  @Override
  public void rewriteExprs(ExprRewriter rewriter, Analyzer analyzer)
      throws AnalysisException {
    super.rewriteExprs(rewriter, analyzer);
    queryStmt_.rewriteExprs(rewriter);
  }

  @Override
  public List<TupleId> getMaterializedTupleIds() {
    Preconditions.checkState(isAnalyzed_);
    Preconditions.checkState(materializedTupleIds_.size() > 0);
    return materializedTupleIds_;
  }

  public Analyzer getAnalyzer() {
    Preconditions.checkState(isAnalyzed_);
    return inlineViewAnalyzer_;
  }

  public ExprSubstitutionMap getSmap() {
    Preconditions.checkState(isAnalyzed_);
    return smap_;
  }

  public ExprSubstitutionMap getBaseTblSmap() {
    Preconditions.checkState(isAnalyzed_);
    return baseTblSmap_;
  }

  public QueryStmt getViewStmt() { return queryStmt_; }

  public List<String> getExplicitColLabels() { return explicitColLabels_; }

  public List<String> getColLabels() {
    if (explicitColLabels_ != null) return explicitColLabels_;
    return queryStmt_.getColLabels();
  }

  @Override
  public List<Column> getColumnsInHiveOrder() {
    return view_.getColumnsInHiveOrder();
  }

  public FeView getView() { return view_; }

  public boolean isTableMaskingView() { return isTableMaskingView_; }

  public boolean isCatalogView() { return view_ != null && !view_.isLocalView(); }

  /**
   * Return the unmasked TableRef if this is an inline view for table masking.
   */
  public TableRef getUnMaskedTableRef() {
    Preconditions.checkState(isTableMaskingView_);
    Preconditions.checkState(queryStmt_ instanceof SelectStmt);
    SelectStmt selectStmt = (SelectStmt) queryStmt_;
    Preconditions.checkNotNull(selectStmt.fromClause_);
    // FromClause could have several table refs due to subquery rewrite, i.e. subquery
    // could be rewritten to joins. The first table refs is the original table ref.
    Preconditions.checkState(selectStmt.fromClause_.size() > 0);
    return selectStmt.fromClause_.get(0);
  }

  @Override
  protected TableRef clone() { return new InlineViewRef(this); }

  @Override
  public void reset() {
    super.reset();
    queryStmt_.reset();
    inlineViewAnalyzer_ = null;
    materializedTupleIds_.clear();
    smap_.clear();
    baseTblSmap_.clear();
  }

  @Override
  protected String tableRefToSql(ToSqlOptions options) {
    // Enclose the alias in quotes if Hive cannot parse it without quotes.
    // This is needed for view compatibility between Impala and Hive.
    String alias = getExplicitAlias();
    if (view_ != null) {
      return view_.getTableName().toSql() + ToSqlUtils.formatAlias(alias);
    }
    StringBuilder sql = new StringBuilder()
        .append("(")
        .append(queryStmt_.toSql(options))
        .append(")")
        .append(ToSqlUtils.formatAlias(alias));
    // Add explicit col labels for debugging even though this syntax isn't supported.
    if (explicitColLabels_ != null) {
      sql.append(" (");
      for (int i = 0; i < getExplicitColLabels().size(); i++) {
        if (i > 0) sql.append(", ");
        sql.append(ToSqlUtils.getIdentSql(getExplicitColLabels().get(i)));
      }
      sql.append(")");
    }
    return sql.toString();
  }

  public void setIsNonCorrelatedScalarSubquery() {
    isNonCorrelatedScalarSubquery_ = true;
  }
  public boolean isNonCorrelatedScalarSubquery() {
    return isNonCorrelatedScalarSubquery_;
  }
}
