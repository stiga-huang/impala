# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from __future__ import absolute_import, division, print_function
import re
import time

from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.test_dimensions import (
    create_parquet_dimension,
    create_single_exec_option_dimension,
)

QUERY_OPTIONS = {'use_historical_stats': True, 'store_historical_stats': True}

class TestHBO(ImpalaTestSuite):
  """Tests for HBO (History-Based Optimization) cardinality tracking."""

  @classmethod
  def add_test_dimensions(cls):
    super(TestHBO, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(create_single_exec_option_dimension())
    cls.ImpalaTestMatrix.add_dimension(
        create_parquet_dimension(cls.get_workload()))
    cls.ImpalaTestMatrix.add_constraint(
        lambda v: v.get_value('table_format').file_format == 'parquet' and
        v.get_value('table_format').compression_codec == 'none')

  @staticmethod
  def _verify_explain_has_hbo_cardinality(
      explain_lines, expected_cardinality, node_name=None):
    """Returns True if the explain output contains 'cardinality=value (from HBO)'.
    If node_name is provided, restricts the search to the section of the explain
    output belonging to that node (e.g. 'functional.alltypes b')."""
    pattern = re.compile(r'cardinality=([^\s(]+)\s*\(from HBO\)')
    node_header_pattern = re.compile(r'^\s*[|\-\s]*\d+:')

    if node_name is not None:
      # Find the line containing the node
      start_idx = None
      for i, line in enumerate(explain_lines):
        if node_name in line:
          start_idx = i
          break
      assert start_idx is not None, "Expected node %r in explain: %s" % (
          node_name, explain_lines)
      # Find the end of this node's section (next node header)
      end_idx = len(explain_lines)
      for i in range(start_idx + 1, len(explain_lines)):
        if node_header_pattern.match(explain_lines[i]):
          end_idx = i
          break
      scope_lines = explain_lines[start_idx:end_idx]
    else:
      scope_lines = explain_lines

    combined = '\n'.join(line for line in scope_lines)
    match = pattern.search(combined)
    assert match is not None, "Expected 'cardinality=%s (from HBO)' in explain: %s"\
        % (expected_cardinality, explain_lines)
    actual = match.group(1)
    assert actual == expected_cardinality, "Expected cardinality %r but got %r in "\
        "explain: %s" % (expected_cardinality, actual, explain_lines)

  def _test_single_scan_alltypes(self, partitioned, no_stats):
    # Based on testdata/bin/compute-table-stats.sh, table functional_parquet.alltypes and
    # functional_parquet.alltypes_nonpartitioned don't have HMS stats, i.e. numRows.
    # But table functional.alltypes and functional.alltypes_nonpartitioned do.
    db = "functional_parquet" if no_stats else "functional"
    tbl = "alltypes" if partitioned else "alltypes_nonpartitioned"
    def query(select_expr, year):
      return ("SELECT {0} FROM {1}.{2} WHERE year={3}"
              " AND int_col=1 AND string_col='1'").format(select_expr, db, tbl, year)

    self.client.set_configuration(QUERY_OPTIONS)
    # Run query to populate HBO stats
    self.execute_query(query("count(*)", 2009))
    # Wait for 1 second to ensure the stats are written to the cache.
    time.sleep(1)
    # Explain the same/similar queries
    select_exprs = ["count(*)", "count(id)", "count(id), min(int_col)",
                    "count(distinct id)", "id", "*"]
    for y in [2009, 2010]:
      # If HMS stats are missing, HBO only allows exact match so skip if using year=2010.
      # For non-partitioned tables, `year` is a regular column so year=2010 can't match
      # year=2009.
      if (no_stats or not partitioned) and y == 2010:
        continue
      for select_expr in select_exprs:
        result = self.execute_query("EXPLAIN " + query(select_expr, y))
        self._verify_explain_has_hbo_cardinality(result.data, "365")
    # Refresh the table to bump the catalog version. So we can test HBO matches the input
    # file size when HMS stats (numRows) are missing.
    if no_stats:
      self.execute_query("refresh {0}.{1}".format(db, tbl))
      for select_expr in select_exprs:
        result = self.execute_query("EXPLAIN " + query(select_expr, 2009))
        self._verify_explain_has_hbo_cardinality(result.data, "365")

  def test_single_scan_cardinality_partitioned_with_stats(self):
    self._test_single_scan_alltypes(partitioned=True, no_stats=False)

  def test_single_scan_cardinality_partitioned_no_stats(self):
    self._test_single_scan_alltypes(partitioned=True, no_stats=True)

  def test_single_scan_cardinality_non_partitioned_with_stats(self):
    self._test_single_scan_alltypes(partitioned=False, no_stats=False)

  def test_single_scan_cardinality_non_partitioned_no_stats(self):
    self._test_single_scan_alltypes(partitioned=False, no_stats=True)

  def test_multiple_scans_cardinality(self):
    self.client.set_configuration(QUERY_OPTIONS)
    stmt = """
      select count(*) from functional.alltypes a, functional.alltypes b
      where a.id = b.id
        and a.year = 2010 and a.month = 1 and b.year = 2010
        and a.int_col = 0 and b.int_col = 0 and b.string_col = '0'
    """
    # Run query to populate HBO stats
    self.execute_query(stmt)
    # Wait for 1 second to ensure the stats are written to the cache.
    time.sleep(1)
    # Explain the same query
    res = self.execute_query("EXPLAIN " + stmt)
    self._verify_explain_has_hbo_cardinality(
        res.data, "365", node_name="functional.alltypes b")
    self._verify_explain_has_hbo_cardinality(
        res.data, "31", node_name="functional.alltypes a")
    # Explain similar queries
    similar_stmts = [
      # Same conjuncts but different output exprs
      """select count(a.id), min(b.int_col)
         from functional.alltypes a, functional.alltypes b
         where a.id = b.id
           and a.year = 2010 and a.month = 1 and b.year = 2010
           and a.int_col = 0 and b.int_col = 0 and b.string_col = '0'""",
      # Different partition conjuncts but same input size (numRows)
      """select count(*) from functional.alltypes a, functional.alltypes b
         where a.id = b.id
         and a.year = 2009 and a.month = 10 and b.year = 2009
         and a.int_col = 0 and b.int_col = 0 and b.string_col = '0'""",
      # Different statement but same ScanNodes
      """select * from functional.alltypes a
         where year = 2010 and month = 1 and int_col = 0
         union all
         select * from functional.alltypes b
         where year = 2010 and int_col = 0 and string_col = '0'
         """
    ]
    for similar_stmt in similar_stmts:
      res = self.execute_query("EXPLAIN " + similar_stmt)
      self._verify_explain_has_hbo_cardinality(
          res.data, "365", node_name="functional.alltypes b")
      self._verify_explain_has_hbo_cardinality(
          res.data, "31", node_name="functional.alltypes a")

  def test_collection_scan_cardinality(self):
    stmt = "SELECT %s FROM functional_parquet.complextypestbl.int_array"
    self.client.set_configuration(QUERY_OPTIONS)
    where_exprs_cards = {
      "": "10",
      "pos = 0": "3",
      "pos > 1": "5",
      "item > 0": "6",
      "item > 0 and pos > 1": "3",
    }
    for where_expr in where_exprs_cards:
      where = " WHERE " + where_expr if len(where_expr) > 0 else ""
      # Run query to populate HBO stats
      self.execute_query(stmt % "count(*)" + where)
      # Wait for 1 second to ensure the stats are written to the cache.
      time.sleep(1)
      # Explain the same/similar queries
      select_exprs = ["count(*)", "count(pos)", "count(item)", "max(item)", "pos", "item"]
      for select_expr in select_exprs:
        res = self.execute_query("EXPLAIN " + stmt % select_expr + where)
        self._verify_explain_has_hbo_cardinality(res.data, where_exprs_cards[where_expr])

  def test_iceberg_scan_cardinality(self):
    stmt = "select %s from functional_parquet.iceberg_partitioned where id > 10"
    self.client.set_configuration(QUERY_OPTIONS)
    self.execute_query(stmt % "count(id)")
    # Wait for 1 second to ensure the stats are written to the cache.
    time.sleep(1)
    exprs = ["count(id)", "max(id)", "count(distinct user)", "min(event_time)"]
    for expr in exprs:
      res = self.execute_query("EXPLAIN " + stmt % expr)
      self._verify_explain_has_hbo_cardinality(res.data, "10")
