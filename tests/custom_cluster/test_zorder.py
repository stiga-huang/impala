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

import shlex
import pprint
import re

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.parametrize import UniqueDatabase
from tests.common.skip import SkipIfLocal
from tests.common.test_dimensions import create_uncompressed_text_dimension
from tests.util.test_file_parser import QueryTestSectionReader, remove_comments


# Temporary classes for testing Z-ordering frontend features.
# These classes are based on tests/metadata/test_ddl.py and
# tests/metadata/test_show_create_table.py.
# TODO: merge the testfiles and delete this test when Z-order tests do not
# require custom cluster anymore.
class TestZOrder(CustomClusterTestSuite):
  VALID_SECTION_NAMES = ["CREATE_TABLE", "CREATE_VIEW", "QUERY", "RESULTS"]

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestZOrder, cls).add_test_dimensions()

  @CustomClusterTestSuite.with_args(impalad_args="--unlock_zorder_sort=true")
  @SkipIfLocal.hdfs_client
  @UniqueDatabase.parametrize(sync_ddl=True, num_dbs=2)
  def test_alter_table(self, vector, unique_database):
    vector.get_value('exec_option')['abort_on_error'] = False

    # Create an unpartitioned table to get a filesystem directory that does not
    # use the (key=value) format. The directory is automatically cleanup up
    # by the unique_database fixture.
    self.client.execute("create table {0}.part_data (i int)".format(unique_database))
    assert self.filesystem_client.exists(
        "test-warehouse/{0}.db/part_data".format(unique_database))
    self.filesystem_client.create_file(
        "test-warehouse/{0}.db/part_data/data.txt".format(unique_database),
        file_data='1984')
    self.run_test_case('QueryTest/alter-table-zorder', vector, use_db=unique_database)

  @CustomClusterTestSuite.with_args(impalad_args="--unlock_zorder_sort=true")
  @UniqueDatabase.parametrize(sync_ddl=True)
  def test_create_table(self, vector, unique_database):
    vector.get_value('exec_option')['abort_on_error'] = False
    self.run_test_case('QueryTest/create-table-zorder', vector, use_db=unique_database)

  @CustomClusterTestSuite.with_args(impalad_args="--unlock_zorder_sort=true")
  @UniqueDatabase.parametrize(sync_ddl=True)
  def test_create_table_like_table(self, vector, unique_database):
    vector.get_value('exec_option')['abort_on_error'] = False
    self.run_test_case('QueryTest/create-table-like-table-zorder', vector,
        use_db=unique_database)

  @CustomClusterTestSuite.with_args(impalad_args="--unlock_zorder_sort=true")
  @UniqueDatabase.parametrize(sync_ddl=True)
  def test_create_table_like_file(self, vector, unique_database):
    vector.get_value('exec_option')['abort_on_error'] = False
    self.run_test_case('QueryTest/create-table-like-file-zorder', vector,
        use_db=unique_database)

  @CustomClusterTestSuite.with_args(impalad_args="--unlock_zorder_sort=true")
  @UniqueDatabase.parametrize(sync_ddl=True)
  def test_create_table_as_select(self, vector, unique_database):
    vector.get_value('exec_option')['abort_on_error'] = False
    self.run_test_case('QueryTest/create-table-as-select-zorder', vector,
        use_db=unique_database)


# The purpose of the show create table tests are to ensure that the "SHOW CREATE TABLE"
# output can actually be used to recreate the table. A test consists of a table
# definition. The table is created, then the output of "SHOW CREATE TABLE" is used to
# test if the table can be recreated. This test class does not support --update-results.
class TestZOrderShowCreateTable(CustomClusterTestSuite):
  VALID_SECTION_NAMES = ["CREATE_TABLE", "CREATE_VIEW", "QUERY", "RESULTS"]
  # Properties to filter before comparing results
  FILTER_TBL_PROPERTIES = ["transient_lastDdlTime", "numFiles", "numPartitions",
                           "numRows", "rawDataSize", "totalSize", "COLUMN_STATS_ACCURATE",
                           "STATS_GENERATED_VIA_STATS_TASK", "last_modified_by",
                           "last_modified_time", "numFilesErasureCoded",
                           "bucketing_version"]

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestZOrderShowCreateTable, cls).add_test_dimensions()
    # There is no reason to run these tests using all dimensions.
    cls.ImpalaTestMatrix.add_dimension(
        create_uncompressed_text_dimension(cls.get_workload()))
    cls.ImpalaTestMatrix.add_constraint(
        lambda v: v.get_value('table_format').file_format == 'text' and
        v.get_value('table_format').compression_codec == 'none')

  @CustomClusterTestSuite.with_args(impalad_args="--unlock_zorder_sort=true")
  def test_show_create_table(self, vector, unique_database):
    self.__run_show_create_table_test_case('QueryTest/show-create-table-zorder', vector,
                                           unique_database)

  def __run_show_create_table_test_case(self, test_file_name, vector, unique_db_name):
    """
    Runs a show-create-table test file, containing the following sections:

    ---- CREATE_TABLE
    contains a table creation statement to create table TABLE_NAME
    ---- RESULTS
    contains the expected result of SHOW CREATE TABLE table_name

    OR

    ---- CREATE_VIEW
    contains a view creation statement to create table VIEW_NAME
    ---- RESULTS
    contains the expected result of SHOW CREATE VIEW table_name

    OR

    ---- QUERY
    a show create table query
    ---- RESULTS
    contains the expected output of the SHOW CREATE TABLE query

    unique_db_name is the name of the database to use for all tables and
    views and must be unique so as not to conflict with other tests.
    """
    sections = self.load_query_test_file(self.get_workload(), test_file_name,
                                         self.VALID_SECTION_NAMES)
    for test_section in sections:
      test_case = ShowCreateTableTestCase(test_section, test_file_name, unique_db_name)

      if not test_case.existing_table:
        # create table in Impala
        self.__exec(test_case.create_table_sql)
      # execute "SHOW CREATE TABLE ..."
      result = self.__exec(test_case.show_create_table_sql)
      create_table_result = self.__normalize(result.data[0])

      if not test_case.existing_table:
        # drop the table
        self.__exec(test_case.drop_table_sql)

      # check the result matches the expected result
      expected_result = self.__normalize(self.__replace_uri(
          test_case.expected_result,
          self.__get_location_uri(create_table_result)))
      self.__compare_result(expected_result, create_table_result)

      if test_case.existing_table:
        continue

      # recreate the table with the result from above
      self.__exec(create_table_result)
      try:
        # we should get the same result from "show create table ..."
        result = self.__exec(test_case.show_create_table_sql)
        new_create_table_result = self.__normalize(result.data[0])
        assert create_table_result == new_create_table_result
      finally:
        # drop the table
        self.__exec(test_case.drop_table_sql)

  def __exec(self, sql_str):
    return self.execute_query_expect_success(self.client, sql_str)

  def __get_location_uri(self, sql_str):
    m = re.search("LOCATION '([^\']+)'", sql_str)
    if m is not None:
      return m.group(1)

  def __compare_result(self, expected_sql, actual_sql):
    """ Extract all properties """
    expected_tbl_props = self.__get_properties_map(expected_sql, "TBLPROPERTIES")
    actual_tbl_props = self.__get_properties_map(actual_sql, "TBLPROPERTIES")
    assert expected_tbl_props == actual_tbl_props

    expected_serde_props = self.__get_properties_map(expected_sql, "SERDEPROPERTIES")
    actual_serde_props = self.__get_properties_map(actual_sql, "SERDEPROPERTIES")
    assert expected_serde_props == actual_serde_props

    expected_sql_filtered = self.__remove_properties_maps(expected_sql)
    actual_sql_filtered = self.__remove_properties_maps(actual_sql)
    assert expected_sql_filtered == actual_sql_filtered

  def __normalize(self, s):
    """ Normalize the string to remove extra whitespaces and remove keys
    from tblproperties and serdeproperties that we don't want
    """
    s = ' '.join(s.split())
    for k in self.FILTER_TBL_PROPERTIES:
      kv_regex = "'%s'\s*=\s*'[^\']+'\s*,?" % (k)
      s = re.sub(kv_regex, "", s)
    # If we removed the last property, there will be a dangling comma that is not valid
    # e.g. 'k1'='v1', ) -> 'k1'='v1')
    s = re.sub(",\s*\)", ")", s)
    # Need to remove any whitespace after left parens and before right parens
    s = re.sub("\(\s+", "(", s)
    s = re.sub("\s+\)", ")", s)
    # If the only properties were removed, the properties sections may be empty, which
    # is not valid
    s = re.sub("TBLPROPERTIES\s*\(\s*\)", "", s)
    s = re.sub("SERDEPROPERTIES\s*\(\s*\)", "", s)
    return s

  def __properties_map_regex(self, name):
    return "%s \(([^)]+)\)" % name

  def __remove_properties_maps(self, s):
    """ Removes the tblproperties and serdeproperties from the string """
    return re.sub(
        self.__properties_map_regex("WITH SERDEPROPERTIES"), "",
        re.sub(self.__properties_map_regex("TBLPROPERTIES"), "", s)).strip()

  def __get_properties_map(self, s, properties_map_name):
    """ Extracts a dict of key-value pairs from the sql string s. The properties_map_name
    is the name of the properties map, e.g. 'tblproperties' or 'serdeproperties'
    """
    map_match = re.search(self.__properties_map_regex(properties_map_name), s)
    if map_match is None:
      return dict()
    kv_regex = "'([^\']+)'\s*=\s*'([^\']+)'"
    kv_results = dict(re.findall(kv_regex, map_match.group(1)))
    for filtered_key in self.FILTER_TBL_PROPERTIES:
      if filtered_key in kv_results:
        del kv_results[filtered_key]
    return kv_results

  def __replace_uri(self, s, uri):
    return s if uri is None else s.replace("$$location_uri$$", uri)


# Represents one show-create-table test case. Performs validation of the test sections
# and provides SQL to execute for each section.
class ShowCreateTableTestCase(object):
  RESULTS_DB_NAME_TOKEN = "show_create_table_test_db"

  def __init__(self, test_section, test_file_name, test_db_name):
    if 'QUERY' in test_section:
      self.existing_table = True
      self.show_create_table_sql = remove_comments(test_section['QUERY']).strip()
    elif 'CREATE_TABLE' in test_section:
      self.__process_create_section(
          test_section['CREATE_TABLE'], test_file_name, test_db_name, 'table')
    elif 'CREATE_VIEW' in test_section:
      self.__process_create_section(
          test_section['CREATE_VIEW'], test_file_name, test_db_name, 'view')
    else:
      assert 0, 'Error in test file %s. Test cases require a '\
          'CREATE_TABLE section.\n%s' %\
          (test_file_name, pprint.pformat(test_section))
    expected_result = remove_comments(test_section['RESULTS'])
    self.expected_result = expected_result.replace(
        ShowCreateTableTestCase.RESULTS_DB_NAME_TOKEN, test_db_name)

  def __process_create_section(self, section, test_file_name, test_db_name, table_type):
    self.existing_table = False
    self.create_table_sql = QueryTestSectionReader.build_query(remove_comments(section))
    name = self.__get_table_name(self.create_table_sql, table_type)
    assert name.find(".") == -1, 'Error in test file %s. Found unexpected %s '\
        'name %s that is qualified with a database' % (table_type, test_file_name, name)
    self.table_name = test_db_name + '.' + name
    self.create_table_sql = self.create_table_sql.replace(name, self.table_name, 1)
    self.show_create_table_sql = 'show create %s %s' % (table_type, self.table_name)
    self.drop_table_sql = "drop %s %s" % (table_type, self.table_name)

  def __get_table_name(self, create_table_sql, table_type):
    lexer = shlex.shlex(create_table_sql)
    tokens = list(lexer)
    # sanity check the create table statement
    if len(tokens) < 3 or tokens[0].lower() != "create":
      assert 0, 'Error in test. Invalid CREATE TABLE statement: %s' % (create_table_sql)
    if tokens[1].lower() != table_type.lower() and \
       (tokens[1].lower() != "external" or tokens[2].lower() != table_type.lower()):
      assert 0, 'Error in test. Invalid CREATE TABLE statement: %s' % (create_table_sql)

    if tokens[1].lower() == "external":
      # expect "create external table table_name ..."
      return tokens[3]
    else:
      # expect a create table table_name ...
      return tokens[2]
