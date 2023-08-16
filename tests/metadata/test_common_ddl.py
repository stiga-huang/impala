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
from builtins import map, range
import getpass
import re
import time

from tests.common.environ import (HIVE_MAJOR_VERSION)
from tests.common.impala_test_suite import LOG
from tests.metadata.test_ddl_base import TestDdlBase
from tests.util.filesystem_utils import (
    get_fs_path,
    WAREHOUSE,
    WAREHOUSE_PREFIX,
    IS_S3,
    IS_ADLS,
    IS_OZONE)


def get_trash_path(bucket, path):
  if IS_OZONE:
    return get_fs_path('/{0}/.Trash/{1}/Current{2}/{0}/{3}'.format(bucket,
        getpass.getuser(), WAREHOUSE_PREFIX, path))
  return '/user/{0}/.Trash/Current/{1}/{2}'.format(getpass.getuser(), bucket, path)


# Common class that tests both test_ddl and test_sync_to_latest_hms_events classes.
class TestCommonDdl(TestDdlBase):
  @classmethod
  def _test_truncate_cleans_hdfs_files_impl(self, client, filesystem_client,
      unique_database):
    # Verify the db directory exists
    assert filesystem_client.exists(
        "{1}/{0}.db/".format(unique_database, WAREHOUSE))

    client.execute("create table {0}.t1(i int)".format(unique_database))
    # Verify the table directory exists
    assert filesystem_client.exists(
        "{1}/{0}.db/t1/".format(unique_database, WAREHOUSE))

    try:
      # If we're testing S3, we want the staging directory to be created.
      client.execute("set s3_skip_insert_staging=false")
      # Should have created one file in the table's dir
      client.execute("insert into {0}.t1 values (1)".format(unique_database))
      assert len(filesystem_client.ls(
          "{1}/{0}.db/t1/".format(unique_database, WAREHOUSE))) == 2

      # Truncating the table removes the data files and preserves the table's directory
      client.execute("truncate table {0}.t1".format(unique_database))
      assert len(filesystem_client.ls(
          "{1}/{0}.db/t1/".format(unique_database, WAREHOUSE))) == 1

      client.execute(
          "create table {0}.t2(i int) partitioned by (p int)".format(unique_database))
      # Verify the table directory exists
      assert filesystem_client.exists(
          "{1}/{0}.db/t2/".format(unique_database, WAREHOUSE))

      # Should have created the partition dir, which should contain exactly one file
      client.execute(
          "insert into {0}.t2 partition(p=1) values (1)".format(unique_database))
      assert len(filesystem_client.ls(
          "{1}/{0}.db/t2/p=1".format(unique_database, WAREHOUSE))) == 1

      # Truncating the table removes the data files and preserves the partition's
      # directory
      client.execute("truncate table {0}.t2".format(unique_database))
      assert filesystem_client.exists(
          "{1}/{0}.db/t2/p=1".format(unique_database, WAREHOUSE))
      assert len(filesystem_client.ls(
          "{1}/{0}.db/t2/p=1".format(unique_database, WAREHOUSE))) == 0
    finally:
      # Reset to its default value.
      client.execute("set s3_skip_insert_staging=true")

  @classmethod
  def _test_metadata_after_alter_database_impl(self, client, vector, unique_database):
    client.execute("create table {0}.tbl (i int)".format(unique_database))
    client.execute("create function {0}.f() returns int "
                        "location '{1}/libTestUdfs.so' symbol='NoArgs'"
                        .format(unique_database, WAREHOUSE))
    client.execute("alter database {0} set owner user foo_user".format(
      unique_database))
    table_names = client.execute("show tables in {0}".format(
      unique_database)).get_data()
    assert "tbl" == table_names
    func_names = client.execute("show functions in {0}".format(
      unique_database)).get_data()
    assert "INT\tf()\tNATIVE\ttrue" == func_names

  @classmethod
  def _test_alter_table_set_owner_impl(self, client, unique_database):
    table_name = "{0}.test_owner_tbl".format(unique_database)
    client.execute("create table {0}(i int)".format(table_name))
    client.execute("alter table {0} set owner user foo_user".format(table_name))
    owner = TestDdlBase._get_table_or_view_owner(client, table_name)
    assert ('foo_user', 'USER') == owner

    client.execute("alter table {0} set owner role foo_role".format(table_name))
    owner = TestDdlBase._get_table_or_view_owner(client, table_name)

    assert ('foo_role', 'ROLE') == owner

  @classmethod
  def _test_alter_view_set_owner_impl(self, client, unique_database):
    view_name = "{0}.test_owner_tbl".format(unique_database)
    client.execute("create view {0} as select 1".format(view_name))
    client.execute("alter view {0} set owner user foo_user".format(view_name))
    owner = TestDdlBase._get_table_or_view_owner(client, view_name)
    assert ('foo_user', 'USER') == owner

    client.execute("alter view {0} set owner role foo_role".format(view_name))
    owner = TestDdlBase._get_table_or_view_owner(client, view_name)
    assert ('foo_role', 'ROLE') == owner

  @classmethod
  def _test_drop_partition_with_purge_impl(self, client, filesystem_client, vector,
      unique_database):
    """Verfies whether alter <tbl> drop partition purge actually skips trash"""
    client.execute(
        "create table {0}.t1(i int) partitioned by (j int)".format(unique_database))
    # Add two partitions (j=1) and (j=2) to table t1
    client.execute("alter table {0}.t1 add partition(j=1)".format(unique_database))
    client.execute("alter table {0}.t1 add partition(j=2)".format(unique_database))
    dbpath = "{1}/{0}.db".format(unique_database, WAREHOUSE)
    filesystem_client.create_file("{}/t1/j=1/j1.txt".format(dbpath), file_data='j1')
    filesystem_client.create_file("{}/t1/j=2/j2.txt".format(dbpath), file_data='j2')
    # Drop the partition (j=1) without purge and make sure it exists in trash
    client.execute("alter table {0}.t1 drop partition(j=1)".format(unique_database))
    assert not filesystem_client.exists("{}/t1/j=1/j1.txt".format(dbpath))
    assert not filesystem_client.exists("{}/t1/j=1".format(dbpath))
    trash = get_trash_path("test-warehouse", unique_database + ".db")
    assert filesystem_client.exists('{}/t1/j=1/j1.txt'.format(trash))
    assert filesystem_client.exists('{}/t1/j=1'.format(trash))
    # Drop the partition (with purge) and make sure it doesn't exist in trash
    client.execute("alter table {0}.t1 drop partition(j=2) purge".
        format(unique_database))
    if not IS_S3 and not IS_ADLS:
      # In S3, deletes are eventual. So even though we dropped the partition, the files
      # belonging to this partition may still be visible for some unbounded time. This
      # happens only with PURGE. A regular DROP TABLE is just a copy of files which is
      # consistent.
      # The ADLS Python client is not strongly consistent, so these files may still be
      # visible after a DROP. (Remove after IMPALA-5335 is resolved)
      assert not filesystem_client.exists("{}/t1/j=2/j2.txt".format(dbpath))
      assert not filesystem_client.exists("{}/t1/j=2".format(dbpath))
    assert not filesystem_client.exists('{}/t1/j=2/j2.txt'.format(trash))
    assert not filesystem_client.exists('{}/t1/j=2'.format(trash))

  @classmethod
  def _test_create_alter_bulk_partition_impl(self, client,
      filesystem_client, unique_database):
    # Change the scale depending on the exploration strategy, with 50 partitions this
    # test runs a few minutes, with 10 partitions it takes ~50s for two configurations.
    num_parts = 50 if self.exploration_strategy() == 'exhaustive' else 10
    fq_tbl_name = unique_database + ".part_test_tbl"
    client.execute("create table {0}(i int) partitioned by(j int, s string) "
         "location '{1}/{0}'".format(fq_tbl_name, WAREHOUSE))

    # Add some partitions (first batch of two)
    for i in range(num_parts // 5):
      start = time.time()
      client.execute(
          "alter table {0} add partition(j={1}, s='{1}')".format(fq_tbl_name, i))
      LOG.info('ADD PARTITION #%d exec time: %s' % (i, time.time() - start))

    # Modify one of the partitions
    client.execute("alter table {0} partition(j=1, s='1')"
        " set fileformat parquetfile".format(fq_tbl_name))

    # Alter one partition to a non-existent location twice (IMPALA-741)
    filesystem_client.delete_file_dir("tmp/dont_exist1/", recursive=True)
    filesystem_client.delete_file_dir("tmp/dont_exist2/", recursive=True)

    self.execute_query_expect_success(client,
        "alter table {0} partition(j=1,s='1') set location '{1}/tmp/dont_exist1'"
        .format(fq_tbl_name, WAREHOUSE))
    self.execute_query_expect_success(client,
        "alter table {0} partition(j=1,s='1') set location '{1}/tmp/dont_exist2'"
        .format(fq_tbl_name, WAREHOUSE))

    # Add some more partitions
    for i in range(num_parts // 5, num_parts):
      start = time.time()
      client.execute(
          "alter table {0} add partition(j={1},s='{1}')".format(fq_tbl_name, i))
      LOG.info('ADD PARTITION #%d exec time: %s' % (i, time.time() - start))

    # Insert data and verify it shows up.
    client.execute(
        "insert into table {0} partition(j=1, s='1') select 1".format(fq_tbl_name))
    assert '1' == self.execute_scalar_expect_success(client,
        "select count(*) from {0}".format(fq_tbl_name))

  @classmethod
  def _test_alter_table_set_fileformat_impl(self, client, vector, unique_database):
    # Tests that SET FILEFORMAT clause is set for ALTER TABLE ADD PARTITION statement
    fq_tbl_name = unique_database + ".p_fileformat"
    client.execute(
        "create table {0}(i int) partitioned by (p int)".format(fq_tbl_name))

    # Add a partition with Parquet fileformat
    self.execute_query_expect_success(client,
        "alter table {0} add partition(p=1) set fileformat parquet"
        .format(fq_tbl_name))

    # Add two partitions with ORC fileformat
    self.execute_query_expect_success(client,
        "alter table {0} add partition(p=2) partition(p=3) set fileformat orc"
        .format(fq_tbl_name))

    result = self.execute_query_expect_success(client,
        "SHOW PARTITIONS %s" % fq_tbl_name)

    assert 1 == len([line for line in result.data if line.find("PARQUET") != -1])
    assert 2 == len([line for line in result.data if line.find("ORC") != -1])

  @classmethod
  def _test_alter_table_create_many_partitions_impl(self, client, vector,
      unique_database):
    """
    Checks that creating more partitions than the MAX_PARTITION_UPDATES_PER_RPC
    batch size works, in that it creates all the underlying partitions.
    """
    client.execute(
        "create table {0}.t(i int) partitioned by (p int)".format(unique_database))
    MAX_PARTITION_UPDATES_PER_RPC = 500
    alter_stmt = "alter table {0}.t add ".format(unique_database) + " ".join(
        "partition(p=%d)" % (i,) for i in range(MAX_PARTITION_UPDATES_PER_RPC + 2))
    client.execute(alter_stmt)
    partitions = client.execute("show partitions {0}.t".format(unique_database))
    # Show partitions will contain partition HDFS paths, which we expect to contain
    # "p=val" subdirectories for each partition. The regexp finds all the "p=[0-9]*"
    # paths, converts them to integers, and checks that wehave all the ones we
    # expect.
    PARTITION_RE = re.compile("p=([0-9]+)")
    assert list(map(int, PARTITION_RE.findall(str(partitions)))) == \
        list(range(MAX_PARTITION_UPDATES_PER_RPC + 2))

  @classmethod
  def _test_create_alter_tbl_properties_impl(self, client, unique_database):
    fq_tbl_name = unique_database + ".test_alter_tbl"

    # Specify TBLPROPERTIES and SERDEPROPERTIES at CREATE time
    client.execute("""create table {0} (i int)
    with serdeproperties ('s1'='s2', 's3'='s4')
    tblproperties ('p1'='v0', 'p1'='v1')""".format(fq_tbl_name))
    properties = TestDdlBase._get_tbl_properties(client, fq_tbl_name)

    if HIVE_MAJOR_VERSION > 2:
      assert properties['OBJCAPABILITIES'] == 'EXTREAD,EXTWRITE'
      assert properties['TRANSLATED_TO_EXTERNAL'] == 'TRUE'
      assert properties['external.table.purge'] == 'TRUE'
      assert properties['EXTERNAL'] == 'TRUE'
      del properties['OBJCAPABILITIES']
      del properties['TRANSLATED_TO_EXTERNAL']
      del properties['external.table.purge']
      del properties['EXTERNAL']
    assert len(properties) == 2
    # The transient_lastDdlTime is variable, so don't verify the value.
    assert 'transient_lastDdlTime' in properties
    del properties['transient_lastDdlTime']
    assert {'p1': 'v1'} == properties

    properties = TestDdlBase._get_serde_properties(client, fq_tbl_name)
    assert {'s1': 's2', 's3': 's4'} == properties

    # Modify the SERDEPROPERTIES using ALTER TABLE SET.
    client.execute("alter table {0} set serdeproperties "
        "('s1'='new', 's5'='s6')".format(fq_tbl_name))
    properties = TestDdlBase._get_serde_properties(client, fq_tbl_name)
    assert {'s1': 'new', 's3': 's4', 's5': 's6'} == properties

    # Modify the TBLPROPERTIES using ALTER TABLE SET.
    client.execute("alter table {0} set tblproperties "
        "('prop1'='val1', 'p2'='val2', 'p2'='val3', ''='')".format(fq_tbl_name))
    properties = TestDdlBase._get_tbl_properties(client, fq_tbl_name)

    if HIVE_MAJOR_VERSION > 2:
      assert 'OBJCAPABILITIES' in properties
    assert 'transient_lastDdlTime' in properties
    assert properties['p1'] == 'v1'
    assert properties['prop1'] == 'val1'
    assert properties['p2'] == 'val3'
    assert properties[''] == ''

  @classmethod
  def _test_alter_tbl_properties_reload_impl(self, client, filesystem_client, vector,
      unique_database):
    # IMPALA-8734: Force a table schema reload when setting table properties.
    tbl_name = "test_tbl"
    self.execute_query_expect_success(client, "create table {0}.{1} (c1 string)"
                                      .format(unique_database, tbl_name))
    filesystem_client.create_file("{2}/{0}.db/{1}/f".
                                       format(unique_database, tbl_name, WAREHOUSE),
                                       file_data="\nfoo\n")
    self.execute_query_expect_success(client,
                                      "alter table {0}.{1} set tblproperties"
                                      "('serialization.null.format'='foo')"
                                      .format(unique_database, tbl_name))
    result = self.execute_query_expect_success(client,
                                               "select * from {0}.{1}"
                                               .format(unique_database, tbl_name))
    assert len(result.data) == 2
    assert result.data[0] == ''
    assert result.data[1] == 'NULL'
