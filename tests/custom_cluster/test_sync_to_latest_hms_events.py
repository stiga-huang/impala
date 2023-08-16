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
import getpass

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.impala_test_suite import (ImpalaTestSuite)
from tests.common.parametrize import UniqueDatabase
from tests.common.skip import (SkipIfFS, SkipIfLocal, SkipIfCatalogV2)
from tests.metadata.test_common_ddl import TestCommonDdl
from tests.metadata.test_event_processing_base import TestEventProcessingBase
from tests.util.filesystem_utils import (
    get_fs_path, WAREHOUSE, WAREHOUSE_PREFIX, IS_HDFS, IS_OZONE)


CATALOGD_ARGS_ENABLE_SYNC_EVENTS = "--catalog_topic_mode=minimal "\
                                     "--enable_sync_to_latest_event_on_ddls=true"


def get_trash_path(bucket, path):
  if IS_OZONE:
    return get_fs_path('/{0}/.Trash/{1}/Current{2}/{0}/{3}'.format(bucket,
        getpass.getuser(), WAREHOUSE_PREFIX, path))
  return '/user/{0}/.Trash/Current/{1}/{2}'.format(getpass.getuser(), bucket, path)


@SkipIfCatalogV2.hms_event_polling_disabled()
class TestSyncToLatestHmsEvents(CustomClusterTestSuite):
  """
  Tests for the syncing latest events from HMS in the catalogD's cache. Each test in
  this class should be using the catalogd flag enable_sync_to_latest_event_on_ddls=true.
  """
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @SkipIfFS.eventually_consistent
  @SkipIfLocal.hdfs_client
  @CustomClusterTestSuite.with_args(
      impalad_args="--use_local_catalog=true",
      catalogd_args=CATALOGD_ARGS_ENABLE_SYNC_EVENTS)
  def test_truncate_cleans_hdfs_files_sync_ddl(self, unique_database):
    TestCommonDdl._test_truncate_cleans_hdfs_files_impl(self.client,
      self.filesystem_client, unique_database)

  @SkipIfFS.incorrent_reported_ec
  @UniqueDatabase.parametrize(sync_ddl=True)
  @CustomClusterTestSuite.with_args(
      impalad_args="--use_local_catalog=true",
      catalogd_args=CATALOGD_ARGS_ENABLE_SYNC_EVENTS)
  def test_truncate_table(self, vector, unique_database):
    vector.get_value('exec_option')['abort_on_error'] = False
    self.run_test_case('QueryTest/truncate-table', vector, use_db=unique_database)

  @CustomClusterTestSuite.with_args(
      impalad_args="--use_local_catalog=true",
      catalogd_args=CATALOGD_ARGS_ENABLE_SYNC_EVENTS)
  def test_metadata_after_alter_database(self, vector, unique_database):
    TestCommonDdl._test_metadata_after_alter_database_impl(self.client, vector,
      unique_database)

  @CustomClusterTestSuite.with_args(
      impalad_args="--use_local_catalog=true",
      catalogd_args=CATALOGD_ARGS_ENABLE_SYNC_EVENTS)
  def test_alter_table_set_owner(self, unique_database):
    TestCommonDdl._test_alter_table_set_owner_impl(self.client, unique_database)

  @CustomClusterTestSuite.with_args(
      impalad_args="--use_local_catalog=true",
      catalogd_args=CATALOGD_ARGS_ENABLE_SYNC_EVENTS)
  def test_alter_view_set_owner(self, vector, unique_database):
    TestCommonDdl._test_alter_view_set_owner_impl(self.client, unique_database)

  @SkipIfLocal.hdfs_client
  @SkipIfFS.incorrent_reported_ec
  @UniqueDatabase.parametrize(sync_ddl=True, num_dbs=2)
  @CustomClusterTestSuite.with_args(
      impalad_args="--use_local_catalog=true",
      catalogd_args=CATALOGD_ARGS_ENABLE_SYNC_EVENTS)
  def test_alter_table(self, vector, unique_database):
    vector.get_value('exec_option')['abort_on_error'] = False

    # Create an unpartitioned table to get a filesystem directory that does not
    # use the (key=value) format. The directory is automatically cleanup up
    # by the unique_database fixture.
    self.client.execute("create table {0}.part_data (i int)".format(unique_database))
    dbpath = "{1}/{0}.db".format(unique_database, WAREHOUSE)
    assert self.filesystem_client.exists("{}/part_data".format(dbpath))
    self.filesystem_client.create_file(
        "{}/part_data/data.txt".format(dbpath), file_data='1984')
    self.run_test_case('QueryTest/alter-table', vector, use_db=unique_database)

  @UniqueDatabase.parametrize(sync_ddl=True)
  @CustomClusterTestSuite.with_args(
      impalad_args="--use_local_catalog=true",
      catalogd_args=CATALOGD_ARGS_ENABLE_SYNC_EVENTS)
  def test_alter_set_column_stats(self, vector, unique_database):
    self.run_test_case('QueryTest/alter-table-set-column-stats', vector,
        use_db=unique_database)

  @SkipIfLocal.hdfs_client
  @CustomClusterTestSuite.with_args(
      impalad_args="--use_local_catalog=true",
      catalogd_args=CATALOGD_ARGS_ENABLE_SYNC_EVENTS)
  def test_drop_partition_with_purge(self, vector, unique_database):
    TestCommonDdl._test_drop_partition_with_purge_impl(self.client,
      self.filesystem_client, vector, unique_database)

  @SkipIfLocal.hdfs_client
  @CustomClusterTestSuite.with_args(
      impalad_args="--use_local_catalog=true",
      catalogd_args=CATALOGD_ARGS_ENABLE_SYNC_EVENTS)
  def test_create_alter_bulk_partition(self, vector, unique_database):
    TestCommonDdl._test_create_alter_bulk_partition_impl(self.client,
      self.filesystem_client, unique_database)

  @SkipIfLocal.hdfs_client
  @CustomClusterTestSuite.with_args(
      impalad_args="--use_local_catalog=true",
      catalogd_args=CATALOGD_ARGS_ENABLE_SYNC_EVENTS)
  def test_alter_table_set_fileformat(self, vector, unique_database):
    TestCommonDdl._test_alter_table_set_fileformat_impl(self.client, vector,
      unique_database)

  @CustomClusterTestSuite.with_args(
      impalad_args="--use_local_catalog=true",
      catalogd_args=CATALOGD_ARGS_ENABLE_SYNC_EVENTS)
  def test_alter_table_create_many_partitions(self, vector, unique_database):
    TestCommonDdl._test_alter_table_create_many_partitions_impl(self.client, vector,
      unique_database)

  @CustomClusterTestSuite.with_args(
      impalad_args="--use_local_catalog=true",
      catalogd_args=CATALOGD_ARGS_ENABLE_SYNC_EVENTS)
  def test_create_alter_tbl_properties(self, unique_database):
    TestCommonDdl._test_create_alter_tbl_properties_impl(self.client,
      unique_database)

  @CustomClusterTestSuite.with_args(
      impalad_args="--use_local_catalog=true",
      catalogd_args=CATALOGD_ARGS_ENABLE_SYNC_EVENTS)
  def test_alter_tbl_properties_reload(self, vector, unique_database):
    TestCommonDdl._test_alter_tbl_properties_reload_impl(self.client,
      self.filesystem_client, vector, unique_database)

  @SkipIfFS.incorrent_reported_ec
  @UniqueDatabase.parametrize(sync_ddl=True)
  @CustomClusterTestSuite.with_args(
      impalad_args="--use_local_catalog=true",
      catalogd_args=CATALOGD_ARGS_ENABLE_SYNC_EVENTS)
  def test_partition_ddl_predicates(self, vector, unique_database):
    self.run_test_case('QueryTest/partition-ddl-predicates-all-fs', vector,
        use_db=unique_database)
    if IS_HDFS:
      self.run_test_case('QueryTest/partition-ddl-predicates-hdfs-only', vector,
          use_db=unique_database)

  @CustomClusterTestSuite.with_args(
      impalad_args="--use_local_catalog=true",
      catalogd_args=CATALOGD_ARGS_ENABLE_SYNC_EVENTS)
  def test_transactional_insert_events(self, unique_database):
    """Executes 'run_test_insert_events' for transactional tables.
    """
    TestEventProcessingBase._run_test_insert_events_impl(self.hive_client, self.client,
      self.cluster, unique_database, True)

  @CustomClusterTestSuite.with_args(
      impalad_args="--use_local_catalog=true",
      catalogd_args=CATALOGD_ARGS_ENABLE_SYNC_EVENTS)
  def test_non_transactional_insert_events(self, unique_database):
    TestEventProcessingBase._run_test_insert_events_impl(self.hive_client, self.client,
      self.cluster, unique_database, False)

  @SkipIfFS.file_or_folder_name_ends_with_period
  @CustomClusterTestSuite.with_args(
      impalad_args="--use_local_catalog=true",
      catalogd_args=CATALOGD_ARGS_ENABLE_SYNC_EVENTS)
  def test_insert(self, vector, unique_database):
    if (vector.get_value('table_format').file_format == 'parquet'):
      vector.get_value('exec_option')['COMPRESSION_CODEC'] = \
          vector.get_value('compression_codec')
    self.run_test_case('QueryTest/insert', vector, unique_database,
        test_file_vars={'$ORIGINAL_DB': ImpalaTestSuite
        .get_db_name_from_format(vector.get_value('table_format'))})

  @CustomClusterTestSuite.with_args(
      impalad_args="--use_local_catalog=true",
      catalogd_args="--catalog_topic_mode=minimal "
                    "--enable_sync_to_latest_event_on_ddls=true "
                    "--hms_event_polling_interval_s=5")
  def test_sync_event_based_replication(self):
    TestEventProcessingBase._run_event_based_replication_tests_impl(self.hive_client,
      self.client, self.cluster, self.filesystem_client)
