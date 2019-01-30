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
package org.apache.impala.catalog;

import java.util.List;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.impala.analysis.TableName;
import org.apache.impala.common.InternalException;
import org.apache.impala.thrift.TCatalogObject;
import org.apache.impala.thrift.TPartitionKeyValue;
import org.apache.impala.thrift.TUniqueId;
import org.apache.impala.util.PatternMatcher;
import org.apache.thrift.TException;

/**
 * Interface between the front-end (analysis and planning) classes and the Catalog.
 */
public interface FeCatalog {
  /** @see Catalog#getDbs(PatternMatcher) */
  List<? extends FeDb> getDbs(PatternMatcher matcher);

  /** @see Catalog#getTableNames(String, PatternMatcher) */
  List<String> getTableNames(String dbName, PatternMatcher matcher)
      throws DatabaseNotFoundException;

  /** @see Catalog#getTable(String, String) */
  FeTable getTable(String db_name, String table_name)
      throws DatabaseNotFoundException;

  /** @see Catalog#getTCatalogObject(TCatalogObject) */
  TCatalogObject getTCatalogObject(TCatalogObject objectDesc)
      throws CatalogException;

  /** @see Catalog#getDb(String) */
  FeDb getDb(String db);

  /** @see Catalog#getHdfsPartition(String, String, List) */
  FeFsPartition getHdfsPartition(String db, String tbl,
      List<TPartitionKeyValue> partition_spec) throws CatalogException;

  /** @see Catalog#getDataSources(PatternMatcher) */
  List<DataSource> getDataSources(PatternMatcher createHivePatternMatcher);

  /** @see Catalog#getDataSource(String) */
  // TODO(todd): introduce FeDataSource
  public DataSource getDataSource(String dataSourceName);

  /** @see Catalog#getFunction(Function, Function.CompareMode) */
  // TODO(todd): introduce FeFunction
  public Function getFunction(Function desc, Function.CompareMode mode);

  // TODO(todd): introduce FeFsCachePool
  /** @see Catalog#getHdfsCachePool(String) */
  public HdfsCachePool getHdfsCachePool(String poolName);

  /**
   * Issues a load request to the catalogd for the given tables.
   */
  void prioritizeLoad(Set<TableName> tableNames) throws InternalException;

  /**
   * Causes the calling thread to wait until a catalog update notification has been sent
   * or the given timeout has been reached. A timeout value of 0 indicates an indefinite
   * wait. Does not protect against spurious wakeups, so this should be called in a loop.
   */
  void waitForCatalogUpdate(long timeoutMs);

  /**
   * Returns the FS path where the metastore would create the given table. If the table
   * has a "location" set, that will be returned. Otherwise the path will be resolved
   * based on the location of the parent database. The metastore folder hierarchy is:
   * <warehouse directory>/<db name>.db/<table name>
   * Except for items in the default database which will be:
   * <warehouse directory>/<table name>
   * This method handles both of these cases.
   */
  public Path getTablePath(Table msTbl) throws TException;

  /**
   * @return the ID of the catalog service from which this catalog most recently
   * loaded.
   */
  TUniqueId getCatalogServiceId();

  AuthorizationPolicy getAuthPolicy();
  String getDefaultKuduMasterHosts();


  /**
   * Returns true if the catalog is ready to accept requests (has
   * received and processed a valid catalog topic update from the StateStore),
   * false otherwise.
   */
  boolean isReady();

  /**
   * Force the catalog into a particular readiness state.
   * Used only by tests.
   */
  void setIsReady(boolean isReady);

}
