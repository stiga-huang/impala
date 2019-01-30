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

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.impala.thrift.TDatabase;
import org.apache.impala.thrift.TFunctionCategory;
import org.apache.impala.util.PatternMatcher;

/**
 * Frontend interface for interacting with a database.
 */
public interface FeDb {
  /**
   * @return the name of the database
   */
  String getName();

  /**
   * @return the metastore.api.Database object this Database was created from,
   * or null if it is not related to a hive database such as builtins_db.
   */
  Database getMetaStoreDb();

  /**
   * @return true if the database contains a table with the given name
   */
  boolean containsTable(String tableName);

  /**
   * @return the table with the given name
   */
  FeTable getTable(String tbl);

  /**
   * @return the names of the tables within this database
   */
  List<String> getAllTableNames();

  /**
   * @return true if this is a system database (i.e. cannot be dropped,
   * modified, etc)
   */
  boolean isSystemDb();

  // TODO(todd): can we simplify the many related 'getFunctions' calls
  // in this interface?

  /**
   * @see Catalog#getFunction(Function, Function.CompareMode)
   */
  public Function getFunction(Function desc, Function.CompareMode mode);

  /**
   * @return all functions with the given name
   */
  List<Function> getFunctions(String functionName);

  /**
   * @return all functions with the given category and name
   */
  List<Function> getFunctions(TFunctionCategory category, String function);

  /**
   * @return all functions with the given category that match the given pattern
   */
  List<Function> getFunctions(TFunctionCategory category,
      PatternMatcher patternMatcher);

  /**
   * @return the number of functions in this database.
   */
  int numFunctions();

  /**
   * @see Catalog#containsFunction(org.apache.impala.analysis.FunctionName)
   */
  boolean containsFunction(String function);

  /**
   * @return the Thrift-serialized structure for this database
   */
  TDatabase toThrift();
}
