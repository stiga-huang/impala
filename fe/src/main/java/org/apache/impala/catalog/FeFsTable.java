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

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.impala.analysis.LiteralExpr;
import org.apache.impala.catalog.HdfsPartition.FileDescriptor;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.thrift.TPartitionKeyValue;
import org.apache.impala.thrift.TResultSet;
import org.apache.impala.util.ListMap;

/**
 * Frontend interface for interacting with a filesystem-backed table.
 */
public interface FeFsTable extends FeTable {
  /** hive's default value for table property 'serialization.null.format' */
  public static final String DEFAULT_NULL_COLUMN_VALUE = "\\N";

  /**
   * @return true if the table and all its partitions reside at locations which
   * support caching (e.g. HDFS).
   */
  public boolean isCacheable();

  /**
   * @return true if the table resides at a location which supports caching
   * (e.g. HDFS).
   */
  public boolean isLocationCacheable();

  /**
   * @return true if this table is marked as cached
   */
  boolean isMarkedCached();

  /*
   * Returns the storage location (HDFS path) of this table.
   */
  public String getLocation();

  /**
   * @return the value Hive is configured to use for NULL partition key values.
   */
  public String getNullPartitionKeyValue();

  /**
   * Get file info for the given set of partitions, or all partitions if
   * partitionSet is null.
   *
   * @return partition file info, ordered by partition
   */
  TResultSet getFiles(List<List<TPartitionKeyValue>> partitionSet)
      throws CatalogException;

  /**
   * @return the base HDFS directory where files of this table are stored.
   */
  public String getHdfsBaseDir();

  /**
   * @return the total number of bytes stored for this table.
   */
  long getTotalHdfsBytes();

  /**
   * @return true if this table is backed by the Avro file format
   */
  boolean isAvroTable();

  /**
   * @param totalBytes_ the known number of bytes in the table
   * @return Returns an estimated row count for the given number of file bytes
   */
  public long getExtrapolatedNumRows(long totalBytes);

  /**
   * @return true if stats extrapolation is enabled for this table, false otherwise.
   */
  boolean isStatsExtrapolationEnabled();

  /**
   * @return statistics on this table as a tabular result set. Used for the
   * SHOW TABLE STATS statement. The schema of the returned TResultSet is set
   * inside this method.
   */
  public TResultSet getTableStats();

  /**
   * @return all partitions of this table
   */
  Collection<? extends FeFsPartition> getPartitions();

  /**
   * @return identifiers for all partitions in this table
   */
  public Set<Long> getPartitionIds();

  /**
   * @return the map from partition identifier to partition object
   */
  Map<Long, ? extends FeFsPartition> getPartitionMap();

  /**
   * @param the index of the target partitioning column
   * @return a map from value to a set of partitions for which column 'col'
   * has that value.
   */
  TreeMap<LiteralExpr, HashSet<Long>> getPartitionValueMap(int col);

  /**
   * @return the set of partitions which have a null value for column
   * index 'colIdx'.
   */
  Set<Long> getNullPartitionIds(int colIdx);

  /**
   * Parses and returns the value of the 'skip.header.line.count' table property. If the
   * value is not set for the table, returns 0. If parsing fails or a value < 0 is found,
   * the error parameter is updated to contain an error message.
   */
  int parseSkipHeaderLineCount(StringBuilder error);

  /**
   * Selects a random sample of files from the given list of partitions such that the sum
   * of file sizes is at least 'percentBytes' percent of the total number of bytes in
   * those partitions and at least 'minSampleBytes'. The sample is returned as a map from
   * partition id to a list of file descriptors selected from that partition.
   * This function allocates memory proportional to the number of files in 'inputParts'.
   * Its implementation tries to minimize the constant factor and object generation.
   * The given 'randomSeed' is used for random number generation.
   * The 'percentBytes' parameter must be between 0 and 100.
   */
  Map<Long, List<FileDescriptor>> getFilesSample(
      Collection<? extends FeFsPartition> inputParts,
      long percentBytes, long minSampleBytes,
      long randomSeed);

  /**
   * @return the index of hosts that store replicas of blocks of this table.
   */
  ListMap<TNetworkAddress> getHostIndex();
 }
