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

package org.apache.impala.service;

import java.util.List;

import org.apache.impala.thrift.THboStatsType;
import org.apache.impala.thrift.THistoricalStatsUpdate;
import org.apache.impala.thrift.TPlanNodeRun;
import org.apache.impala.thrift.TPlanNodeRunWithKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class HistoricalStats {
  private final static Logger LOG = LoggerFactory.getLogger(HistoricalStats.class);
  public static HistoricalStats INSTANCE = new HistoricalStats();
  private final CacheBackend cacheBackend_;
  private final double similarityThreshold_;
  private final int maxRunsPerKey_;

  private HistoricalStats() {
    int concurrencyLevel = BackendConfig.INSTANCE != null
        ? BackendConfig.INSTANCE.getHboInMemoryBackendConcurrencyLevel()
        : 4;
    long cacheSizeBytes = BackendConfig.INSTANCE != null
        ? BackendConfig.INSTANCE.getHboInMemoryBackendCacheSizeBytes()
        : 1024L * 1024 * 1024;
    cacheBackend_ = new InMemoryCacheBackend(concurrencyLevel, cacheSizeBytes);
    similarityThreshold_ = getSimilarityThreshold();
    maxRunsPerKey_ = getMaxRunsPerKey();
  }

  private double getSimilarityThreshold() {
    return BackendConfig.INSTANCE != null
        ? BackendConfig.INSTANCE.getHboSimilarityThreshold()
        : 0.1;
  }

  private int getMaxRunsPerKey() {
    return BackendConfig.INSTANCE != null
        ? BackendConfig.INSTANCE.getHboMaxRunsPerKey()
        : 100;
  }

  public void writeStats(THistoricalStatsUpdate stats) {
    for (TPlanNodeRunWithKeys runWithKeys : stats.plan_node_runs) {
      writeScanStats(runWithKeys.run, runWithKeys.hash_keys, runWithKeys.stats_type);
    }
  }

  private boolean catalogVersionMatches(TPlanNodeRun a, TPlanNodeRun b) {
    if (a.isSetCatalog_version() && b.isSetCatalog_version()) {
      return a.getCatalog_version() == b.getCatalog_version();
    }
    return false;
  }

  private boolean scanInputSizeMatches(TPlanNodeRun a, TPlanNodeRun b) {
    if (a.isSetInput_file_size() && b.isSetInput_file_size()) {
      long x = a.getInput_file_size();
      long y = b.getInput_file_size();
      if (x == 0) return y == 0;
      return Math.abs(x - y) / (double)x < similarityThreshold_;
    }
    return false;
  }

  private int foundSimilarRunWithoutStats(List<TPlanNodeRun> runs, TPlanNodeRun currRun) {
    if (currRun.isSetScan_input_rows()) {
      for (long n : currRun.getScan_input_rows()) {
        Preconditions.checkState(n < 0,
             "exactMatch is only used when missing numRows but got %s", n);
      }
    }
    // For exact match, first find a run with the exact catalog version. If missing,
    // find a run with the similar scan input size. Note that the hash key matching
    // already ensures conjuncts are the same.
    int sizeMatchIndex = -1;
    for (int i = 0; i < runs.size(); i++) {
      TPlanNodeRun run = runs.get(i);
      if (catalogVersionMatches(run, currRun)) return i;
      if (scanInputSizeMatches(run, currRun)) sizeMatchIndex = i;
    }
    return sizeMatchIndex;
  }

  private int foundSimilarRunWithNumRows(List<TPlanNodeRun> runs, TPlanNodeRun currRun) {
    for (int i = 0; i < runs.size(); i++) {
      TPlanNodeRun run = runs.get(i);
      // Currently HBO only supports HdfsScanNode which just has one table thus only one
      // scan_input_rows. We just need to compare the first one.
      long curr = currRun.getScan_input_rows().get(0);
      long historical = run.getScan_input_rows().get(0);
      if (curr == 0) {
        if (historical == 0) {
          return i;
        }
      } else if (Math.abs(historical - curr) / (double)curr < similarityThreshold_) {
        return i;
      }
    }
    return -1;
  }

  public void writeScanStats(TPlanNodeRun currRun, List<String> hashKeys,
      THboStatsType statsType) {
    // TODO: handle races from concurrent writers.
    for (String hashKey : hashKeys) {
      @SuppressWarnings("unchecked")
      HistoricalStatsValue<TPlanNodeRun> statsValue =
          (HistoricalStatsValue<TPlanNodeRun>) cacheBackend_.getIfPresent(
              statsType, hashKey);
      if (statsValue == null) {
        cacheBackend_.put(statsType, hashKey, new HistoricalStatsValue<>(currRun));
        LOG.debug("Write HBO key: {}, stats type: {}, stats: {}",
            hashKey, statsType, currRun);
      } else {
        List<TPlanNodeRun> runs = statsValue.getRuns();
        int similarRunIndex = foundSimilarRunWithNumRows(runs, currRun);
        if (similarRunIndex >= 0) {
          // Remove the similar one since we are adding a newer run.
          runs.remove(similarRunIndex);
        }
        if (runs.size() >= maxRunsPerKey_) {
          // Remove the oldest run since we are at the limit.
          runs.remove(0);
        }
        runs.add(currRun);
        cacheBackend_.put(statsType, hashKey, statsValue);
        LOG.debug("Write HBO key: {}, stats type: {}, stats: {}",
            hashKey, statsType, currRun);
      }
    }
  }

  /**
   * Retrieves the number of output rows from historical stats, trying multiple hash keys
   * in order from most accurate to most aggressive canonicalization strategy.
   * Returns the first match found, or null if no match exists.
   *
   * @param hashKeys List of hash keys to try (ordered by canonicalization strategy)
   * @return Number of rows from matched historical run, or null if no match
   */
  public Long getNumScanOutputRows(List<String> hashKeys, String tblName,
      TPlanNodeRun currRun) {
    Preconditions.checkNotNull(currRun.getScan_input_rows());
    for (int i = 0; i < hashKeys.size(); i++) {
      // If scanInputRows is unknown, only allow exact match, i.e. EXPR_REWRITE strategy
      // with catalog version match.
      if (currRun.getScan_input_rows().get(0) < 0 && i > 0) break;
      String hashKey = hashKeys.get(i);
      @SuppressWarnings("unchecked")
      HistoricalStatsValue<TPlanNodeRun> statsValue =
          (HistoricalStatsValue<TPlanNodeRun>) cacheBackend_.getIfPresent(
              THboStatsType.CARDINALITY, hashKey);
      if (statsValue != null) {
        List<TPlanNodeRun> runs = statsValue.getRuns();
        int similarRunIndex;
        if (currRun.getScan_input_rows().get(0) < 0 && i == 0) {
          similarRunIndex = foundSimilarRunWithoutStats(runs, currRun);
        } else {
          similarRunIndex = foundSimilarRunWithNumRows(runs, currRun);
        }
        if (similarRunIndex >= 0) {
          // TODO: Consider moving this to the tail.
          LOG.debug("HBO cache hit for {} using strategy level {} (key: {}, currRun: {}):"
                  + "cardinality={}",
              tblName, i, hashKey, currRun, runs.get(similarRunIndex).getNum_rows());
          return runs.get(similarRunIndex).getNum_rows();
        } else {
          LOG.debug("HBO cache miss for {} using strategy level {} (key: {}, scanInputRows: {}). No similar run",
          tblName, i, hashKey, currRun);
        }
      } else {
        LOG.debug("HBO cache miss for {} using strategy level {} (key: {}, scanInputRows: {}). Hash key not found",
        tblName, i, hashKey, currRun);
      }
    }
    // No match found with any strategy
    return null;
  }

  /**
   * Get statistics about the cache backend.
   * @return Cache statistics string
   */
  public String getCacheStats() {
    return cacheBackend_.getStats();
  }
}
