package org.apache.impala.service;

import java.util.List;

import org.apache.impala.thrift.THistoricalStatsUpdate;
import org.apache.impala.thrift.TPlanNodeRun;
import org.apache.impala.thrift.TPlanNodeRunWithKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class HistoricalStats {
  private final static Logger LOG = LoggerFactory.getLogger(HistoricalStats.class);
  public static HistoricalStats INSTANCE = new HistoricalStats();
  private final CacheBackend cacheBackend_;

  private HistoricalStats() {
    // Determine which cache backend to use based on configuration
    if (BackendConfig.INSTANCE != null && 
        BackendConfig.INSTANCE.historyStatsUseRedis()) {
      LOG.info("Initializing HistoricalStats with Redis cache backend");
      cacheBackend_ = new RedisCacheBackend(
          BackendConfig.INSTANCE.historyStatsRedisHost(),
          BackendConfig.INSTANCE.historyStatsRedisPort(),
          BackendConfig.INSTANCE.historyStatsRedisPassword(),
          BackendConfig.INSTANCE.historyStatsRedisDb(),
          BackendConfig.INSTANCE.historyStatsRedisTimeoutMs()
      );
    } else {
      LOG.info("Initializing HistoricalStats with in-memory cache backend");
      int concurrencyLevel = 4;
      long cacheSizeBytes = 1024L * 1024 * 1024;
      cacheBackend_ = new InMemoryCacheBackend(concurrencyLevel, cacheSizeBytes);
    }
  }

  public void writeStats(THistoricalStatsUpdate stats) {
    for (TPlanNodeRunWithKeys runWithKeys : stats.plan_node_runs) {
      writeScanStats(runWithKeys.run, runWithKeys.hash_keys);
    }
  }

  public void writeScanStats(TPlanNodeRun currRun, List<String> hashKeys) {
    for (String hashKey : hashKeys) {
      Object value = cacheBackend_.getIfPresent(hashKey);
      if (value == null) {
        cacheBackend_.put(hashKey, Lists.newArrayList(currRun));
        LOG.debug("Write HBO key: {}, stats: {}", hashKey, currRun);
      } else if (value instanceof List<?>) {
        @SuppressWarnings("unchecked")
        List<TPlanNodeRun> runs = (List<TPlanNodeRun>) value;
        boolean foundSimilar = false;
        for (TPlanNodeRun run : runs) {
          long currInputRows = currRun.num_input_rows;
          long historicalInputRows = run.num_input_rows;
          if (Math.abs(historicalInputRows - currInputRows) / currInputRows <= 0.1) {
            LOG.trace("Ignored HBO stats for {} with strategy level {} (key: {}) because the number of input rows is similar ({} vs {})",
                hashKey, historicalInputRows, currInputRows);
            foundSimilar = true;
            break;
          }
        }
        if (foundSimilar) continue;
        // TODO: make 100 configurable
        if (runs.size() >= 100) {
          runs.remove(0);
        }
        runs.add(currRun);
        cacheBackend_.put(hashKey, runs);
        LOG.debug("Write HBO key: {}, stats: {}", hashKey, currRun);
      }
    }
  }

  /**
   * Retrieves the number of rows from historical stats, trying multiple hash keys
   * in order from most accurate to most aggressive canonicalization strategy.
   * Returns the first match found, or null if no match exists.
   *
   * @param hashKeys List of hash keys to try (ordered by accuracy)
   * @param tblName Table name for logging
   * @param numInputRows Expected number of input rows
   * @return Number of rows from matched historical run, or null if no match
   */
  public Long getNumRows(List<String> hashKeys, String tblName, long numInputRows) {
    if (numInputRows == 0) return 0L;
    for (int i = 0; i < hashKeys.size(); i++) {
      String hashKey = hashKeys.get(i);
      Object value = cacheBackend_.getIfPresent(hashKey);
      if (value instanceof List<?>) {
        @SuppressWarnings("unchecked")
        List<TPlanNodeRun> runs = (List<TPlanNodeRun>) value;
        for (TPlanNodeRun run : runs) {
          long historicalNumInputRows = run.getNum_input_rows();
          // TODO: make 0.1 configurable
          if (Math.abs(historicalNumInputRows - numInputRows) / numInputRows > 0.1) {
            LOG.trace("Ignored HBO stats for {} with strategy level {} (key: {}) because the number of input rows is too different ({} vs {})",
                tblName, i, hashKey, historicalNumInputRows, numInputRows);
            continue;
          }
          LOG.info("HBO cache hit for {} using strategy level {} (key: {}, numInputRows: {}): cardinality={}",
              tblName, i, hashKey, numInputRows, run.getNum_rows());
          return run.getNum_rows();
        }
      } else if (value != null) {
        LOG.warn("Cached value has wrong class: {}", value.getClass().getName());
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
