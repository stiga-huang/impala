package org.apache.impala.service;

import java.util.List;

import org.apache.impala.thrift.THistoricalStatsUpdate;
import org.apache.impala.thrift.TScanNodeRun;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

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
    for (String hashKey : stats.scan_node_cards.keySet()) {
      writeScanStats(hashKey, stats.scan_node_cards.get(hashKey));
    }
  }

  public void writeScanStats(String hashKey, List<TScanNodeRun> runs) {
    cacheBackend_.put(hashKey, runs);
    LOG.info("Write HBO key: {}, stats: {}", hashKey, runs);
  }

  public Long getNumRows(String hashKey, String tblName, long numInputRows) {
    Object value = cacheBackend_.getIfPresent(hashKey);
    if (value instanceof List<?>) {
      @SuppressWarnings("unchecked")
      List<TScanNodeRun> runs = (List<TScanNodeRun>) value;
      Preconditions.checkState(runs.size() == 1);
      // TODO: pick the similar run based on the input info once we add
      // canonicalization strategies and the value list have multiple entries.
      TScanNodeRun run = runs.get(0);
      if (run.getNum_input_rows() != numInputRows) {
        LOG.warn("Mismatched HBO numInputRows for {} {}: {} vs {}",
            tblName, hashKey, run.getNum_input_rows(), numInputRows);
      }
      return run.getNum_input_rows();
    } else if (value != null) {
      LOG.warn("Cached value has wrong class: {}", value.getClass().getName());
    }
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
