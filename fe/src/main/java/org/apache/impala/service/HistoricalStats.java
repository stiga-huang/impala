package org.apache.impala.service;

import org.apache.impala.thrift.THistoricalStatsUpdate;
import org.apache.impala.thrift.TScanNodeCardinality;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  public void writeScanStats(String hashKey, TScanNodeCardinality stats) {
    cacheBackend_.put(hashKey, stats);
    LOG.info("Write HBO key: {}, stats: {}", hashKey, stats);
  }

  public Long getNumRows(String hashKey) {
    Object value = cacheBackend_.getIfPresent(hashKey);
    if (value instanceof TScanNodeCardinality) {
      TScanNodeCardinality stats = (TScanNodeCardinality) value;
      return stats.num_rows;
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
