package org.apache.impala.service;

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import org.apache.impala.thrift.THistoryStatsUpdate;
import org.apache.impala.thrift.TScanNodeCardinality;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HistoryStats {
  private final static Logger LOG = LoggerFactory.getLogger(HistoryStats.class);
  public static HistoryStats INSTANCE = new HistoryStats();
  private final CacheBackend cacheBackend_;

  private HistoryStats() {
    // Determine which cache backend to use based on configuration
    if (BackendConfig.INSTANCE != null && 
        BackendConfig.INSTANCE.historyStatsUseRedis()) {
      LOG.info("Initializing HistoryStats with Redis cache backend");
      cacheBackend_ = new RedisCacheBackend(
          BackendConfig.INSTANCE.historyStatsRedisHost(),
          BackendConfig.INSTANCE.historyStatsRedisPort(),
          BackendConfig.INSTANCE.historyStatsRedisPassword(),
          BackendConfig.INSTANCE.historyStatsRedisDb(),
          BackendConfig.INSTANCE.historyStatsRedisTimeoutMs()
      );
    } else {
      LOG.info("Initializing HistoryStats with in-memory cache backend");
      int concurrencyLevel = 4;
      long cacheSizeBytes = 1024L * 1024 * 1024;
      cacheBackend_ = new InMemoryCacheBackend(concurrencyLevel, cacheSizeBytes);
    }
  }

  public void writeStats(THistoryStatsUpdate stats) {
    for (TScanNodeCardinality ss : stats.scan_node_cards) {
      writeScanStats(ss);
    }
  }

  public void writeScanStats(TScanNodeCardinality stats) {
    Hasher hasher = Hashing.murmur3_128().newHasher();
    hasher.putUnencodedChars(stats.table_name);
    hasher.putLong(stats.catalog_version);
    if (stats.isSetConjuncts_string()) {
      hasher.putUnencodedChars(stats.conjuncts_string);
    }
    String key = hasher.hash().toString();
    cacheBackend_.put(key, stats);
    LOG.info("Write HBO key: {}, tableName: {}, stats: {}", key, stats.table_name, stats);
  }

  public Long getNumRows(String fqTblName, long catalogVersion, String conjuncts) {
    Hasher hasher = Hashing.murmur3_128().newHasher();
    hasher.putUnencodedChars(fqTblName);
    hasher.putLong(catalogVersion);
    if (conjuncts != null) hasher.putUnencodedChars(conjuncts);
    String key = hasher.hash().toString();
    LOG.info("Read HBO key: {} for table {}", key, fqTblName);
    Object value = cacheBackend_.getIfPresent(key);
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
