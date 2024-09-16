package org.apache.impala.service;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import org.apache.impala.thrift.THistoryStatsUpdate;
import org.apache.impala.thrift.TScanNodeCardinality;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HistoryStats {
  private final static Logger LOG = LoggerFactory.getLogger(HistoryStats.class);
  public static HistoryStats INSTANCE = new HistoryStats();
  final Cache<String, Object> cache_;

  private HistoryStats() {
    int concurrencyLevel = 4;
    long cacheSizeBytes = 1024L * 1024 * 1024;
    cache_ = CacheBuilder.newBuilder()
        .concurrencyLevel(concurrencyLevel)
        .maximumWeight(cacheSizeBytes)
        //.expireAfterAccess(expirationSecs, TimeUnit.SECONDS)
        .weigher(new HBOWeither())
        .recordStats()
        .build();
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
    cache_.put(key, stats);
    LOG.info("Write HBO key: {}, tableName: {}, stats: {}", key, stats.table_name, stats);
  }

  public Long getNumRows(String fqTblName, long catalogVersion, String conjuncts) {
    Hasher hasher = Hashing.murmur3_128().newHasher();
    hasher.putUnencodedChars(fqTblName);
    hasher.putLong(catalogVersion);
    if (conjuncts != null) hasher.putUnencodedChars(conjuncts);
    String key = hasher.hash().toString();
    LOG.info("Read HBO key: {} for table {}", key, fqTblName);
    Object value = cache_.getIfPresent(key);
    if (value instanceof TScanNodeCardinality) {
      TScanNodeCardinality stats = (TScanNodeCardinality) value;
      return stats.num_rows;
    } else {
      LOG.info("Wrong class");
    }
    return null;
  }

  static class HBOWeither implements Weigher<String, Object> {

    @Override
    public int weigh(String key, Object value) {
      return key.length() + 100;
    }
  }
}
