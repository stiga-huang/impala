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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;

/**
 * In-memory cache backend implementation using Guava Cache.
 * This is the default cache backend that stores data in local memory.
 */
public class InMemoryCacheBackend implements CacheBackend {
  private final Cache<String, Object> cache_;

  /**
   * Weigher implementation for calculating the memory footprint of cache entries.
   */
  static class HBOWeigher implements Weigher<String, Object> {
    @Override
    public int weigh(String key, Object value) {
      return key.length() + 100;
    }
  }

  /**
   * Create an in-memory cache backend with default configuration.
   */
  public InMemoryCacheBackend() {
    this(4, 1024L * 1024 * 1024);
  }

  /**
   * Create an in-memory cache backend with custom configuration.
   * @param concurrencyLevel The estimated number of concurrent threads
   * @param cacheSizeBytes The maximum weight (approximate memory) of the cache
   */
  public InMemoryCacheBackend(int concurrencyLevel, long cacheSizeBytes) {
    cache_ = CacheBuilder.newBuilder()
        .concurrencyLevel(concurrencyLevel)
        .maximumWeight(cacheSizeBytes)
        .weigher(new HBOWeigher())
        .recordStats()
        .build();
  }

  @Override
  public void put(String key, Object value) {
    cache_.put(key, value);
  }

  @Override
  public Object getIfPresent(String key) {
    return cache_.getIfPresent(key);
  }

  @Override
  public String getStats() {
    return cache_.stats().toString();
  }
}

