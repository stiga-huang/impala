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

import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.exceptions.JedisException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Base64;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Redis-based cache backend implementation.
 * This backend stores cache entries in a Redis server, allowing
 * cache sharing across multiple Impala coordinators.
 */
public class RedisCacheBackend implements CacheBackend {
  private final static Logger LOG = LoggerFactory.getLogger(RedisCacheBackend.class);
  private final JedisPool jedisPool_;
  private final AtomicLong hits_ = new AtomicLong(0);
  private final AtomicLong misses_ = new AtomicLong(0);
  private final AtomicLong errors_ = new AtomicLong(0);

  /**
   * Create a Redis cache backend.
   * @param host Redis server hostname
   * @param port Redis server port
   * @param password Redis server password (null or empty if no auth required)
   * @param database Redis database number
   * @param timeoutMs Connection and operation timeout in milliseconds
   */
  public RedisCacheBackend(String host, int port, String password, 
      int database, int timeoutMs) {
    JedisPoolConfig poolConfig = new JedisPoolConfig();
    poolConfig.setMaxTotal(16);
    poolConfig.setMaxIdle(8);
    poolConfig.setMinIdle(2);
    poolConfig.setTestOnBorrow(true);
    poolConfig.setTestOnReturn(false);
    poolConfig.setTestWhileIdle(true);

    if (Strings.isNullOrEmpty(password)) {
      jedisPool_ = new JedisPool(poolConfig, host, port, timeoutMs, null, database);
    } else {
      jedisPool_ = new JedisPool(poolConfig, host, port, timeoutMs, password, database);
    }
    LOG.info("Initialized Redis cache backend: {}:{} db={}", host, port, database);
  }

  @Override
  public void put(String key, Object value) {
    try (Jedis jedis = jedisPool_.getResource()) {
      String serialized = serializeObject(value);
      jedis.set(key, serialized);
    } catch (JedisException | IOException e) {
      errors_.incrementAndGet();
      LOG.warn("Failed to put key {} to Redis: {}", key, e.getMessage());
    }
  }

  @Override
  public Object getIfPresent(String key) {
    try (Jedis jedis = jedisPool_.getResource()) {
      String serialized = jedis.get(key);
      if (serialized != null) {
        hits_.incrementAndGet();
        return deserializeObject(serialized);
      } else {
        misses_.incrementAndGet();
        return null;
      }
    } catch (JedisException | IOException | ClassNotFoundException e) {
      errors_.incrementAndGet();
      LOG.warn("Failed to get key {} from Redis: {}", key, e.getMessage());
      return null;
    }
  }

  @Override
  public String getStats() {
    return String.format("Redis cache stats - hits: %d, misses: %d, errors: %d",
        hits_.get(), misses_.get(), errors_.get());
  }

  /**
   * Serialize an object to a Base64-encoded string.
   */
  private String serializeObject(Object obj) throws IOException {
    ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
    try (ObjectOutputStream objectOut = new ObjectOutputStream(byteOut)) {
      objectOut.writeObject(obj);
      objectOut.flush();
      return Base64.getEncoder().encodeToString(byteOut.toByteArray());
    }
  }

  /**
   * Deserialize an object from a Base64-encoded string.
   */
  private Object deserializeObject(String str) throws IOException, ClassNotFoundException {
    byte[] bytes = Base64.getDecoder().decode(str);
    ByteArrayInputStream byteIn = new ByteArrayInputStream(bytes);
    try (ObjectInputStream objectIn = new ObjectInputStream(byteIn)) {
      return objectIn.readObject();
    }
  }

  /**
   * Close the Redis connection pool.
   * Should be called during shutdown.
   */
  public void close() {
    if (jedisPool_ != null && !jedisPool_.isClosed()) {
      jedisPool_.close();
      LOG.info("Redis connection pool closed");
    }
  }
}

