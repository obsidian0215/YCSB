/**
 * Copyright (c) 2012 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

/**
 * Redis client binding for YCSB.
 *
 * All YCSB records are mapped to a Redis *hash field*.  For scanning
 * operations, all keys are saved (by an arbitrary hash) in a sorted set.
 */

package site.ycsb.db;

import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;
import redis.clients.jedis.BasicCommands;

// 添加日志框架导入 (使用Java标准库避免添加新Maven依赖)
import java.util.logging.Logger;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.Protocol;

import java.io.Closeable;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.util.HashMap;
import java.util.Map;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

/**
 * YCSB binding for <a href="http://redis.io/">Redis</a>.
 *
 * See {@code redis/README.md} for details.
 */
/**
 * Redis client binding for YCSB.
 *
 * All YCSB records are mapped to a Redis *hash field*.  For scanning
 * operations, all keys are saved (by an arbitrary hash) in a sorted set.
 */
public class RedisClient extends DB {

  private static final Logger LOGGER = Logger.getLogger(RedisClient.class.getName());

  private JedisCommands jedis;

  public static final String HOST_PROPERTY = "redis.host";
  public static final String PORT_PROPERTY = "redis.port";
  public static final String PASSWORD_PROPERTY = "redis.password";
  public static final String CLUSTER_PROPERTY = "redis.cluster";
  public static final String TIMEOUT_PROPERTY = "redis.timeout";

  public static final String INDEX_KEY = "_indices";

  public void init() throws DBException {
    Properties props = getProperties();
    int port;

    String portString = props.getProperty(PORT_PROPERTY);
    if (portString != null) {
      port = Integer.parseInt(portString);
      LOGGER.info("Redis port configured: " + port);
    } else {
      port = Protocol.DEFAULT_PORT;
      LOGGER.info("Using default Redis port: " + port);
    }
    String host = props.getProperty(HOST_PROPERTY);
    LOGGER.info("Redis host configured: " + host);

    // ======= Enhanced timeout configuration processing (simplified) =======
    String redisTimeout = props.getProperty(TIMEOUT_PROPERTY);

    // Use default if not specified
    int connectionTimeout = 3000; // 3s default connection timeout
    int soTimeout = 5000; // 5s default socket read timeout

    if (redisTimeout != null) {
      // Parse flexible redis.timeout format: "connectionTimeout:multiplier" or "connectionTimeout"
      if (redisTimeout.contains(":")) {
        // Split by colon and parse multiplier
        String[] parts = redisTimeout.split(":");
        connectionTimeout = Integer.parseInt(parts[0]);
        double multiplier = Double.parseDouble(parts[1]);
        soTimeout = (int) Math.max(connectionTimeout * multiplier, 1000); // Minimum 1000ms
        LOGGER.info(String.format("Using flexible timeout format: %s -> connectionTimeout=%dms * %.2f = soTimeout=%dms",
                        redisTimeout, connectionTimeout, multiplier, soTimeout));
      } else {
        // Default use connectionTimeout * 2 for soTimeout
        connectionTimeout = Integer.parseInt(redisTimeout);
        soTimeout = Math.max(connectionTimeout * 2, 1000); // Minimum 1000ms
        LOGGER.info("Using standard timeout format: " + redisTimeout + "ms -> connectionTimeout=" + connectionTimeout +
                    "ms, soTimeout=" + soTimeout + "ms (2x factor)");
      }
    } else {
      LOGGER.warning("No redis.timeout specified, using defaults: connectionTimeout=" + connectionTimeout +
                     "ms, soTimeout=" + soTimeout + "ms");
    }

    boolean clusterEnabled = Boolean.parseBoolean(props.getProperty(CLUSTER_PROPERTY));
    if (clusterEnabled) {
      LOGGER.warning("Cluster mode detected: enhanced timeout configuration will be used for cluster");
      Set<HostAndPort> jedisClusterNodes = new HashSet<>();
      jedisClusterNodes.add(new HostAndPort(host, port));
      // For cluster, we still create JedisCluster without explicit timeout
      jedis = new JedisCluster(jedisClusterNodes);
      LOGGER.info("JedisCluster created: timeout control may be limited");
    } else {
      LOGGER.info("Standalone mode: creating Jedis instance with enhanced timeout control");

      // Use 4-parameter constructor to fix timeout transmission: both connectionTimeout and soTimeout
      LOGGER.info(String.format("Creating Jedis instance with dual timeout: host=%s, port=%d, " +
                                "connectionTimeout=%dms, soTimeout=%dms (was: %s)",
                                host, port, connectionTimeout, soTimeout,
                                redisTimeout != null ? "configured" : "default"));
      jedis = new Jedis(host, port, connectionTimeout, soTimeout);

      try {
        LOGGER.info("Calling Jedis.connect() with enhanced timeout parameters...");
        ((Jedis) jedis).connect();

        // Configure TCP Keepalive after successful connection
        try {
          Socket jedisSocket = ((Jedis) jedis).getClient().getSocket();
          jedisSocket.setKeepAlive(true);
          jedisSocket.setSoLinger(true, 0); // Optimize for immediate close
          LOGGER.info("TCP Keepalive enabled for connection stability");
        } catch (SocketException e) {
          LOGGER.warning("Failed to configure TCP Keepalive, connection may be less stable: " + e.getMessage());
        }

        LOGGER.info("Jedis connection established successfully with enhanced timeout control");
      } catch (Exception e) {
        LOGGER.severe("Jedis connection FAILED: " + e.getMessage());
        throw e;
      }
    }

    String password = props.getProperty(PASSWORD_PROPERTY);
    if (password != null) {
      LOGGER.info("Password configured, attempting Redis authentication");
      ((BasicCommands) jedis).auth(password);
      LOGGER.info("Redis authentication completed");
    } else {
      LOGGER.info("No Redis password configured");
    }
  }

  public void cleanup() throws DBException {
    try {
      ((Closeable) jedis).close();
    } catch (IOException e) {
      throw new DBException("Closing connection failed.");
    }
  }

  /*
   * Calculate a hash for a key to store it in an index. The actual return value
   * of this function is not interesting -- it primarily needs to be fast and
   * scattered along the whole space of doubles. In a real world scenario one
   * would probably use the ASCII values of the keys.
   */
  private double hash(String key) {
    return key.hashCode();
  }

  // XXX jedis.select(int index) to switch to `table`

  @Override
  public Status read(String table, String key, Set<String> fields,
      Map<String, ByteIterator> result) {
    if (fields == null) {
      StringByteIterator.putAllAsByteIterators(result, jedis.hgetAll(key));
    } else {
      String[] fieldArray =
          (String[]) fields.toArray(new String[fields.size()]);
      List<String> values = jedis.hmget(key, fieldArray);

      Iterator<String> fieldIterator = fields.iterator();
      Iterator<String> valueIterator = values.iterator();

      while (fieldIterator.hasNext() && valueIterator.hasNext()) {
        result.put(fieldIterator.next(),
            new StringByteIterator(valueIterator.next()));
      }
      assert !fieldIterator.hasNext() && !valueIterator.hasNext();
    }
    return result.isEmpty() ? Status.ERROR : Status.OK;
  }

  @Override
  public Status insert(String table, String key,
      Map<String, ByteIterator> values) {
    if (jedis.hmset(key, StringByteIterator.getStringMap(values))
        .equals("OK")) {
      jedis.zadd(INDEX_KEY, hash(key), key);
      return Status.OK;
    }
    return Status.ERROR;
  }

  @Override
  public Status delete(String table, String key) {
    return jedis.del(key) == 0 && jedis.zrem(INDEX_KEY, key) == 0 ? Status.ERROR
        : Status.OK;
  }

  @Override
  public Status update(String table, String key,
      Map<String, ByteIterator> values) {
    return jedis.hmset(key, StringByteIterator.getStringMap(values))
        .equals("OK") ? Status.OK : Status.ERROR;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
      Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    Set<String> keys = jedis.zrangeByScore(INDEX_KEY, hash(startkey),
        Double.POSITIVE_INFINITY, 0, recordcount);

    HashMap<String, ByteIterator> values;
    for (String key : keys) {
      values = new HashMap<String, ByteIterator>();
      read(table, key, fields, values);
      result.add(values);
    }

    return Status.OK;
  }

}
