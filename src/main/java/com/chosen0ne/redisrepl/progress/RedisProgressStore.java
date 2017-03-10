package com.chosen0ne.redisrepl.progress;

import com.chosen0ne.redisrepl.util.LogUtils;
import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.Map;

public class RedisProgressStore implements IProgressStore {
    private static Logger logger = Logger.getLogger(RedisProgressStore.class);

    private JedisPool redisPool;

    public RedisProgressStore(String host, int port) {
        redisPool = new JedisPool(host, port);
    }

    @Override
    public void set(String hostport, Map<String, String> progress) {
        Jedis jedis = null;
        try {
            jedis = redisPool.getResource();
            jedis.hmset(hostport, progress);
        } catch (Exception e) {
            if (jedis != null) {
                redisPool.returnBrokenResource(jedis);
                jedis = null;
            }
            LogUtils.error(logger, e, "failed to hmset, hostport: {0}", hostport);
            throw new RuntimeException("failed to hmset, hostport=" + hostport, e);
        } finally {
            if (jedis != null) {
                redisPool.returnResource(jedis);
            }
        }
    }

    @Override
    public Map<String, String> get(String hostport) {
        Jedis jedis = null;
        Map<String, String> m = null;
        try {
            jedis = redisPool.getResource();
            m = redisPool.getResource().hgetAll(hostport);
        } catch (Exception e) {
            if (jedis != null) {
                redisPool.returnBrokenResource(jedis);
                jedis = null;
            }
            LogUtils.error(logger, e, "failed to hgetall, hostport: {0}", hostport);
            throw new RuntimeException("failed to hgetall, hostport=" + hostport, e);
        } finally {
            if (jedis != null) {
                redisPool.returnResource(jedis);
            }
        }

        return m;
    }
}
