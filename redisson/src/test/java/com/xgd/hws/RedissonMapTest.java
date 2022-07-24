package com.xgd.hws;

import org.junit.jupiter.api.Test;
import org.redisson.Redisson;
import org.redisson.RedissonMapCache;
import org.redisson.api.EvictionMode;
import org.redisson.api.LocalCachedMapOptions;
import org.redisson.client.codec.StringCodec;

import java.util.concurrent.TimeUnit;

/**
 * @author huwansong
 * @version 1.0, 2022/7/22 19:52
 * @since
 */
class RedissonMapTest {

    private static final Redisson redisson = (Redisson) Redisson.create();

    @Test
    void mapSample() throws InterruptedException {
        LocalCachedMapOptions.defaults();
        RedissonMapCache<String, String> map = (RedissonMapCache) redisson.getMapCache("test-key1", StringCodec.INSTANCE,
            LocalCachedMapOptions.defaults());
        map.put("test-hkey1", "test-value1");
        map.setMaxSize(1000);
        RedissonMapCache<String, String> map2 = (RedissonMapCache) redisson.getMapCache("test-key2", StringCodec.INSTANCE,
            LocalCachedMapOptions.defaults());
        map2.put("test-hkey2", "test-value2");
        // 可以指定淘汰算法 默认LRU LRU,LFU
        map2.setMaxSize(1000, EvictionMode.LFU);
        map2.expire(30, TimeUnit.MINUTES);
        map2.get("test-hkey2");
        Thread.sleep(10 * 1000);
    }
}
