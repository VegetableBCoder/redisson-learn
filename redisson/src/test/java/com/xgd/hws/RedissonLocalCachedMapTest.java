package com.xgd.hws;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.redisson.Redisson;
import org.redisson.api.LocalCachedMapOptions;
import org.redisson.api.MapOptions;
import org.redisson.api.RLocalCachedMap;
import org.redisson.api.RMapCache;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.LongCodec;

import java.util.concurrent.TimeUnit;

/**
 * @Classname RedissonLocalCachedMapTest
 * @Description TODO
 * @Date 2022/7/9 10:10
 * @Created by huwansong
 */
class RedissonLocalCachedMapTest {

    private static RedissonClient redisson;

    @BeforeAll
    static void createClient() {
        redisson = Redisson.create();
    }

    @Test
    void redissonCachedMap() {
        LocalCachedMapOptions options = LocalCachedMapOptions
            .defaults()
            .timeToLive(10, TimeUnit.SECONDS)
            .maxIdle(5, TimeUnit.SECONDS)
            .cacheSize(1000)
            .evictionPolicy(LocalCachedMapOptions.EvictionPolicy.LRU)
            .reconnectionStrategy(LocalCachedMapOptions.ReconnectionStrategy.CLEAR)
            .syncStrategy(LocalCachedMapOptions.SyncStrategy.INVALIDATE);
        RLocalCachedMap<String, Long> localCachedMap = redisson.getLocalCachedMap("test-local", LongCodec.INSTANCE, options);

        localCachedMap.put("test", 123L);
    }

    @Test
    void redissonMapCache() throws InterruptedException {
        MapOptions options = MapOptions.defaults().writeMode(MapOptions.WriteMode.WRITE_BEHIND);
        RMapCache<String, Long> cache = redisson.getMapCache("test-cache", LongCodec.INSTANCE, options);
        cache.put("test", 1111L, 1, TimeUnit.MINUTES);
        cache.put("test2", 2222L, 45, TimeUnit.SECONDS);
        Thread.sleep(120 * 1000);
    }

}
