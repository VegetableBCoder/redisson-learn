package com.xgd.hws;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.redisson.Redisson;
import org.redisson.RedissonLocalCachedMap;
import org.redisson.api.LocalCachedMapOptions;
import org.redisson.api.RedissonClient;
import org.redisson.cache.CacheValue;
import org.redisson.client.codec.LongCodec;
import org.redisson.codec.JsonJacksonCodec;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

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

    @AfterAll
    static void close() {
        redisson.shutdown();
    }

    @Test
    void redissonCachedMap() {
        // 构造方法是protected的 使用defaults来创建
        LocalCachedMapOptions<String, Long> options = LocalCachedMapOptions.<String, Long>defaults()
            // 本地缓存存活时长
            .timeToLive(100, TimeUnit.SECONDS)
            // 最大空闲时长
            .maxIdle(50, TimeUnit.SECONDS)
            // 本地缓存容量
            .cacheSize(1000)
            // 使用默认实现
            .cacheProvider(LocalCachedMapOptions.CacheProvider.REDISSON)
            // 存储方式 仅本地缓存/本地+redis (默认本地+redis)
            .storeMode(LocalCachedMapOptions.StoreMode.LOCALCACHE_REDIS)
            // 淘汰策略 LRU,LFU算法及由jvm管理的软引用,弱引用 和不淘汰
            .evictionPolicy(LocalCachedMapOptions.EvictionPolicy.LRU)
            // 断线重连之后动作
            // NONE 无操作(默认)
            // CLEAR 清空本地缓存 个人认为用CLEAR会比较好
            // Load (保存10分钟 如果10分钟内重连成功则清除失效的hash条目 10分钟以上本地缓存全部清除)
            .reconnectionStrategy(LocalCachedMapOptions.ReconnectionStrategy.CLEAR)
            // 缓存同步策略
            // INVALIDATE redis内的数据变更时 删本地缓存(默认)
            // UPDATE 更新本地缓存
            // NONE 无操作
            .syncStrategy(LocalCachedMapOptions.SyncStrategy.INVALIDATE);
        RedissonLocalCachedMap<String, Long> localCachedMap = (RedissonLocalCachedMap<String, Long>) redisson
            .getLocalCachedMap("test-local", LongCodec.INSTANCE, options);
        // 使用>127的数据 防止常量池导致判断对象是不是同一个出错
        localCachedMap.put("test", Long.MAX_VALUE);
        localCachedMap.get("test");
        // 看看相同的设置能不能使用同一个本地缓存
        RedissonLocalCachedMap<String, Long> localCachedMap2 = (RedissonLocalCachedMap<String, Long>) redisson
            .getLocalCachedMap("test-local", LongCodec.INSTANCE, options);
        // 带有本地缓存的
        localCachedMap2.get("test");
        // 在这里debug 判断cache中缓存的是不是同一个对象 这个方法是自己加的
        CacheValue cachedValue1 = localCachedMap.getCacheObject("test");
        CacheValue cachedValue2 = localCachedMap2.getCacheObject("test");
        // 注意 这两个Long 仅仅是值相同 不是同一个对象 说明多个RedissonLocalCachedMap没法共用相同的本地缓存 甚至仅仅共享相同的对象
        assertEquals(cachedValue1.getValue(), cachedValue2.getValue());
        assertFalse(cachedValue1.getValue() == cachedValue2.getValue());
        localCachedMap.destroy();
        localCachedMap2.destroy();
    }

    @Test
    void testCustomClass() throws InterruptedException {
        LocalCachedMapOptions<String, TestClass> options = LocalCachedMapOptions.<String, TestClass>defaults()
            .timeToLive(100, TimeUnit.SECONDS)
            .maxIdle(50, TimeUnit.SECONDS)
            .cacheSize(1000)
            .cacheProvider(LocalCachedMapOptions.CacheProvider.REDISSON)
            .storeMode(LocalCachedMapOptions.StoreMode.LOCALCACHE_REDIS)
            .evictionPolicy(LocalCachedMapOptions.EvictionPolicy.LRU)
            .reconnectionStrategy(LocalCachedMapOptions.ReconnectionStrategy.CLEAR)
            .syncStrategy(LocalCachedMapOptions.SyncStrategy.INVALIDATE);
        TestClass data = new TestClass("a", "b");
        RedissonLocalCachedMap<String, TestClass> localCachedMap1 = (RedissonLocalCachedMap<String, TestClass>) redisson
            .getLocalCachedMap("test-local-cos", JsonJacksonCodec.INSTANCE, options);
        RedissonLocalCachedMap<String, TestClass> localCachedMap2 = (RedissonLocalCachedMap<String, TestClass>) redisson
            .getLocalCachedMap("test-local-cos", JsonJacksonCodec.INSTANCE, options);
        // set
        localCachedMap1.put("hash-key1", data);
        localCachedMap2.put("hash-key1", data);
        // get to cache
        localCachedMap1.get("hash-key1");
        localCachedMap2.get("hash-key1");
        // verify
        CacheValue cachedValue1 = localCachedMap1.getCacheObject("hash-key1");
        CacheValue cachedValue2 = localCachedMap2.getCacheObject("hash-key1");
        assertEquals(cachedValue1.getValue(), cachedValue2.getValue());
        assertFalse(cachedValue1.getValue() == cachedValue2.getValue());
    }

    public static class TestClass {
        private String field1;
        private String field2;

        public TestClass(String field1, String field2) {
            this.field1 = field1;
            this.field2 = field2;
        }

        public TestClass() {
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TestClass testClass = (TestClass) o;
            return Objects.equals(field1, testClass.field1) &&
                Objects.equals(field2, testClass.field2);
        }

        @Override
        public int hashCode() {
            return Objects.hash(field1, field2);
        }
    }
}
