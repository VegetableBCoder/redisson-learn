package com.xgd.hws;

import org.junit.jupiter.api.Test;
import org.redisson.Redisson;
import org.redisson.RedissonMapCache;
import org.redisson.api.EvictionMode;
import org.redisson.api.LocalCachedMapOptions;
import org.redisson.api.MapOptions;
import org.redisson.api.RMapCache;
import org.redisson.api.RedissonClient;
import org.redisson.api.map.MapLoader;
import org.redisson.api.map.MapWriter;
import org.redisson.api.map.event.EntryCreatedListener;
import org.redisson.api.map.event.EntryEvent;
import org.redisson.api.map.event.EntryExpiredListener;
import org.redisson.api.map.event.EntryRemovedListener;
import org.redisson.api.map.event.EntryUpdatedListener;
import org.redisson.api.map.event.MapEntryListener;
import org.redisson.client.codec.StringCodec;
import org.redisson.eviction.EvictionScheduler;
import org.redisson.eviction.MapCacheEvictionTask;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * @author huwansong
 * @version 1.0, 2022/7/22 19:52
 * @since
 */
class RedissonMapTest {

    private static final Redisson redisson = (Redisson) Redisson.create();

    /**
     * debug/看源码入口
     *
     * @throws InterruptedException
     */
    @Test
    void mapSample() throws InterruptedException {
        MapOptions<Object, Object> options = MapOptions.defaults();
        LocalCachedMapOptions.defaults();
        RedissonMapCache<String, String> map = (RedissonMapCache) redisson.getMapCache("test-key1", StringCodec.INSTANCE,
            options);
        map.put("test-hkey1", "test-value1", 20, TimeUnit.SECONDS);
        map.setMaxSize(1000);
        RedissonMapCache<String, String> map2 = (RedissonMapCache) redisson.getMapCache("test-key2", StringCodec.INSTANCE,
            LocalCachedMapOptions.defaults());
        // 可以指定timeout 30s过期,idle time, 10s也淘汰掉
        // 入口看这个
        map2.put("test-hkey2", "test-value2", 30, TimeUnit.SECONDS, 10, TimeUnit.SECONDS);
        // 可以指定淘汰算法 默认LRU,LRU,LFU
        map2.setMaxSize(1000, EvictionMode.LFU);
        map2.get("test-hkey2");
        Thread.sleep(10 * 1000);
        map2.loadAll(true, 10);
    }

    /**
     * 同步的情况下的writer loader
     * 同步模式是先写外部存储 再写redis
     * 这里用作本地缓存是有问题的(最重要的是Loader执行的顺序是先查redis再查loader)
     * 如果用来持久化的话 还需要再处理listener
     */
    @Test
    void writerLoaderSampleSync() {
        Map<String, Object> outStorage = new HashMap<>();
        MapOptions<String, Object> options = MapOptions.<String, Object>defaults()
            .writeMode(MapOptions.WriteMode.WRITE_THROUGH)
            .loader(new MapLoader<String, Object>() {
                @Override
                public Object load(String key) {
                    return outStorage.get(key);
                }

                @Override
                public Iterable<String> loadAllKeys() {
                    return outStorage.keySet();
                }
            })
            .writer(new MapWriter<String, Object>() {

                @Override
                public void write(Map<String, Object> map) {
                    // 这里可以用外部存储装一下 但是还得通过pubsub接受其他节点发布的
                    outStorage.putAll(map);
                }

                @Override
                public void delete(Collection<String> keys) {
                    outStorage.keySet().removeAll(keys);
                }
            });
        RMapCache<String, Object> mapCache = redisson.getMapCache("test-cache", StringCodec.INSTANCE, options);
        mapCache.put("test", "value");
        mapCache.put("test2", "value2");
        mapCache.put("test3", "value3");
        assertEquals("value", outStorage.get("test"));
        // 注意 test2从外部存储移除了
        outStorage.remove("test2");
        // 这里外部存储已经没了 loader取到的是null 走redis取
        assertEquals("value2", mapCache.get("test2"));
        mapCache.remove("test");
        assertNull(outStorage.get("test"));
        // 这里只是改了外部存储
        outStorage.put("test3", "modify-3");
        outStorage.put("test4", "value4");
        outStorage.put("notExistsInRedis", "value");
        // 传false redis的不会被修改 如果是数据库做外部数据源loader 使用loadAll必须十分谨慎 且传keys
        mapCache.loadAll(false, 1);
        // 及时外部存储修改了 没有replace缓存 redis数据依旧不会变更
        assertEquals("value3", mapCache.get("test3"));
        assertEquals("value4", mapCache.get("test4"));
        //虽然redis没有 但是外部存储有 可以走loader拿到
        assertEquals("value", mapCache.get("notExistsInRedis"));
        // 传true redis的也会被修改
        outStorage.put("test5", "value5");
        mapCache.loadAll(true, 1);
        assertEquals("modify-3", mapCache.get("test3"));
        assertEquals("value5", mapCache.get("test5"));
        // 不管是true传false 外部存储新增的load的时候都会写到redis
    }


    void writerLoaderSampleASync() {
        //其实只是顺序变了 write的时候先写redis 再异步调用writer
    }

    /**
     * 监听器使用
     */
    @Test
    void listenerSample() throws InterruptedException, NoSuchFieldException, IllegalAccessException {
        Map<String, String> ourStorage = new HashMap<>();
        MapOptions<String, String> options = MapOptions.<String, String>defaults()
            .writeMode(MapOptions.WriteMode.WRITE_THROUGH)
            .loader(new MapLoader<String, String>() {
                @Override
                public String load(String key) {
                    return ourStorage.get(key);
                }

                @Override
                public Iterable<String> loadAllKeys() {
                    return ourStorage.keySet();
                }
            })
            .writer(new MapWriter<String, String>() {

                @Override
                public void write(Map<String, String> map) {
                    // 这里可以用外部存储装一下 但是还得通过listener接受其他节点发布的
                    ourStorage.putAll(map);
                }

                @Override
                public void delete(Collection<String> keys) {
                    ourStorage.keySet().removeAll(keys);
                }
            });
        RMapCache<String, String> mapCache = redisson.getMapCache("test-cache-listener", StringCodec.INSTANCE, options);
        mapCache.put("hkey1", "value1");
        mapCache.put("hkey2", "value2");
        mapCache.put("hkey3", "value3", 1, TimeUnit.SECONDS);
        // 新增 修改 删除 过期 这里简单化处理一下
        mapCache.addListener((EntryCreatedListener<String, String>) event -> {
            System.out.print("ts: " + System.currentTimeMillis() / 1000 + ", ");
            System.out.println("Entry created : key: " + event.getKey() + " value:" + event.getValue());
            ourStorage.put((String) event.getKey(), event.getValue());
        });
        mapCache.addListener((EntryUpdatedListener<String, String>) event -> {
            System.out.print("ts: " + System.currentTimeMillis() / 1000 + ", ");
            System.out.println("Entry updated : key: " + event.getKey() + ", value:" + event.getValue()
                + ", old value in redis:" + event.getOldValue() + ",old value in local cache:" + ourStorage.get((String) event.getKey()));
            ourStorage.put((String) event.getKey(), (String) event.getValue());
        });
        mapCache.addListener((EntryRemovedListener<String, String>) event -> {
            System.out.print("ts: " + System.currentTimeMillis() / 1000 + ", ");
            System.out.println("Entry removed : key: " + event.getKey() + ", value:" + event.getValue()
                + ",old value in local cache:" + ourStorage.get((String) event.getKey()));
            ourStorage.remove((String) event.getKey());
        });
        mapCache.addListener((EntryExpiredListener<String, String>) event -> {
            System.out.print("ts: " + System.currentTimeMillis() / 1000 + ", ");
            System.out.println("Entry expired : key: " + String.valueOf(event.getKey()) + ", value:" + String.valueOf(event.getValue())
                + ", old value in redis:" + String.valueOf(event.getOldValue()) + ",old value in local cache:" + String.valueOf(ourStorage.get((String) event.getKey())));
            ourStorage.put((String) event.getKey(), (String) event.getValue());
            ourStorage.remove((String) event.getKey());
        });
        //另外一个线程 用另一个客户端操作
        RedissonClient client2 = Redisson.create();
        RMapCache<Object, Object> cache2 = client2.getMapCache("test-cache-listener", StringCodec.INSTANCE);

        cache2.put("hkey4", "value4");
        cache2.put("hkey1", "modify-1");
        cache2.remove("hkey2");
        //少sleep一会 自己启动下evict清理
        Field field = RedissonMapCache.class.getDeclaredField("evictionScheduler");
        field.setAccessible(true);
        EvictionScheduler scheduler = (EvictionScheduler) field.get(cache2);
        //getTask是自己加的 懒得去反射了
        ((MapCacheEvictionTask) scheduler.getTask("test-cache-listener")).schedule();
        Thread.sleep(1000);
        // 外部存储的情况
        assertEquals("modify-1", ourStorage.get("hkey1"));
        assertEquals("value4", ourStorage.get("hkey4"));
        assertNull(ourStorage.get("hkey2"));
        assertNull(ourStorage.get("hkey3"));
    }
}
