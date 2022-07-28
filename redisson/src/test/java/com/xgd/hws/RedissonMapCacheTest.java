package com.xgd.hws;

import org.junit.jupiter.api.BeforeAll;
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
import org.redisson.api.map.event.EntryExpiredListener;
import org.redisson.api.map.event.EntryRemovedListener;
import org.redisson.api.map.event.EntryUpdatedListener;
import org.redisson.client.codec.StringCodec;
import org.redisson.eviction.EvictionScheduler;
import org.redisson.eviction.MapCacheEvictionTask;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * @Classname RedissonMapCacheTest
 * @Description TODO
 * @Date 2022/7/24 18:34
 * @Created by huwansong
 */
public class RedissonMapCacheTest {

    private static RedissonClient redisson;

    @BeforeAll
    static void createClient() {
        redisson = Redisson.create();
    }

    /**
     * debug/看源码入口
     *
     * @throws InterruptedException
     */
    @Test
    void debug() throws InterruptedException {
        MapOptions<Object, Object> options = MapOptions.defaults();
        LocalCachedMapOptions.defaults();
        RedissonMapCache<String, String> map = (RedissonMapCache) redisson.getMapCache("test-key1", StringCodec.INSTANCE,
            options);
        map.put("test-hkey1", "test-value1", 20, TimeUnit.SECONDS);
        map.setMaxSize(1000);
        map.get("test-hkey1");
        RedissonMapCache<String, String> map2 = (RedissonMapCache) redisson.getMapCache("test-key2", StringCodec.INSTANCE,
            LocalCachedMapOptions.defaults());
        // 可以指定timeout 30s过期,idle time, 10s也淘汰掉
        // put入口看这个
        // 可以指定淘汰算法 默认LRU,LRU,LFU
        map2.setMaxSize(1000, EvictionMode.LFU);
        map2.put("test-hkey2", "test-value2", 30, TimeUnit.SECONDS, 10, TimeUnit.SECONDS);
        // get入口看这个(和redissonMap一样的)
        map2.get("test-hkey2");
        Thread.sleep(10 * 1000);
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
            ourStorage.put(event.getKey(), event.getValue());
        });
        mapCache.addListener((EntryRemovedListener<String, String>) event -> {
            System.out.print("ts: " + System.currentTimeMillis() / 1000 + ", ");
            System.out.println("Entry removed : key: " + event.getKey() + ", value:" + event.getValue()
                + ",old value in local cache:" + ourStorage.get(event.getKey()));
            ourStorage.remove(event.getKey());
        });
        mapCache.addListener((EntryExpiredListener<String, String>) event -> {
            System.out.print("ts: " + System.currentTimeMillis() / 1000 + ", ");
            // np?
            System.out.println("Entry expired : key: " + String.valueOf(event.getKey()) + ", value:" + String.valueOf(event.getValue())
                + ", old value in redis:" + String.valueOf(event.getOldValue()) + ",old value in local cache:" + String.valueOf(ourStorage.get((String) event.getKey())));
            ourStorage.put(event.getKey(), event.getValue());
            ourStorage.remove(event.getKey());
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
        // 等待key过期
        Thread.sleep(1200);
        //getTask是自己加的 这样就不去反射了
        ((MapCacheEvictionTask) scheduler.getTask("test-cache-listener")).run();
        // 外部存储的情况
        assertEquals("modify-1", ourStorage.get("hkey1"));
        assertEquals("value4", ourStorage.get("hkey4"));
        assertNull(ourStorage.get("hkey2"));
        assertNull(ourStorage.get("hkey3"));
        client2.shutdown();
    }

}
