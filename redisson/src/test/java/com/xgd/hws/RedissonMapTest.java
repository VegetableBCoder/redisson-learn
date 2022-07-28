package com.xgd.hws;

import org.junit.jupiter.api.Test;
import org.redisson.Redisson;
import org.redisson.api.MapOptions;
import org.redisson.api.RMap;
import org.redisson.api.map.MapLoader;
import org.redisson.api.map.MapWriter;
import org.redisson.client.codec.StringCodec;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * @author huwansong
 * @version 1.0, 2022/7/22 19:52
 * @since
 */
class RedissonMapTest {

    private static final Redisson redisson = (Redisson) Redisson.create();

    @Test
    void debug() {
        RMap<String, String> map = redisson.getMap("debug-map", StringCodec.INSTANCE);
        map.put("key1", "value1");
        String value1 = map.get("key1");
        assertEquals("value1", value1);
    }

    /**
     * 同步的情况下的writer loader
     * 同步模式是先写外部存储 再写redis
     * 这里用作本地缓存是有问题的(最重要的是Loader执行的顺序是先查redis再查loader)
     * 如果用来持久化的话 还需要再处理listener
     */
    @Test
    void writerLoaderAndListenerSample() {
        Map<String, Object> outStorage = new HashMap<>();
        MapOptions<String, Object> options = MapOptions.<String, Object>defaults()
            .writeMode(MapOptions.WriteMode.WRITE_THROUGH)
            // .writeBehindDelay(500)
            // .writeBehindBatchSize(100)
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
                    // 这里可以用外部存储装一下
                    outStorage.putAll(map);
                }

                @Override
                public void delete(Collection<String> keys) {
                    outStorage.keySet().removeAll(keys);
                }
            });
        RMap<String, Object> map = redisson.getMap("test-map", StringCodec.INSTANCE, options);
        map.put("test", "value");
        map.put("test2", "value2");
        map.put("test3", "value3");
        assertEquals("value", outStorage.get("test"));
        // 注意 test2从外部存储移除了
        outStorage.remove("test2");
        // 这里外部存储已经没了 loader取到的是null 走redis取
        assertEquals("value2", map.get("test2"));
        map.remove("test");
        assertNull(outStorage.get("test"));
        // 这里只是改了外部存储
        outStorage.put("test3", "modify-3");
        outStorage.put("test4", "value4");
        // 传false redis的不会被修改 如果是数据库做外部数据源loader 使用loadAll必须十分谨慎 且传keys
        map.loadAll(false, 1);
        outStorage.put("notExistsInRedis", "value");
        // 及时外部存储修改了 没有replace缓存 redis数据依旧不会变更
        assertEquals("value3", map.get("test3"));
        // 这里说明loadAll的过程中将test4加入到了缓存
        outStorage.remove("test4");
        assertEquals("value4", map.get("test4"));
        // 虽然redis没有 但是外部存储有 可以走loader拿到
        assertEquals("value", map.get("notExistsInRedis"));
        // 传true redis的也会被修改
        outStorage.put("test5", "value5");
        map.loadAll(true, 1);
        assertEquals("modify-3", map.get("test3"));
        assertEquals("value5", map.get("test5"));
        // 不管是true传false 外部存储新增的load的时候都会写到redis
    }


}
