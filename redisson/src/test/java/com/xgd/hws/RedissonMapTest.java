package com.xgd.hws;

import org.junit.jupiter.api.Test;
import org.redisson.Redisson;
import org.redisson.RedissonMapCache;
import org.redisson.api.MapOptions;

import java.util.concurrent.TimeUnit;

/**
 * @author huwansong
 * @version 1.0, 2022/7/22 19:52
 * @since
 */
class RedissonMapTest {

    private static final Redisson redisson = (Redisson) Redisson.create();

    @Test
    void mapSample() {
        RedissonMapCache<String, Object> map = (RedissonMapCache) redisson.getMapCache("name", MapOptions.defaults());
        map.setMaxSizeAsync(1000);
        map.expire(30, TimeUnit.MINUTES);
        map.containsKey("aa");
    }
}
