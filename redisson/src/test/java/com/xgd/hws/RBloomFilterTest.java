package com.xgd.hws;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.redisson.Redisson;
import org.redisson.api.RBloomFilter;
import org.redisson.api.RedissonClient;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author huwansong
 * @version 1.0, 2022/7/11 15:46
 * @since
 */
class RBloomFilterTest {
    private static final RedissonClient redisson = Redisson.create();

    @AfterEach
    void clear() {
        redisson.getKeys().flushdb();
    }

    @AfterAll
    static void close() {
        redisson.shutdown();
    }

    @Test
    void bloomFilterOperation() {
        //开源版不支持集群布隆过滤器 需要自己去实现
        //思路1:按照hash函数分到不同节点上去 单个key会比较大但是要考虑hash函数个数和节点个数的负载关系
        //思路2:按照hash分段放到不同节点 这种应该比较适合
        RBloomFilter<String> bloomFilter = redisson.getBloomFilter("test-bloom");
        //设置最大个数和允许的误判率(无法精确判断是否一定存在 但是可以判断一定不存在)
        bloomFilter.tryInit(1000, 0.005f);
        bloomFilter.add("aaa");
        bloomFilter.add("bbb");

        assertEquals(2, bloomFilter.count());
        assertFalse(bloomFilter.contains("aa"));
        assertTrue(bloomFilter.contains("aaa"));

    }
}
