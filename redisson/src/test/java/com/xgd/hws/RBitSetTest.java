package com.xgd.hws;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.redisson.Redisson;
import org.redisson.api.RBitSet;
import org.redisson.api.RedissonClient;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author huwansong
 * @version 1.0, 2022/7/11 14:38
 * @since
 */
class RBitSetTest {

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
    void bitSetOperation() {
        int bitSize = 1 << 21;
        RBitSet rBitSet = redisson.getBitSet("test-bitset");
        assertFalse(rBitSet.set(bitSize, false));
        long size = rBitSet.sizeInMemory();
        assertFalse(rBitSet.get(bitSize));
        assertFalse(rBitSet.set(bitSize));
        assertTrue(rBitSet.get(bitSize));
        assertTrue(rBitSet.set(bitSize, false));
        //按区间批量写
        rBitSet.set(1 << 20, 1 << 21, true);
        // 256KB
        assertEquals(256L, size / 1024);
    }

    @Test
    void countBitSet() {
        RBitSet rBitSet = redisson.getBitSet("test-bitset");
        //设置64个1
        rBitSet.setLong(0, -1);
        for (int i = 0; i < 64; i++) {
            assertTrue(rBitSet.get(i));
        }
        assertFalse(rBitSet.get(64));
    }
}
