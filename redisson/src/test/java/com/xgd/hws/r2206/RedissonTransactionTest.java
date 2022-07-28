package com.xgd.hws.r2206;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.redisson.Redisson;
import org.redisson.api.RMap;
import org.redisson.api.RTransaction;
import org.redisson.api.RedissonClient;
import org.redisson.api.TransactionOptions;
import org.redisson.transaction.RedissonTransactionalMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author huwansong
 * @version 1.0, 2022/7/12 9:24
 * @since
 */
class RedissonTransactionTest {
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
    void transactionMap() {
        RTransaction transaction = redisson.createTransaction(TransactionOptions.defaults());
        RMap<Object, Object> map1 = redisson.getMap("test-trans-map");
        //return new RedissonTransactionalMap<K, V>(commandExecutor, name, operations, options.getTimeout(), executed, id);
        RedissonTransactionalMap<Object, Object> map = (RedissonTransactionalMap<Object, Object>) transaction.getMap("test-trans-map");
        map.put("aa", "bb");
        //此时redis内还没有值
        assertNull(map1.get("aa"));
        map.put("bb", "cc");
        map.put("cc", "dd");
        //也可以commitAsync() 这里debug就用同步提交了
        transaction.commit();
        assertEquals("dd", map1.get("cc"));
        //这里操作应该报错
        IllegalStateException illegalStateException = assertThrows(IllegalStateException.class, () -> map.put("bb", "bb"));
        assertEquals("Unable to execute operation. Transaction is in finished state!", illegalStateException.getMessage());
    }
}
