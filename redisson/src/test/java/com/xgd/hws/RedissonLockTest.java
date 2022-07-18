package com.xgd.hws;

import org.junit.jupiter.api.Test;
import org.redisson.Redisson;
import org.redisson.RedissonLock;

import java.util.concurrent.TimeUnit;

/**
 * redisson distribute lock
 *
 * @author huwansong
 * @version 1.0, 2022/7/16 10:14
 * @since 1.0
 */
public class RedissonLockTest {
    /**
     * 默认会连localhost:6379
     */
    private static final Redisson redisson = (Redisson) Redisson.create();

    @Test
    void unFairLock() throws InterruptedException {
        // new 一个锁对象
        RedissonLock lock = (RedissonLock) redisson.getLock("test-lock");
        lock.tryLock(100, -1, TimeUnit.SECONDS);
        // if (lock.tryLock()) {
        // lock到try中间不要加内容防止抛异常导致没有unlock 考虑移到try中还是移到tryLock之前
        try {
            // do something
            Thread.sleep(60 * 1000);
        }
        finally {
            // 释放锁放到finally
            lock.unlock();
        }
        // }
    }
}