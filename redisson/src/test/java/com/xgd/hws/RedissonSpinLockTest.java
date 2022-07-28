package com.xgd.hws;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.redisson.Redisson;
import org.redisson.RedissonSpinLock;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author huwansong
 * @version 1.0, 2022/7/19 20:52
 * @since
 */
class RedissonSpinLockTest {
    private static final Redisson redisson = (Redisson) Redisson.create();
    private static final String NAME = "test-spin-lock";

    @AfterAll
    static void close() {
        redisson.shutdown();
    }

    @Test
    void testSpinLock() {
        List<CompletableFuture<Void>> list = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            list.add(tryLock());
        }
        CompletableFuture.allOf(list.toArray(new CompletableFuture[0]))
            .whenComplete((r, e) -> System.out.println("Finished!"))
            .join();
    }

    private CompletableFuture<Void> tryLock() {
        return CompletableFuture.runAsync(() -> {
            RedissonSpinLock lock = (RedissonSpinLock) redisson.getSpinLock(NAME);
            try {
                //这个方法是 RedissonLock,spinLock各自实现
                lock.tryLock(10, 30, TimeUnit.SECONDS);
                Thread.sleep(20 * 1000);
                reentrant();
                Thread.sleep(12 * 1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } finally {
                lock.unlock();
            }
        });
    }

    private void reentrant() {
        boolean bool = false;
        RedissonSpinLock lock = (RedissonSpinLock) redisson.getSpinLock(NAME);
        try {
            lock.tryLock(10, 30, TimeUnit.SECONDS);
            Thread.sleep(20 * 1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            if (bool) {
                lock.unlock();
            }
        }
    }
}
