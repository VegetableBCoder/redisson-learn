package com.xgd.hws;

import org.junit.jupiter.api.Test;
import org.redisson.Redisson;
import org.redisson.api.RLock;
import org.redisson.api.RReadWriteLock;
import org.redisson.api.RedissonClient;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author huwansong
 * @version 1.0, 2022/7/18 12:51
 * @since
 */
class ReadWriteLockTest {
    private static final String LOCK_NAME = "test-rw-lock";
    private static final RedissonClient redisson = Redisson.create();

    @Test
    void readWrite() throws InterruptedException {
        List<CompletableFuture<Void>> list = new ArrayList<>();
        // for (int i = 0; i < 5; i++) {
        //     list.add(read());
        // }
        for (int i = 0; i < 3; i++) {
            list.add(write());
        }
        CompletableFuture.allOf(list.toArray(new CompletableFuture[0]))
            .thenRun(() -> System.out.println("Finished"))
            .join();
    }

    private CompletableFuture<Void> read() {
        return CompletableFuture.runAsync(() -> {
            RReadWriteLock readWriteLock = redisson.getReadWriteLock(LOCK_NAME);
            RLock lock = readWriteLock.readLock();
            try {
                boolean b = lock.tryLock(30, -1, TimeUnit.SECONDS);
                if (b) {
                    System.out.println("Thread " + Thread.currentThread().getId() + " get read lock, time:" + System.currentTimeMillis() / 1000);
                    Thread.sleep(30000);
                }
            } catch (InterruptedException e) {
                System.out.println("Thread" + Thread.currentThread().getId() + " interrupted.");
            } finally {
                lock.unlock();
            }
        });
    }

    private CompletableFuture<Void> write() {
        return CompletableFuture.runAsync(() -> {
            RReadWriteLock readWriteLock = redisson.getReadWriteLock(LOCK_NAME);
            RLock lock = readWriteLock.writeLock();
            try {
                boolean b = lock.tryLock(30, -1, TimeUnit.SECONDS);
                if (b) {
                    System.out.println("Thread " + Thread.currentThread().getId() + " get write lock, time:" + System.currentTimeMillis() / 1000);
                    Thread.sleep(20000);
                }
            } catch (InterruptedException e) {
                System.out.println("Thread" + Thread.currentThread().getId() + " interrupted.");
            } finally {
                lock.unlock();
            }
        });
    }
}
