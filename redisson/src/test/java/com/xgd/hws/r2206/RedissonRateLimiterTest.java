package com.xgd.hws.r2206;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.redisson.Redisson;
import org.redisson.api.RRateLimiter;
import org.redisson.api.RateIntervalUnit;
import org.redisson.api.RateType;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * @author huwansong
 * @version 1.0, 2022/7/19 21:21
 * @since
 */
class RedissonRateLimiterTest {
    private static final Redisson redisson = (Redisson) Redisson.create();
    private static final RRateLimiter rateLimiter = redisson.getRateLimiter("test-rate-limiter");


    @AfterAll
    static void close() {
        redisson.shutdown();
    }

    @Test
    void simpleRateLimiter() throws ExecutionException, InterruptedException {
        rateLimiter.setRate(RateType.OVERALL, 10, 1, RateIntervalUnit.SECONDS);
        CompletableFuture<Void>[] array = new CompletableFuture[100];
        for (int i = 0; i < 100; i++) {
            array[i] = executeWithResource();
        }
        CompletableFuture.allOf(array).whenComplete((r, e) -> System.out.println("Finished")).join();
    }

    private CompletableFuture<Void> executeWithResource() {
        return CompletableFuture.runAsync(() -> {
            // 权重
            if (rateLimiter.tryAcquire(1)) {
                System.out.println("Time " + System.currentTimeMillis() / 1000 + " Thread " + Thread.currentThread().getId() + " get rate-limit resource success.");
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                System.out.println("Time " + System.currentTimeMillis() / 1000 + " Thread " + Thread.currentThread().getId() + " get rate-limit resource failed.");
            }
        });
    }
}
