/**
 * Copyright (c) 2013-2021 Nikita Koksharov
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.redisson.pubsub;

import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 信号量
 *
 * @author Nikita Koksharov
 */
public class AsyncSemaphore {

    private final AtomicInteger counter;
    private final Queue<CompletableFuture<Void>> listeners = new ConcurrentLinkedQueue<>();

    public AsyncSemaphore(int permits) {
        // 设置初始值
        counter = new AtomicInteger(permits);
    }

    public boolean tryAcquire(long timeoutMillis) {
        CompletableFuture<Void> f = acquire();
        try {
            f.get(timeoutMillis, TimeUnit.MILLISECONDS);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } catch (ExecutionException e) {
            throw new IllegalStateException(e);
        } catch (TimeoutException e) {
            return false;
        }
    }

    public int queueSize() {
        return listeners.size();
    }

    public void removeListeners() {
        listeners.clear();
    }

    public CompletableFuture<Void> acquire() {
        CompletableFuture<Void> future = new CompletableFuture<>();
        listeners.add(future);
        //传进去的future刚new出来的 complete一定会true(只有这里会 add future 只有里面会poll future)
        tryRun();
        return future;
    }

    public void acquire(Runnable listener) {
        // 获取资源到资源然后执行
        acquire().thenAccept(r -> listener.run());
    }

    private void tryRun() {
        // 自旋...
        while (true) {
            //--后还是>=0
            if (counter.decrementAndGet() >= 0) {
                // 从队列取出
                CompletableFuture<Void> future = listeners.poll();
                // 是null重新加回去
                if (future == null) {
                    counter.incrementAndGet();
                    return;
                }

                if (future.complete(null)) {
                    return;
                }
            }
            // <=0 就结束循环
            if (counter.incrementAndGet() <= 0) {
                return;
            }
        }
    }

    public int getCounter() {
        return counter.get();
    }

    public void release() {
        counter.incrementAndGet();
        tryRun();
    }

    @Override
    public String toString() {
        return "value:" + counter + ":queue:" + queueSize();
    }

    public static void main(String[] args) throws InterruptedException {
        // 两个资源
        AsyncSemaphore semaphore = new AsyncSemaphore(1);
        for (int i = 0; i < 3; i++) {
            final int finalI = i;
            CompletableFuture.runAsync(() -> semaphore.acquire(() -> {
                System.out.println("task " + finalI + " get resource at " + System.currentTimeMillis() / 1000);
                try {
                    Thread.sleep(2 * 1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println("task " + finalI + " release resource at " + System.currentTimeMillis() / 1000);
                semaphore.release();
            }));
        }
        Thread.sleep(4000);
    }
}
