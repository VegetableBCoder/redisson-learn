/**
 * Copyright (c) 2013-2021 Nikita Koksharov
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.redisson.pubsub;

import org.redisson.RedissonLockEntry;

import java.util.concurrent.CompletableFuture;

/**
 * 
 * @author Nikita Koksharov
 *
 */
public class LockPubSub extends PublishSubscribe<RedissonLockEntry> {

    public static final Long UNLOCK_MESSAGE = 0L;
    public static final Long READ_UNLOCK_MESSAGE = 1L;

    public LockPubSub(PublishSubscribeService service) {
        super(service);
    }
    
    @Override
    protected RedissonLockEntry createEntry(CompletableFuture<RedissonLockEntry> newPromise) {
        return new RedissonLockEntry(newPromise);
    }

    @Override
    protected void onMessage(RedissonLockEntry value, Long message) {
        // 如果是解锁(读锁除外)的
        if (message.equals(UNLOCK_MESSAGE)) {
            // 监听消息的开始执行
            Runnable runnableToExecute = value.getListeners().poll();
            if (runnableToExecute != null) {
                // 取出来执行 比如让线程去尝试获取锁 (这里每个订阅者都可以唤醒一个线程去竞争)
                runnableToExecute.run();
            }
            //release掉
            value.getLatch().release();
        }
        // 读锁解锁的消息处理
        else if (message.equals(READ_UNLOCK_MESSAGE)) {
            while (true) {
                Runnable runnableToExecute = value.getListeners().poll();
                if (runnableToExecute == null) {
                    break;
                }
                runnableToExecute.run();
            }
            // 释放资源个数 和等待队列的长度相同
            value.getLatch().release(value.getLatch().getQueueLength());
        }
    }

}
