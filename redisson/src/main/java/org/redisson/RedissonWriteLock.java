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
package org.redisson;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

import org.redisson.api.RFuture;
import org.redisson.api.RLock;
import org.redisson.client.codec.LongCodec;
import org.redisson.client.codec.StringCodec;
import org.redisson.client.protocol.RedisCommands;
import org.redisson.client.protocol.RedisStrictCommand;
import org.redisson.command.CommandAsyncExecutor;
import org.redisson.pubsub.LockPubSub;

/**
 * Lock will be removed automatically if client disconnects.
 *
 * @author Nikita Koksharov
 *
 */
public class RedissonWriteLock extends RedissonLock implements RLock {

    protected RedissonWriteLock(CommandAsyncExecutor commandExecutor, String name) {
        super(commandExecutor, name);
    }

    @Override
    String getChannelName() {
        return prefixName("redisson_rwlock", getRawName());
    }

    @Override
    protected String getLockName(long threadId) {
        return super.getLockName(threadId) + ":write";
    }

    @Override
    <T> RFuture<T> tryLockInnerAsync(long waitTime, long leaseTime, TimeUnit unit, long threadId, RedisStrictCommand<T> command) {
        return evalWriteAsync(getRawName(), LongCodec.INSTANCE, command,
                            //获取mode
                            "local mode = redis.call('hget', KEYS[1], 'mode'); " +
                            // 无锁
                            "if (mode == false) then " +
                                    //标记 当前是写锁
                                  "redis.call('hset', KEYS[1], 'mode', 'write'); " +
                                    //set 重入次数
                                  "redis.call('hset', KEYS[1], ARGV[2], 1); " +
                                    //设置过期时间
                                  "redis.call('pexpire', KEYS[1], ARGV[1]); " +
                                  "return nil; " +
                              "end; " +
                            // 如果是写锁
                              "if (mode == 'write') then " +
                            // 判断自己是不是持有写锁 (排它性)
                                  "if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then " +
                                      // 重入次数
                                      "redis.call('hincrby', KEYS[1], ARGV[2], 1); " +
                                      // 过期时间
                                      "local currentExpire = redis.call('pttl', KEYS[1]); " +
                                      // 续期
                                      "redis.call('pexpire', KEYS[1], currentExpire + ARGV[1]); " +
                                      "return nil; " +
                                  "end; " +
                                "end;" +
                                //取锁失败了 返回ttl
                                "return redis.call('pttl', KEYS[1]);",
                        //问题 如果仅有自己持有读锁是不是可以加写锁?
                        Arrays.<Object>asList(getRawName()),
                        unit.toMillis(leaseTime), getLockName(threadId));
    }

    @Override
    protected RFuture<Boolean> unlockInnerAsync(long threadId) {
        return evalWriteAsync(getRawName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                // 当前锁的状态
                "local mode = redis.call('hget', KEYS[1], 'mode'); " +
                // 无锁 直接publish
                "if (mode == false) then " +
                    "redis.call('publish', KEYS[2], ARGV[1]); " +
                    "return 1; " +
                "end;" +
                // 当前是写锁
                "if (mode == 'write') then " +
                    // 判断是不是自己持有的
                    "local lockExists = redis.call('hexists', KEYS[1], ARGV[3]); " +
                    // 其他线程锁住 自己不能去解锁
                    "if (lockExists == 0) then " +
                        "return nil;" +
                    "else " +
                        // 重入次数-1
                        "local counter = redis.call('hincrby', KEYS[1], ARGV[3], -1); " +
                        //还有重入次数
                        "if (counter > 0) then " +
                            //更新过期时间
                            "redis.call('pexpire', KEYS[1], ARGV[2]); " +
                            "return 0; " +
                        //没重入计数了
                        "else " +
                            // 删掉自己的持有记录
                            "redis.call('hdel', KEYS[1], ARGV[3]); " +
                            // 如果自己没有持有读锁
                            "if (redis.call('hlen', KEYS[1]) == 1) then " +
                                //整把锁一起删了
                                "redis.call('del', KEYS[1]); " +
                                //发布消息,其他线程来竞争
                                "redis.call('publish', KEYS[2], ARGV[1]); " +
                            "else " +
                                // 还有读锁没解开 改成读锁模式 其他线程尝试加读锁就能成功了
                                "redis.call('hset', KEYS[1], 'mode', 'read'); " +
                            "end; " +
                            "return 1; "+
                        "end; " +
                    "end; " +
                "end; "
                + "return nil;",
        Arrays.<Object>asList(getRawName(), getChannelName()),
        LockPubSub.READ_UNLOCK_MESSAGE, internalLockLeaseTime, getLockName(threadId));
    }

    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected CompletionStage<Boolean> renewExpirationAsync(long threadId) {
        CompletionStage<Boolean> f = super.renewExpirationAsync(threadId);
        return f.thenCompose(r -> {
            if (!r) {
                RedissonReadLock lock = new RedissonReadLock(commandExecutor, getRawName());
                return lock.renewExpirationAsync(threadId);
            }
            return CompletableFuture.completedFuture(r);
        });
    }

    @Override
    public RFuture<Boolean> forceUnlockAsync() {
        cancelExpirationRenewal(null);
        return evalWriteAsync(getRawName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
              "if (redis.call('hget', KEYS[1], 'mode') == 'write') then " +
                  "redis.call('del', KEYS[1]); " +
                  "redis.call('publish', KEYS[2], ARGV[1]); " +
                  "return 1; " +
              "end; " +
              "return 0; ",
              Arrays.<Object>asList(getRawName(), getChannelName()), LockPubSub.READ_UNLOCK_MESSAGE);
    }

    @Override
    public boolean isLocked() {
        RFuture<String> future = commandExecutor.writeAsync(getRawName(), StringCodec.INSTANCE, RedisCommands.HGET, getRawName(), "mode");
        String res = get(future);
        return "write".equals(res);
    }

}
