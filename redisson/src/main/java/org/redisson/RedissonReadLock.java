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
package org.redisson;

import java.util.Arrays;
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
public class RedissonReadLock extends RedissonLock implements RLock {

    protected RedissonReadLock(CommandAsyncExecutor commandExecutor, String name) {
        super(commandExecutor, name);
    }

    @Override
    String getChannelName() {
        return prefixName("redisson_rwlock", getRawName());
    }

    String getWriteLockName(long threadId) {
        return super.getLockName(threadId) + ":write";
    }

    String getReadWriteTimeoutNamePrefix(long threadId) {
        return suffixName(getRawName(), getLockName(threadId)) + ":rwlock_timeout";
    }

    @Override
    <T> RFuture<T> tryLockInnerAsync(long waitTime, long leaseTime, TimeUnit unit, long threadId, RedisStrictCommand<T> command) {

        return evalWriteAsync(getRawName(), LongCodec.INSTANCE, command,
            //获取锁的模式
            "local mode = redis.call('hget', KEYS[1], 'mode'); " +
                //无锁
                "if (mode == false) then " +
                    // 加读锁 mode=read
                    "redis.call('hset', KEYS[1], 'mode', 'read'); " +
                    // 锁key - 锁名称-线程id - 1 持有的次数
                    "redis.call('hset', KEYS[1], ARGV[2], 1); " +
                    // 这个是字符串拼接函数 单独记录的key 记录了这个线程的重入次数 (放在外面应该是为了方便记录过期时间)
                    "redis.call('set', KEYS[2] .. ':1', 1); " +
                    // 过期时间
                    "redis.call('pexpire', KEYS[2] .. ':1', ARGV[1]); " +
                    // 过期时间
                    "redis.call('pexpire', KEYS[1], ARGV[1]); " +
                "return nil; " +
            "end; " +
            // 如果是读锁 那么是可以共享的 或者是被加了写锁 但是写锁是自己的
            "if (mode == 'read') or (mode == 'write' and redis.call('hexists', KEYS[1], ARGV[3]) == 1) then " +
                //增加读锁的重入计数
                "local ind = redis.call('hincrby', KEYS[1], ARGV[2], 1); " +
                // 拼接
                "local key = KEYS[2] .. ':' .. ind;" +
                // 设置这次重入的过期时间
                "redis.call('set', key, 1); " +
                "redis.call('pexpire', key, ARGV[1]); " +
                // 锁的剩余时间ms
                "local remainTime = redis.call('pttl', KEYS[1]); " +
                // 设置个较大值
                "redis.call('pexpire', KEYS[1], math.max(remainTime, ARGV[1])); " +
                "return nil; " +
            "end;" +
                //如果是其他线程的写锁 被排斥就返回写锁过期时间
                "return redis.call('pttl', KEYS[1]);",
            Arrays.<Object>asList(getRawName(), getReadWriteTimeoutNamePrefix(threadId)),
            unit.toMillis(leaseTime), getLockName(threadId), getWriteLockName(threadId));
    }

    @Override
    protected RFuture<Boolean> unlockInnerAsync(long threadId) {
        String timeoutPrefix = getReadWriteTimeoutNamePrefix(threadId);
        String keyPrefix = getKeyPrefix(threadId, timeoutPrefix);

        return evalWriteAsync(getRawName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
            "local mode = redis.call('hget', KEYS[1], 'mode'); " +
                "if (mode == false) then " +
                    //没有锁 直接发消息就完事了
                    "redis.call('publish', KEYS[2], ARGV[1]); " +
                    "return 1; " +
                "end; " +
                // 锁没有被当前线程持有的记录
                "local lockExists = redis.call('hexists', KEYS[1], ARGV[2]); " +
                "if (lockExists == 0) then " +
                    "return nil;" +
                "end; " +
                //减少持有次数(重入计数)
                "local counter = redis.call('hincrby', KEYS[1], ARGV[2], -1); " +
                "if (counter == 0) then " +
                // 自己没有持有了 删掉
                    "redis.call('hdel', KEYS[1], ARGV[2]); " +
                "end;" +
                // 删除记录过期时间的key
                "redis.call('del', KEYS[3] .. ':' .. (counter+1)); " +
                // 除了mode还有别的key
                "if (redis.call('hlen', KEYS[1]) > 1) then " +
                    // -1,-2都是有意义的 先定义为ttl -3
                    "local maxRemainTime = -3; " +
                    //hkeys
                    "local keys = redis.call('hkeys', KEYS[1]); " +
                    // 取最大的remainTime
                    "for n, key in ipairs(keys) do " +
                        "counter = tonumber(redis.call('hget', KEYS[1], key)); " +
                        //除了mode之外的
                        "if type(counter) == 'number' then " +
                            //遍历重入 取最大的过期时间
                            "for i=counter, 1, -1 do " +
                                "local remainTime = redis.call('pttl', KEYS[4] .. ':' .. key .. ':rwlock_timeout:' .. i); " +
                                "maxRemainTime = math.max(remainTime, maxRemainTime);" +
                            "end; " +
                        "end; " +
                    "end; " +
                    //设置过期时间 为最大的过期时间
                    "if maxRemainTime > 0 then " +
                        "redis.call('pexpire', KEYS[1], maxRemainTime); " +
                        "return 0; " +
                    "end;" +
                    // 遍历完也没找到过期记录 说明遍历过程中都过期了
                    // 被持有的是写锁 说明自己还有写锁 不管 过期时间
                    "if mode == 'write' then " +
                        "return 0;" +
                    "end; " +
                "end; " +
                // 全部持有锁的线程都过期了 delete 发布解锁消息
                "redis.call('del', KEYS[1]); " +
                "redis.call('publish', KEYS[2], ARGV[1]); " +
                "return 1; ",
            Arrays.<Object>asList(getRawName(), getChannelName(), timeoutPrefix, keyPrefix),
            LockPubSub.UNLOCK_MESSAGE, getLockName(threadId));
    }

    protected String getKeyPrefix(long threadId, String timeoutPrefix) {
        return timeoutPrefix.split(":" + getLockName(threadId))[0];
    }

    @Override
    protected CompletionStage<Boolean> renewExpirationAsync(long threadId) {
        String timeoutPrefix = getReadWriteTimeoutNamePrefix(threadId);
        String keyPrefix = getKeyPrefix(threadId, timeoutPrefix);

        return evalWriteAsync(getRawName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
            //当前线程的重入计数
            "local counter = redis.call('hget', KEYS[1], ARGV[2]); " +
            "if (counter ~= false) then " +
                //整个hash的过期时间
                "redis.call('pexpire', KEYS[1], ARGV[1]); " +
                // hash的元素个数>1 除去mode外还有其他持有锁的信息
                "if (redis.call('hlen', KEYS[1]) > 1) then " +
                    //hkeys
                    "local keys = redis.call('hkeys', KEYS[1]); " +
                    //遍历持有者
                    "for n, key in ipairs(keys) do " +
                        // 获取持有锁的线程的重入次数
                        "counter = tonumber(redis.call('hget', KEYS[1], key)); " +
                        "if type(counter) == 'number' then " +
                            // 遍历全部重入的key
                            "for i=counter, 1, -1 do " +
                                //刷新过期时间
                                "redis.call('pexpire', KEYS[2] .. ':' .. key .. ':rwlock_timeout:' .. i, ARGV[1]); " +
                            "end; " +
                        "end; " +
                    "end; " +
                "end; " +
                "return 1; " +
            "end; " +
            "return 0;",
            Arrays.<Object>asList(getRawName(), keyPrefix),
            internalLockLeaseTime, getLockName(threadId));
    }

    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }

    @Override
    public RFuture<Boolean> forceUnlockAsync() {
        cancelExpirationRenewal(null);
        //强制解锁
        return evalWriteAsync(getRawName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
            // del了 发布解锁消息
            "if (redis.call('hget', KEYS[1], 'mode') == 'read') then " +
                "redis.call('del', KEYS[1]); " +
                "redis.call('publish', KEYS[2], ARGV[1]); " +
                "return 1; " +
                "end; " +
                "return 0; ",
            Arrays.<Object>asList(getRawName(), getChannelName()), LockPubSub.UNLOCK_MESSAGE);
    }

    @Override
    public boolean isLocked() {
        RFuture<String> future = commandExecutor.writeAsync(getRawName(), StringCodec.INSTANCE, RedisCommands.HGET, getRawName(), "mode");
        String res = get(future);
        return "read".equals(res);
    }

}
