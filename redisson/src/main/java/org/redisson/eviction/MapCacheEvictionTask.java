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
package org.redisson.eviction;

import java.util.Arrays;

import org.redisson.RedissonObject;
import org.redisson.api.RFuture;
import org.redisson.client.codec.LongCodec;
import org.redisson.client.protocol.RedisCommands;
import org.redisson.command.CommandAsyncExecutor;

/**
 *
 * @author Nikita Koksharov
 *
 */
public class MapCacheEvictionTask extends EvictionTask {

    //mapCache的 key name
    private final String name;
    //timeout的zset名称
    private final String timeoutSetName;
    private final String maxIdleSetName;
    private final String expiredChannelName;
    private final String lastAccessTimeSetName;
    private final String executeTaskOnceLatchName;

    public MapCacheEvictionTask(String name, String timeoutSetName, String maxIdleSetName,
            String expiredChannelName, String lastAccessTimeSetName, CommandAsyncExecutor executor) {
        super(executor);
        this.name = name;
        this.timeoutSetName = timeoutSetName;
        this.maxIdleSetName = maxIdleSetName;
        this.expiredChannelName = expiredChannelName;
        this.lastAccessTimeSetName = lastAccessTimeSetName;
        this.executeTaskOnceLatchName = RedissonObject.prefixName("redisson__execute_task_once_latch", name);
    }

    @Override
    String getName() {
        return name;
    }

    @Override
    RFuture<Integer> execute() {
        int latchExpireTime = Math.min(delay, 30);
        // 参数1 当前时间戳
        // 参数2 最多对多少个key进行过期删除(默认100个) 有点像G1垃圾回收器 尽量减少单次GC性能损耗
        // lua的for 初始值 max,每次的增量
        return
            //keys[6]=redisson__execute_task_once_latch:{key}  redisson_map_cache_expired:{key}
            // 使用setnx加锁 是为了防止一个周期内多次清理
            executor.evalWriteNoRetryAsync(name, LongCodec.INSTANCE, RedisCommands.EVAL_INTEGER,
                "if redis.call('setnx', KEYS[6], ARGV[4]) == 0 then "
                 + "return -1;"
              + "end;"
            // 为这个清理的任务的锁设置一个过期时间 让他自己过期就好 latchExpireTime (这个跟最近清理的结果有关)
              + "redis.call('expire', KEYS[6], ARGV[3]); "
            // 从超时的 timeout set 按时间戳(score) 查询过期keys(对多keysLimit个) 并为它声明一个局部变量 expiredKeys1
            // zrangebyscore redisson__timeout__set:{key-name} 0 当前时间戳 limit 0 keysLimit
               +"local expiredKeys1 = redis.call('zrangebyscore', KEYS[2], 0, ARGV[1], 'limit', 0, ARGV[2]); "
            // 查询每个已过期的 判断他们是不是再hash中还存在
                + "for i, key in ipairs(expiredKeys1) do "
                    + "local v = redis.call('hget', KEYS[1], key); "
                    + "if v ~= false then "
                    // 这里这个struct.unpack是lua的解包方法 dLc0 d表示 double L表示unsigned  Long(这里是说明后面动态字符串的长度) , c0:动态长度的字符序列
                    // 整体的用途是 将结果拆分为 前面的数字序号 和后面的key字符串
                        + "local t, val = struct.unpack('dLc0', v); "
                    // 打包结果是将key和val拼接起来 通过publish发送给其他subscriber(主要是给本地缓存清理) 但是开源版并不同时支持本地缓存和hash key淘汰机制
                        + "local msg = struct.pack('Lc0Lc0', string.len(key), key, string.len(val), val); "
                        + "local listeners = redis.call('publish', KEYS[4], msg); "
                    // 这里感觉写的有点潦草.. 为什么在这里再break 不先判断再进来
                        + "if (listeners == 0) then "
                            + "break;"
                        + "end; "
                    + "end;"
                + "end;"
                // #数组 ,getn返回的都是最大下标 且遇到NIL就不统计了 getn({1,2,nil,3})=2
                + "for i=1, #expiredKeys1, 5000 do "
                    //unpack把数组拆出来 一批最多删5000条
                    + "redis.call('zrem', KEYS[5], unpack(expiredKeys1, i, math.min(i+4999, table.getn(expiredKeys1)))); "
                    + "redis.call('zrem', KEYS[3], unpack(expiredKeys1, i, math.min(i+4999, table.getn(expiredKeys1)))); "
                    + "redis.call('zrem', KEYS[2], unpack(expiredKeys1, i, math.min(i+4999, table.getn(expiredKeys1)))); "
                    + "redis.call('hdel', KEYS[1], unpack(expiredKeys1, i, math.min(i+4999, table.getn(expiredKeys1)))); "
                + "end; "
                // 从没有使用到的idle set 取前 100条(默认100) (按照上次访问时间排序的)
              + "local expiredKeys2 = redis.call('zrangebyscore', KEYS[3], 0, ARGV[1], 'limit', 0, ARGV[2]); "
              + "for i, key in ipairs(expiredKeys2) do "
                    //消息发布
                  + "local v = redis.call('hget', KEYS[1], key); "
                  + "if v ~= false then "
                      + "local t, val = struct.unpack('dLc0', v); "
                      + "local msg = struct.pack('Lc0Lc0', string.len(key), key, string.len(val), val); "
                      + "local listeners = redis.call('publish', KEYS[4], msg); "
                      + "if (listeners == 0) then "
                          + "break;"
                      + "end; "
                  + "end;"
              + "end;"
            //一样的删除
              + "for i=1, #expiredKeys2, 5000 do "
                  + "redis.call('zrem', KEYS[5], unpack(expiredKeys2, i, math.min(i+4999, table.getn(expiredKeys2)))); "
                  + "redis.call('zrem', KEYS[3], unpack(expiredKeys2, i, math.min(i+4999, table.getn(expiredKeys2)))); "
                  + "redis.call('zrem', KEYS[2], unpack(expiredKeys2, i, math.min(i+4999, table.getn(expiredKeys2)))); "
                  + "redis.call('hdel', KEYS[1], unpack(expiredKeys2, i, math.min(i+4999, table.getn(expiredKeys2)))); "
              + "end; "
            // 返回的 timeout_set清理的数量和
              + "return #expiredKeys1 + #expiredKeys2;",
              Arrays.<Object>asList(name, timeoutSetName, maxIdleSetName, expiredChannelName, lastAccessTimeSetName, executeTaskOnceLatchName),
              System.currentTimeMillis(), keysLimit, latchExpireTime, 1);
    }

}
