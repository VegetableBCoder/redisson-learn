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

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import org.redisson.PubSubMessageListener;
import org.redisson.PubSubPatternMessageListener;
import org.redisson.client.*;
import org.redisson.client.codec.Codec;
import org.redisson.client.protocol.pubsub.PubSubStatusMessage;
import org.redisson.client.protocol.pubsub.PubSubType;
import org.redisson.connection.ConnectionManager;

import java.util.EventListener;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 
 * @author Nikita Koksharov
 *
 */
public class PubSubConnectionEntry {

    private final AtomicInteger subscribedChannelsAmount;
    private final RedisPubSubConnection conn;

    private final ConcurrentMap<ChannelName, SubscribeListener> subscribeChannelListeners = new ConcurrentHashMap<ChannelName, SubscribeListener>();
    private final ConcurrentMap<ChannelName, Queue<RedisPubSubListener<?>>> channelListeners = new ConcurrentHashMap<ChannelName, Queue<RedisPubSubListener<?>>>();

    private static final Queue<RedisPubSubListener<?>> EMPTY_QUEUE = new LinkedList<>();

    private final ConnectionManager connectionManager;

    public PubSubConnectionEntry(RedisPubSubConnection conn, ConnectionManager connectionManager) {
        super();
        this.conn = conn;
        this.connectionManager = connectionManager;
        this.subscribedChannelsAmount = new AtomicInteger(connectionManager.getConfig().getSubscriptionsPerConnection());
    }

    public int countListeners(ChannelName channelName) {
        return channelListeners.getOrDefault(channelName, EMPTY_QUEUE).size();
    }
    
    public boolean hasListeners(ChannelName channelName) {
        return channelListeners.containsKey(channelName);
    }

    public Queue<RedisPubSubListener<?>> getListeners(ChannelName channelName) {
        return channelListeners.getOrDefault(channelName, EMPTY_QUEUE);
    }

    public void addListener(ChannelName channelName, RedisPubSubListener<?> listener) {
        if (listener == null) {
            return;
        }
        
        Queue<RedisPubSubListener<?>> queue = channelListeners.get(channelName);
        if (queue == null) {
            queue = new ConcurrentLinkedQueue<RedisPubSubListener<?>>();
            Queue<RedisPubSubListener<?>> oldQueue = channelListeners.putIfAbsent(channelName, queue);
            // 并发写入的
            if (oldQueue != null) {
                queue = oldQueue;
            }
        }

        boolean deleted = false;
        synchronized (queue) {
            // 如果拿出来的和队列里面的不是一个对象 synchronized之前的并发了 这里进行判断是避免线程安全问题
            if (channelListeners.get(channelName) != queue) {
                //  标记为被删了
                deleted = true;
            } else {
                //加到队列尾部就好了
                queue.add(listener);
            }
        }
        if (deleted) {
            //重新调用自己 map中的队列对象(这次拿出来肯定不是null了)
            addListener(channelName, listener);
            //避免重复往connection的listener中加
            return;
        }
        //加到连接的监听器列表中
        conn.addListener(listener);
    }

    // TODO optimize
    public boolean removeListener(ChannelName channelName, EventListener msgListener) {
        Queue<RedisPubSubListener<?>> listeners = channelListeners.get(channelName);
        for (RedisPubSubListener<?> listener : listeners) {
            // subscribe和psubcribe
            if (listener instanceof PubSubMessageListener) {
                if (((PubSubMessageListener<?>) listener).getListener() == msgListener) {
                    removeListener(channelName, listener);
                    return true;
                }
            }
            if (listener instanceof PubSubPatternMessageListener) {
                if (((PubSubPatternMessageListener<?>) listener).getListener() == msgListener) {
                    removeListener(channelName, listener);
                    return true;
                }
            }
        }
        return false;
    }
    
    public boolean removeListener(ChannelName channelName, int listenerId) {
        Queue<RedisPubSubListener<?>> listeners = channelListeners.get(channelName);
        for (RedisPubSubListener<?> listener : listeners) {
            if (System.identityHashCode(listener) == listenerId) {
                removeListener(channelName, listener);
                return true;
            }
        }
        return false;
    }

    public void removeListener(ChannelName channelName, RedisPubSubListener<?> listener) {
        Queue<RedisPubSubListener<?>> queue = channelListeners.get(channelName);
        synchronized (queue) {
            //没有监听器了 queue也不要了
            if (queue.remove(listener) && queue.isEmpty()) {
                channelListeners.remove(channelName);
            }
        }
        conn.removeListener(listener);
    }

    public int tryAcquire() {
        while (true) {
            int value = subscribedChannelsAmount.get();
            if (value == 0) {
                return -1;
            }
            
            if (subscribedChannelsAmount.compareAndSet(value, value - 1)) {
                return value - 1;
            }
        }
    }

    public int release() {
        return subscribedChannelsAmount.incrementAndGet();
    }

    public void subscribe(Codec codec, PubSubType type, ChannelName channelName, CompletableFuture<Void> subscribeFuture) {
        ChannelFuture future;
        if (PubSubType.SUBSCRIBE == type) {
            future = conn.subscribe(codec, channelName);
        } else if (PubSubType.SSUBSCRIBE == type) {
            future = conn.ssubscribe(codec, channelName);
        } else {
            future = conn.psubscribe(codec, channelName);
        }

        future.addListener((ChannelFutureListener) future1 -> {
            if (!future1.isSuccess()) {
                subscribeFuture.completeExceptionally(future1.cause());
                return;
            }

            connectionManager.newTimeout(t -> {
                subscribeFuture.completeExceptionally(new RedisTimeoutException(
                        "Subscription timeout after " + connectionManager.getConfig().getTimeout() + "ms. " +
                                "Check network and/or increase 'timeout' parameter."));
            }, connectionManager.getConfig().getTimeout(), TimeUnit.MILLISECONDS);
        });
    }

    public SubscribeListener getSubscribeFuture(ChannelName channel, PubSubType type) {
        SubscribeListener listener = subscribeChannelListeners.get(channel);
        if (listener == null) {
            listener = new SubscribeListener(channel, type);
            SubscribeListener oldSubscribeListener = subscribeChannelListeners.putIfAbsent(channel, listener);
            if (oldSubscribeListener != null) {
                listener = oldSubscribeListener;
            } else {
                conn.addListener(listener);
            }
        }
        return listener;
    }
    
    public void unsubscribe(PubSubType commandType, ChannelName channel, RedisPubSubListener<?> listener) {
        AtomicBoolean executed = new AtomicBoolean();
        conn.addListener(new BaseRedisPubSubListener() {
            @Override
            public boolean onStatus(PubSubType type, CharSequence ch) {
                if (type == commandType && channel.equals(ch)) {
                    executed.set(true);

                    conn.removeListener(this);
                    removeListeners(channel);
                    if (listener != null) {
                        listener.onStatus(type, ch);
                    }
                    return true;
                }
                return false;
            }
        });

        ChannelFuture future = conn.unsubscribe(commandType, channel);
        future.addListener((ChannelFutureListener) f -> {
            if (!f.isSuccess()) {
                return;
            }

            connectionManager.newTimeout(timeout -> {
                if (executed.get()) {
                    return;
                }
                conn.onMessage(new PubSubStatusMessage(commandType, channel));
            }, connectionManager.getConfig().getTimeout(), TimeUnit.MILLISECONDS);
        });
    }

    private void removeListeners(ChannelName channel) {
        conn.removeDisconnectListener(channel);
        SubscribeListener s = subscribeChannelListeners.remove(channel);
        conn.removeListener(s);
        Queue<RedisPubSubListener<?>> queue = channelListeners.get(channel);
        if (queue != null) {
            synchronized (queue) {
                channelListeners.remove(channel);
            }
            for (RedisPubSubListener<?> listener : queue) {
                conn.removeListener(listener);
            }
        }
    }

    public RedisPubSubConnection getConnection() {
        return conn;
    }

    @Override
    public String toString() {
        return "PubSubConnectionEntry [subscribedChannelsAmount=" + subscribedChannelsAmount + ", conn=" + conn + "]";
    }
    
}
