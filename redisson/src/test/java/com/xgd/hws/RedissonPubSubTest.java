package com.xgd.hws;

import org.junit.jupiter.api.Test;
import org.redisson.Redisson;
import org.redisson.api.RPatternTopic;
import org.redisson.api.listener.PatternMessageListener;
import org.redisson.api.listener.PatternStatusListener;
import org.redisson.client.codec.StringCodec;

import java.util.concurrent.CountDownLatch;

/**
 * @Classname RedissonPubSubTest
 * @Description TODO
 * @Date 2022/7/17 11:32
 * @Created by huwansong
 */
class RedissonPubSubTest {
    private static final Redisson redisson = (Redisson) Redisson.create();

    @Test
    void testStatusListener() throws InterruptedException {
        RPatternTopic patternTopic = redisson.getPatternTopic("test.topic.*", StringCodec.INSTANCE);
        //订阅状态监听
        int id1 = patternTopic.addListener(new PatternStatusListener() {

            @Override
            public void onPSubscribe(String pattern) {
                System.out.println("subscribe pattern " + pattern);
            }

            @Override
            public void onPUnsubscribe(String pattern) {
                System.out.println("unsubscribe pattern " + pattern);
            }
        });
        //订阅数据监听
        int id2 = patternTopic.addListener(String.class, new PatternMessageListener<String>() {
            @Override
            public void onMessage(CharSequence pattern, CharSequence channel, String msg) {
                System.out.println("Accept Message from pattern subscribe" + pattern + " channel " + channel + ",msg: " + msg);
            }
        });
        Thread.sleep(10 * 60 * 1000);
        patternTopic.removeListener(id1);
        patternTopic.removeListener(id2);
        Thread.sleep(3 * 1000);
    }
}
