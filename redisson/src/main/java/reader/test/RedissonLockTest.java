package reader.test;

import org.redisson.Redisson;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

/**
 * redisson测试
 *
 * @author huwansong
 * @version 1.0, 2022/6/11 10:39
 * @since 1
 */
public class RedissonLockTest {

    public static void main(String[] args) {
        Config config = new Config();
        config.useSingleServer().setAddress("redis://localhost:6379").setDatabase(0);
        RedissonClient redisson = Redisson.create(config);
        RLock lock = redisson.getFairLock("jd:order:9527");
        boolean locked = lock.tryLock();
        if (locked) {
            try {
                //do some business with lock

            } finally {
                lock.unlock();
            }
        }

    }
}