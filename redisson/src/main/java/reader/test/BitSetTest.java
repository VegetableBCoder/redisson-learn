package reader.test;

import org.redisson.api.RedissonClient;

/**
 * @author huwansong
 * @version 1.0, 2022/7/11 14:04
 * @since
 */
public class BitSetTest {

//    public static void main(String[] args) {
//        RedissonClient redisson = Redisson.create();
//        RBitSet rBitSet = redisson.getBitSet("test-bset");
//        //MAX_VALUE+1=MIN_VALUE
//        rBitSet.set(1 << 21);
//        //太大会卡住
//        long size = rBitSet.sizeInMemory();
//        //预期是2^21/2^3/2^10=2^8 即256KB
//        System.out.println(size / 1024);
//        redisson.shutdown();
//    }

    private  RedissonClient redisson;


}
