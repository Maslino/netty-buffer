package io.netty.buffer;

import java.util.Random;

/**
 * User: liuxiong
 * Date: 13-9-25
 * Time: 下午5:57
 */
public class DiskSwapTest {

    public void testSwapOut() {
        Random random = new Random(System.currentTimeMillis());

        PooledByteBufAllocator allocator = new PooledByteBufAllocator(true);

        ByteBuf bb1 = allocator.buffer(64 << 20);
        ByteBuf bb2 = allocator.buffer((8 << 20) - 1);

        byte[] bytes = new byte[bb2.capacity()];
        random.nextBytes(bytes);
        for (int i = 0; i < bb2.capacity(); i++) {
            bb2.setByte(i, bytes[i]);
        }

        ByteBuf bb3 = allocator.buffer((4 << 20) - 1);
        ByteBuf bb4 = allocator.buffer((4 << 20) - 1);
        ByteBuf bb5 = allocator.buffer(20 << 10);

        printMap();
        for (int i = 0; i < bb2.capacity(); i++) {
            byte b = bb2.getByte(i);
            assert b == bytes[i];
        }
        printMap();
        bb2.release();
        printMap();
    }

    private static void printMap() {
        System.out.println(PooledByteBuf.getInMemoryMap());
        System.out.println(PooledByteBuf.getOnDiskMap());
    }

    public static void main(String... args) {
        new DiskSwapTest().testSwapOut();
    }
}
