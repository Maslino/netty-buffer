package io.netty.buffer;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

/**
 * User: liuxiong
 * Date: 13-9-25
 * Time: 下午5:57
 */
public class DiskSwapTest {

    private static final AtomicInteger count = new AtomicInteger(1);

    public void testSwapOut() {
        PooledByteBufAllocator allocator = new PooledByteBufAllocator(true);

        mark();
        ByteBuf bb1 = allocator.buffer(64 << 20);
        byte[] bytes1 = initByteBuf(bb1);

        mark();
        ByteBuf bb2 = allocator.buffer((8 << 20) - 1);
        byte[] bytes2 = initByteBuf(bb2);

        mark();
        ByteBuf bb3 = allocator.buffer((4 << 20) - 1);
        byte[] bytes3 = initByteBuf(bb3);

        mark();
        ByteBuf bb4 = allocator.buffer((4 << 20) - 1);
        byte[] bytes4 = initByteBuf(bb4);

        printMap();

        mark();
        ByteBuf bb5 = allocator.buffer((2 << 20) - 1);
        byte[] bytes5 = initByteBuf(bb5);

        System.out.println("----------------------------------");
        printMap();

        assertByteBuf(bb1, bytes1);
        assertByteBuf(bb2, bytes2);
        printMap();
        assertByteBuf(bb3, bytes3);
        printMap();
        assertByteBuf(bb4, bytes4);
        bb3.release();
        printMap();
        bb4.release();
        printMap();
        assertByteBuf(bb5, bytes5);

        bb1.release();
        bb2.release();
        bb5.release();

        printMap();
    }

    private byte[] initByteBuf(ByteBuf buf) {
        Random random = new Random(System.currentTimeMillis());

        byte[] bytes = new byte[buf.capacity()];
        random.nextBytes(bytes);
        for (int i = 0; i < buf.capacity(); i++) {
            buf.setByte(i, bytes[i]);
        }

        return bytes;
    }

    private void assertByteBuf(ByteBuf buf, byte[] bytes) {
        for (int i = 0; i < buf.capacity(); i++) {
            assertEquals(buf.getByte(i), bytes[i]);
        }
    }

    private static void mark() {
        System.out.println(
            "*************** " +
            count.getAndIncrement() +
            " ***************");
    }

    private static void printMap() {
        System.out.println(PooledByteBuf.getInMemoryMap());
        System.out.println(PooledByteBuf.getOnDiskMap());
        System.out.println();
    }

    public static void main(String... args) {
        new DiskSwapTest().testSwapOut();
    }
}
