package io.netty.buffer;

import io.netty.util.Pair;
import io.netty.util.internal.chmv8.ConcurrentHashMapV8;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

/**
 * User: liuxiong
 * Date: 13-9-25
 * Time: 下午5:57
 */
public class DiskSwapTest {

    private static final AtomicInteger count = new AtomicInteger(1);

    private static final ConcurrentHashMapV8<Pair<Long, Long>, Long> inMemoryMap = PooledByteBuf.getInMemoryMap();
    private static final ConcurrentHashMapV8<Long, int[]> onDiskMap = PooledByteBuf.getOnDiskMap();

    @Test
    public void testSwapSimple() {
        final PooledByteBufAllocator allocator = new PooledByteBufAllocator(true);

        ByteBuf bb1 = allocator.buffer(64 << 20);
        byte[] bytes1 = initByteBuf(bb1);

        ByteBuf bb2 = allocator.buffer((8 << 20) - 1);
        byte[] bytes2 = initByteBuf(bb2);

        ByteBuf bb3 = allocator.buffer((4 << 20) - 1);
        byte[] bytes3 = initByteBuf(bb3);

        ByteBuf bb4 = allocator.buffer((4 << 20) - 1);
        byte[] bytes4 = initByteBuf(bb4);

        // the following allocation will cause either bb3 or bb4 to be swapped out
        ByteBuf bb5 = allocator.buffer((2 << 20) - 1);
        byte[] bytes5 = initByteBuf(bb5);

        if (!onDiskMap.containsKey(bb3.getId())) {
            assertTrue(onDiskMap.containsKey(bb4.getId()));
        } else {
            assertFalse(onDiskMap.containsKey(bb4.getId()));
        }

        assertByteBuf(bb1, bytes1);
        assertByteBuf(bb2, bytes2);

        // the following access will cause either bb3 or bb4 to be swapped in
        assertByteBuf(bb3, bytes3);
        assertByteBuf(bb4, bytes4);

        bb3.release();
        bb4.release();

        assertByteBuf(bb5, bytes5);

        bb1.release();
        bb2.release();
        bb5.release();
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

    @Test
    public void testReallocate() {
        final PooledByteBufAllocator allocator = new PooledByteBufAllocator(true);

        allocator.buffer(64 << 20);
        allocator.buffer((8 << 20) - 1);
        ByteBuf bb3 = allocator.buffer((4 << 20) - 1);
        ByteBuf bb4 = allocator.buffer((2 << 20) - 1);
        // bb3 will be swapped out
        bb4.capacity(bb4.capacity() * 2);

        assertTrue(onDiskMap.containsKey(bb3.getId()));

        bb3.capacity(bb3.capacity() + 2);

        assertFalse(onDiskMap.containsKey(bb3.getId()));
    }
}
