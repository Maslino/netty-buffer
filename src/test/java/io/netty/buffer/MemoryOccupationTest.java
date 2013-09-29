package io.netty.buffer;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * User: liuxiong
 * Date: 13-9-22
 * Time: 下午4:09
 */
public class MemoryOccupationTest {

    static final PooledByteBufAllocator directAllocator = new PooledByteBufAllocator(true);
    static final PooledByteBufAllocator heapAllocator = new PooledByteBufAllocator(false);
    static final int CHUNK_SIZE = directAllocator.chunkSize();
    static final int CHUNK_SIZE_MB = CHUNK_SIZE >>> 20;

    @Test
    public void testMemoryOccupationComputation() {
        int initialValue = PoolArena.getMemoryOccupationInMB();

        directAllocator.buffer(CHUNK_SIZE - 1);
        assertEquals(CHUNK_SIZE_MB, PoolArena.getMemoryOccupationInMB() - initialValue);

        directAllocator.buffer(CHUNK_SIZE);
        assertEquals(2 * CHUNK_SIZE_MB, PoolArena.getMemoryOccupationInMB() - initialValue);

        directAllocator.buffer(CHUNK_SIZE + 1);
        assertEquals(3 * CHUNK_SIZE_MB, PoolArena.getMemoryOccupationInMB() - initialValue);
    }

    static class AllocateTask implements Runnable {
        @Override
        public void run() {
            directAllocator.buffer(CHUNK_SIZE - 1);
            heapAllocator.buffer(CHUNK_SIZE - 1);
        }
    }

    public static void main(String... args) throws InterruptedException{
        for (int i = 0; i < 10; i++) {
            Thread t = new Thread(new AllocateTask());
            t.start();
        }
        Thread.sleep(1000);
        System.out.println(PoolArena.getMemoryOccupationInMB());
    }
}
