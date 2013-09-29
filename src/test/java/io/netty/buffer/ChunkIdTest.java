package io.netty.buffer;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * User: liuxiong
 * Date: 13-9-24
 * Time: 下午2:00
 */
public class ChunkIdTest {

    static final PooledByteBufAllocator allocator = new PooledByteBufAllocator(true);

    @Test
    public void testChunkId() {
        PoolThreadCache cache = allocator.threadCache.get();

        for (int i = 0; i < 10; i++) {
            PoolChunk<byte[]> chunk = new PoolChunk<byte[]>(cache.heapArena, new byte[1], 1);
            assertEquals(chunk.getId(), PoolChunk.getNextChunkId() - 1);
        }
    }

    static class ChunkIdTask implements Runnable {
        @Override
        public void run() {
            PoolThreadCache cache = allocator.threadCache.get();
            PoolChunk<byte[]> chunk = new PoolChunk<byte[]>(cache.heapArena, new byte[1], 1);
            System.out.println(Thread.currentThread() + " Chunk: " + chunk.getId());
        }
    }

    public static void main(String... args) {
        for (int i = 0; i < 20; i++) {
            Thread t = new Thread(new ChunkIdTask());
            t.start();
        }
    }
}
