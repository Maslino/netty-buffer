package io.netty.buffer;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * User: liuxiong
 * Date: 13-9-22
 * Time: 下午4:09
 */
public class MemoryOccupationTest {

    @Test
    public void testMemoryOccupationComputation() {
        PooledByteBufAllocator allocator = new PooledByteBufAllocator(true);
        final int CHUNK_SIZE = allocator.chunkSize();
        final int CHUNK_SIZE_MB = CHUNK_SIZE >>> 20;

        allocator.buffer(CHUNK_SIZE - 1);
        assertEquals(CHUNK_SIZE_MB, allocator.memoryOccupation());

        allocator.buffer(CHUNK_SIZE);
        assertEquals(2 * CHUNK_SIZE_MB, allocator.memoryOccupation());

        allocator.buffer(CHUNK_SIZE + 1);
        assertEquals(3 * CHUNK_SIZE_MB, allocator.memoryOccupation());
    }
}
