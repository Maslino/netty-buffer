package io.netty.disk;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

import static org.junit.Assert.assertEquals;

/**
 * User: liuxiong
 * Date: 13-9-24
 * Time: 下午4:57
 */
public class BlockDiskTest {

    @Test(expected = IllegalArgumentException.class)
    public void testBlockSizeNotInRange() {
        BlockDisk.validateBlockSize((short)1023);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBlockSizeNotPowerOfTwo() {
        BlockDisk.validateBlockSize((short)4095);
    }

    @Test
    public void testSimpleWriteAndReadHeapBlockDisk() throws IOException {
        Random random = new Random(System.currentTimeMillis());

        File tempFile = File.createTempFile("heap", ".dat");
        BlockDisk<byte[]> heapBlockDisk = new BlockDisk.HeapBlockDisk(tempFile.getAbsolutePath());

        short blockSizeBytes = heapBlockDisk.getBlockSizeBytes();
        byte[] data = new byte[blockSizeBytes];
        random.nextBytes(data);

        int[] blocks = heapBlockDisk.write(data);
        byte[] read = heapBlockDisk.read(blocks);

        assertEquals(data.length, read.length);
        for (int i = 0; i < read.length; i++) {
            assertEquals(read[i], data[i]);
        }

        heapBlockDisk.freeBlocks(blocks);
        assertEquals(heapBlockDisk.getEmptyBlocks(), heapBlockDisk.getNumOfBlocks());
    }

    @Test
    public void testSimpleWriteAndReadDirectBlockDisk() throws IOException {
        Random random = new Random(System.currentTimeMillis());

        File tempFile = File.createTempFile("direct", ".dat");
        BlockDisk<ByteBuffer> directBlockDisk = new BlockDisk.DirectBlockDisk(tempFile.getAbsolutePath());

        short blockSizeBytes = directBlockDisk.getBlockSizeBytes();
        byte[] data = new byte[blockSizeBytes];
        random.nextBytes(data);

        ByteBuffer bufferData = ByteBuffer.wrap(data);

        int[] blocks = directBlockDisk.write(bufferData);
        ByteBuffer read = directBlockDisk.read(blocks);

        assertEquals(data.length, read.remaining());
        for (int i = 0; i < data.length; i++) {
            assertEquals(read.get(i), data[i]);
        }

        directBlockDisk.freeBlocks(blocks);
        assertEquals(directBlockDisk.getEmptyBlocks(), directBlockDisk.getNumOfBlocks());
    }

}
