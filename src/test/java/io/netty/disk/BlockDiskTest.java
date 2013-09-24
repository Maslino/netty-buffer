package io.netty.disk;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
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
    public void testSimpleWriteAndRead() throws IOException {
        Random random = new Random(System.currentTimeMillis());

        File tempFile = File.createTempFile("test", ".dat");
        BlockDisk blockDisk = new BlockDisk(tempFile.getAbsolutePath());

        short blockSizeBytes = blockDisk.getBlockSizeBytes();
        byte[] data = new byte[blockSizeBytes];
        random.nextBytes(data);

        int[] blocks = blockDisk.write(data);
        byte[] read = blockDisk.read(blocks);

        assertEquals(data.length, read.length);
        for (int i = 0; i < read.length; i++) {
            assertEquals(read[i], data[i]);
        }

        blockDisk.freeBlocks(blocks);
        assertEquals(blockDisk.getEmptyBlocks(), blockDisk.getNumOfBlocks());
    }

}
