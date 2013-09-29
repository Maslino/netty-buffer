package io.netty.disk;

import java.io.File;
import java.io.IOException;
import java.util.Random;

/**
 * User: liuxiong
 * Date: 13-9-27
 * Time: 下午5:09
 */
public class HeapBlockDiskBenchmark {

    private static BlockDisk<byte[]> getHeapBlockDisk(short blockSize)  throws IOException {
        File tempFile = File.createTempFile("heap", ".dat");
        return new BlockDisk.HeapBlockDisk(tempFile.getAbsolutePath(), blockSize);
    }

    public static void main(String... args) throws IOException {
        Random random = new Random(System.currentTimeMillis());

        int blockSize = 4 << 10;
        BlockDisk<byte[]> blockDisk = getHeapBlockDisk((short)blockSize);

        int[] sizes = new int[4096];
        for (int i = 0; i < sizes.length; i++) {
            sizes[i] = blockSize * (i + 1);
        }

        for (int i = 0; i < sizes.length; i++) {
            byte[] bytes = new byte[sizes[i]];
            random.nextBytes(bytes);

            long start = System.currentTimeMillis();
            int[] blocks = blockDisk.write(bytes);
            long end = System.currentTimeMillis();

            System.out.println(
                "[" + i + "] " +
                " size: " + (sizes[i] >> 10) + "KB" +
                ", blocks: " + blocks.length +
                ", write time: " + (end - start) + "ms");

            start = System.currentTimeMillis();
            byte[] read = blockDisk.read(blocks);
            end = System.currentTimeMillis();

            assert bytes.length == read.length;
            for (int j = 0; j < bytes.length; j++) {
                assert bytes[j] == read[j];
            }

            System.out.println(
                "[" + i + "] " +
                " size: " + (sizes[i] >> 10) + "KB" +
                ", blocks: " + blocks.length +
                ", read time: " + (end - start) + "ms");

            blockDisk.freeBlocks(blocks);
        }
    }
}
