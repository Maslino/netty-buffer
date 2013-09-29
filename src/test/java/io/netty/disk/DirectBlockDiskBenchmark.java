package io.netty.disk;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

/**
 * User: liuxiong
 * Date: 13-9-25
 * Time: 下午7:57
 */
public class DirectBlockDiskBenchmark {

    private static BlockDisk<ByteBuffer> getDirectBlockDisk(short blockSize)  throws IOException {
        File tempFile = File.createTempFile("direct", ".dat");
        return new BlockDisk.DirectBlockDisk(tempFile.getAbsolutePath(), blockSize);
    }

    public static void main(String... args) throws IOException {
        Random random = new Random(System.currentTimeMillis());

        int blockSize = 4 << 10;
        BlockDisk<ByteBuffer> blockDisk = getDirectBlockDisk((short) blockSize);

        int[] sizes = new int[4096];
        for (int i = 0; i < sizes.length; i++) {
            sizes[i] = blockSize * (i + 1);
        }

        for (int i = 0; i < sizes.length; i++) {
            byte[] bytes = new byte[sizes[i]];
            random.nextBytes(bytes);

            ByteBuffer buffer = ByteBuffer.allocateDirect(sizes[i]);
            buffer.put(bytes);
            buffer.flip();

            long start = System.currentTimeMillis();
            int[] blocks = blockDisk.write(buffer);
            long end = System.currentTimeMillis();

            System.out.println(
                "[" + i + "] " +
                " size: " + (sizes[i] >> 10) + "KB" +
                ", blocks: " + blocks.length +
                ", write time: " + (end - start) + "ms");

            start = System.currentTimeMillis();
            ByteBuffer read = blockDisk.read(blocks);
            end = System.currentTimeMillis();

            assert bytes.length == read.remaining();
            for (int j = 0; j < bytes.length; j++) {
                assert bytes[j] == read.get(j);
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
