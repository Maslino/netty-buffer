package io.netty.buffer;

import java.util.Random;

/**
 * User: liuxiong
 * Date: 13-10-12
 * Time: 下午1:39
 */
class AllocateRunnable implements Runnable {

    private final PooledByteBufAllocator allocator;

    AllocateRunnable(PooledByteBufAllocator allocator) {
        this.allocator = allocator;
    }

    @Override
    public void run() {
        final Random random = new Random(System.currentTimeMillis());
        final int maxMemory = PooledByteBufAllocator.getDefaultMaxMemoryMB();
        final int chunkSizeMB = allocator.chunkSize() >> 20;

        while (true) {
            int sizeInKB = random.nextInt(62) + 1;
            int currentMemory = PoolArena.getMemoryOccupationInMB();

            System.out.println("allocate size: " + sizeInKB + "KB");
            if (currentMemory > maxMemory + chunkSizeMB) {
                System.out.println("current: " + currentMemory + ", max: " + maxMemory);
                allocator.buffer(sizeInKB << 10);
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException iex) {
                    //
                }
            } else {
                allocator.buffer(sizeInKB << 10);
            }
        }
    }
}

public class DiskSwapStressTest {

    public static void main(String... args) throws InterruptedException {
        PooledByteBufAllocator allocator = new PooledByteBufAllocator(true);

        for (int i = 0; i < 10; i++) {
            Thread thread = new Thread(new AllocateRunnable(allocator));
            thread.start();
        }
    }
}
