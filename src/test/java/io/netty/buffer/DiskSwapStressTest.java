package io.netty.buffer;

import io.netty.util.Pair;

import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.Assert.assertEquals;

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


class AllocateTask implements Runnable {

    private final PooledByteBufAllocator allocator;
    private final Queue<Pair<ByteBuf, int[]>> bufQueue;

    AllocateTask(PooledByteBufAllocator allocator, Queue<Pair<ByteBuf, int[]>> bufQueue) {
        this.allocator = allocator;
        this.bufQueue = bufQueue;
    }

    @Override
    public void run() {
        final Random random = new Random(System.currentTimeMillis());

        while (true) {
            int sizeInKB = random.nextInt(63) + 1;

            ByteBuf byteBuf = allocator.buffer(sizeInKB << 10);
            int[] ints = initByteBuf(byteBuf);

            bufQueue.add(new Pair<ByteBuf, int[]>(byteBuf, ints));

            int sleepMillis = random.nextInt(100);
            try {
                Thread.sleep(sleepMillis);
            } catch (InterruptedException iex) {
                //
            }
        }
    }

    private int[] initByteBuf(ByteBuf buf) {
        Random random = new Random(System.currentTimeMillis());

        int[] ints = new int[buf.capacity() >>> 2];
        for (int i = 0; i < ints.length; i++) {
            ints[i] = random.nextInt();
            buf.setInt(i << 2, ints[i]);
        }

        return ints;
    }
}


class FreeTask implements Runnable {

    private final Queue<Pair<ByteBuf, int[]>> bufQueue;

    FreeTask(Queue<Pair<ByteBuf, int[]>> bufQueue) {
        this.bufQueue = bufQueue;
    }

    @Override
    public void run() {
        final Random random = new Random(System.currentTimeMillis());

        while (true) {
            Pair<ByteBuf, int[]> buf = bufQueue.poll();
            if (buf != null) {
                assertByteBuf(buf.first, buf.second);
                buf.first.release();
            }

            int sleepMillis = random.nextInt(1000);
            try {
                Thread.sleep(sleepMillis);
            } catch (InterruptedException iex) {
                //
            }
        }
    }

    private void assertByteBuf(ByteBuf buf, int[] ints) {
        assertEquals(buf.capacity() >>> 2, ints.length);

        for (int i = 0; i < ints.length; i++) {
            assertEquals(buf.getInt(i << 2), ints[i]);
        }
    }
}


public class DiskSwapStressTest {

    public static void main(String... args) {
        PooledByteBufAllocator allocator = new PooledByteBufAllocator(true);
        Queue<Pair<ByteBuf, int[]>> bufQueue = new ConcurrentLinkedQueue<Pair<ByteBuf, int[]>>();

        for (int i = 0; i < 1; i++) {
//            Thread thread = new Thread(new AllocateRunnable(allocator));
            Thread allocateThread = new Thread(new AllocateTask(allocator, bufQueue));
            Thread freeThread = new Thread(new FreeTask(bufQueue));
            allocateThread.start();
            freeThread.start();
        }

        for (Pair<ByteBuf, int[]> pair : bufQueue) {
            pair.first.release();
        }
        bufQueue.clear();
    }
}
