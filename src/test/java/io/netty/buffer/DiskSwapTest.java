package io.netty.buffer;

/**
 * User: liuxiong
 * Date: 13-9-25
 * Time: 下午5:57
 */
public class DiskSwapTest {

    public void testSwapOut() {
        PooledByteBufAllocator allocator = new PooledByteBufAllocator(false);
        allocator.buffer(64 << 20);
        allocator.buffer((8 << 20) - 1);
        allocator.buffer((4 << 20) - 1);
        allocator.buffer((4 << 20) - 1);
        System.out.println(allocator);
        allocator.buffer(20 << 10);
        System.out.println(PooledByteBuf.getInMemoryMap());
        System.out.println(PooledByteBuf.getOnDiskMap());
    }

    public static void main(String... args) {
        new DiskSwapTest().testSwapOut();
    }
}
