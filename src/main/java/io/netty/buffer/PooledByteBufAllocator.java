/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package io.netty.buffer;

import io.netty.disk.BlockDisk;
import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.StringUtil;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class PooledByteBufAllocator extends AbstractByteBufAllocator {

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(PooledByteBufAllocator.class);

    private static final int DEFAULT_NUM_HEAP_ARENA;
    private static final int DEFAULT_NUM_DIRECT_ARENA;

    private static final int DEFAULT_PAGE_SIZE;
    private static final int DEFAULT_MAX_ORDER; // 8192 << 11 = 16 MiB per chunk

    private static final int MIN_PAGE_SIZE = 4096;
    private static final int MAX_CHUNK_SIZE = (int) (((long) Integer.MAX_VALUE + 1) / 2);

    private static final int DEFAULT_MAX_MEMORY_MB;    // in MB, default to 1024

    private static final BlockDisk<byte[]> heapBlockDisk;
    private static final BlockDisk<ByteBuffer> directBlockDisk;

    static {
        Properties allocatorProperties = new Properties();
        try {
            InputStream in = PooledByteBufAllocator.class.getResourceAsStream("/allocator.properties");
            allocatorProperties.load(in);
        } catch (IOException ex) {
            logger.warn("allocator.properties file not found.");
            throw new RuntimeException("allocator.properties file not found.", ex);
        }

        System.out.println(allocatorProperties);

        int defaultPageSize = Integer.valueOf(allocatorProperties.getProperty("pageSize", "8192"));
        Throwable pageSizeFallbackCause = null;
        try {
            validateAndCalculatePageShifts(defaultPageSize);
        } catch (Throwable t) {
            pageSizeFallbackCause = t;
            defaultPageSize = 8192;
        }
        DEFAULT_PAGE_SIZE = defaultPageSize;

        int defaultMaxOrder = Integer.valueOf(allocatorProperties.getProperty("maxOrder", "11"));
        Throwable maxOrderFallbackCause = null;
        try {
            validateAndCalculateChunkSize(DEFAULT_PAGE_SIZE, defaultMaxOrder);
        } catch (Throwable t) {
            maxOrderFallbackCause = t;
            defaultMaxOrder = 11;
        }
        DEFAULT_MAX_ORDER = defaultMaxOrder;

        // Determine reasonable default for nHeapArena and nDirectArena.
        // Assuming each arena has 3 chunks, the pool should not consume more than 50% of max memory.
        final Runtime runtime = Runtime.getRuntime();
        final int defaultChunkSize = DEFAULT_PAGE_SIZE << DEFAULT_MAX_ORDER;
        int numHeapArenas = Integer.valueOf(allocatorProperties.getProperty(
            "numHeapArenas",
            String.valueOf(Math.min(
                runtime.availableProcessors(),
                Runtime.getRuntime().maxMemory() / defaultChunkSize / 2 / 3)
            )));
        DEFAULT_NUM_HEAP_ARENA = Math.max(0, numHeapArenas);
        int numDirectArenas = Integer.valueOf(allocatorProperties.getProperty(
            "numDirectArenas",
            String.valueOf(Math.min(
                runtime.availableProcessors(),
                PlatformDependent.maxDirectMemory() / defaultChunkSize / 2 / 3)
            )));
        DEFAULT_NUM_DIRECT_ARENA = Math.max(0, numDirectArenas);

        int defaultMaxMemory = Integer.valueOf(allocatorProperties.getProperty("maxMemory", "1024"));
        Throwable maxMemoryFallbackCause = null;
        try {
            validateMaxMemory(DEFAULT_PAGE_SIZE, DEFAULT_MAX_ORDER,
                DEFAULT_NUM_HEAP_ARENA, DEFAULT_NUM_DIRECT_ARENA, defaultMaxMemory);
        } catch (Throwable t) {
            System.out.println(t);
            maxMemoryFallbackCause = t;
            defaultMaxMemory = 1024;
        }
        DEFAULT_MAX_MEMORY_MB = defaultMaxMemory;

        try {
            String swapDirStr = allocatorProperties.getProperty("swapDir");
            if (swapDirStr == null) {
                throw new RuntimeException("swapDir not configured.");
            }

            File swapDir = new File(swapDirStr);
            if (!(swapDir.exists() && swapDir.isDirectory())) {
                throw new RuntimeException("swapDir(" + swapDirStr + ") not exists or not a directory.");
            }

            heapBlockDisk = new BlockDisk.HeapBlockDisk(swapDirStr + File.separator + "heap.dat");
            directBlockDisk = new BlockDisk.DirectBlockDisk(swapDirStr + File.separator + "direct.dat");
        } catch (IOException iox) {
            throw new RuntimeException(iox);
        }

        if (logger.isDebugEnabled()) {
            logger.debug("numHeapArenas: {}", DEFAULT_NUM_HEAP_ARENA);
            logger.debug("numDirectArenas: {}", DEFAULT_NUM_DIRECT_ARENA);
            if (pageSizeFallbackCause == null) {
                logger.debug("pageSize: {}", DEFAULT_PAGE_SIZE);
            } else {
                logger.debug("pageSize: {}", DEFAULT_PAGE_SIZE, pageSizeFallbackCause);
            }
            if (maxOrderFallbackCause == null) {
                logger.debug("maxOrder: {}", DEFAULT_MAX_ORDER);
            } else {
                logger.debug("maxOrder: {}", DEFAULT_MAX_ORDER, maxOrderFallbackCause);
            }
            logger.debug("chunkSize: {}", DEFAULT_PAGE_SIZE << DEFAULT_MAX_ORDER);
            if (maxMemoryFallbackCause == null) {
                logger.debug("maxMemory: {}", DEFAULT_MAX_MEMORY_MB);
            } else {
                logger.debug("maxMemory: {}", DEFAULT_MAX_MEMORY_MB, maxMemoryFallbackCause);
            }
        }
    }

    public static final PooledByteBufAllocator DEFAULT =
            new PooledByteBufAllocator(PlatformDependent.directBufferPreferred());

    private final PoolArena<byte[]>[] heapArenas;
    private final PoolArena<ByteBuffer>[] directArenas;

    final ThreadLocal<PoolThreadCache> threadCache = new ThreadLocal<PoolThreadCache>() {
        private final AtomicInteger index = new AtomicInteger();
        @Override
        protected PoolThreadCache initialValue() {
            final int idx = index.getAndIncrement();
            final PoolArena<byte[]> heapArena;
            final PoolArena<ByteBuffer> directArena;

            if (heapArenas != null) {
                heapArena = heapArenas[Math.abs(idx % heapArenas.length)];
            } else {
                heapArena = null;
            }

            if (directArenas != null) {
                directArena = directArenas[Math.abs(idx % directArenas.length)];
            } else {
                directArena = null;
            }

            return new PoolThreadCache(heapArena, directArena);
        }
    };

    public PooledByteBufAllocator(boolean preferDirect) {
        this(preferDirect, DEFAULT_NUM_HEAP_ARENA, DEFAULT_NUM_DIRECT_ARENA, DEFAULT_PAGE_SIZE, DEFAULT_MAX_ORDER);
    }

    private PooledByteBufAllocator(boolean preferDirect, int nHeapArena, int nDirectArena, int pageSize, int maxOrder) {
        super(preferDirect);

        final int chunkSize = validateAndCalculateChunkSize(pageSize, maxOrder);

        if (nHeapArena < 0) {
            throw new IllegalArgumentException("nHeapArena: " + nHeapArena + " (expected: >= 0)");
        }
        if (nDirectArena < 0) {
            throw new IllegalArgumentException("nDirectArea: " + nDirectArena + " (expected: >= 0)");
        }

        int pageShifts = validateAndCalculatePageShifts(pageSize);

        if (nHeapArena > 0) {
            heapArenas = newArenaArray(nHeapArena);
            for (int i = 0; i < heapArenas.length; i ++) {
                heapArenas[i] = new PoolArena.HeapArena(this, pageSize, maxOrder, pageShifts, chunkSize);
            }
        } else {
            heapArenas = null;
        }

        if (nDirectArena > 0) {
            directArenas = newArenaArray(nDirectArena);
            for (int i = 0; i < directArenas.length; i ++) {
                directArenas[i] = new PoolArena.DirectArena(this, pageSize, maxOrder, pageShifts, chunkSize);
            }
        } else {
            directArenas = null;
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> PoolArena<T>[] newArenaArray(int size) {
        return new PoolArena[size];
    }

    private static int validateAndCalculatePageShifts(int pageSize) {
        if (pageSize < MIN_PAGE_SIZE) {
            throw new IllegalArgumentException("pageSize: " + pageSize + " (expected: 4096+)");
        }

        // Ensure pageSize is power of 2.
        boolean found1 = false;
        int pageShifts = 0;
        for (int i = pageSize; i != 0 ; i >>= 1) {
            if ((i & 1) != 0) {
                if (!found1) {
                    found1 = true;
                } else {
                    throw new IllegalArgumentException("pageSize: " + pageSize + " (expected: power of 2");
                }
            } else {
                if (!found1) {
                    pageShifts ++;
                }
            }
        }
        return pageShifts;
    }

    private static int validateAndCalculateChunkSize(int pageSize, int maxOrder) {
        if (maxOrder > 14) {
            throw new IllegalArgumentException("maxOrder: " + maxOrder + " (expected: 0-14)");
        }

        // Ensure the resulting chunkSize does not overflow.
        int chunkSize = pageSize;
        for (int i = maxOrder; i > 0; i --) {
            if (chunkSize > MAX_CHUNK_SIZE / 2) {
                throw new IllegalArgumentException(String.format(
                        "pageSize (%d) << maxOrder (%d) must not exceed %d", pageSize, maxOrder, MAX_CHUNK_SIZE));
            }
            chunkSize <<= 1;
        }
        return chunkSize;
    }

    private static void validateMaxMemory(int pageSize, int maxOrder, int nHeapArena, int nDirectArena, int maxMemory) {
        int chunkSizeInMB = (pageSize << maxOrder) >>> 20;
        if (chunkSizeInMB > maxMemory
            || nHeapArena * chunkSizeInMB > maxMemory
            || nDirectArena * chunkSizeInMB > maxMemory) {
            throw new IllegalArgumentException(String.format(
                "maxMemory (%dMB) must be equal or greater than " +
                    "chunkSize (%dMB) * nHeapArena (%d) and nDirectArena (%d)",
                maxMemory, chunkSizeInMB, nHeapArena, nDirectArena));
        }
    }

    @Override
    protected ByteBuf newHeapBuffer(int initialCapacity, int maxCapacity) {
        PoolThreadCache cache = threadCache.get();
        PoolArena<byte[]> heapArena = cache.heapArena;
        if (heapArena != null) {
            return heapArena.allocate(cache, initialCapacity, maxCapacity);
        } else {
            return new UnpooledHeapByteBuf(this, initialCapacity, maxCapacity);
        }
    }

    @Override
    protected ByteBuf newDirectBuffer(int initialCapacity, int maxCapacity) {
        PoolThreadCache cache = threadCache.get();
        PoolArena<ByteBuffer> directArena = cache.directArena;
        if (directArena != null) {
            return directArena.allocate(cache, initialCapacity, maxCapacity);
        } else {
            if (PlatformDependent.hasUnsafe()) {
                return new UnpooledUnsafeDirectByteBuf(this, initialCapacity, maxCapacity);
            } else {
                return new UnpooledDirectByteBuf(this, initialCapacity, maxCapacity);
            }
        }
    }

    /**
     *  for test
     * @return  chunk size
     */
    public int chunkSize() {
        return DEFAULT_PAGE_SIZE << DEFAULT_MAX_ORDER;
    }

    public static int getDefaultMaxMemoryMB() {
        return DEFAULT_MAX_MEMORY_MB;
    }

    public static BlockDisk<byte[]> getHeapBlockDisk() {
        return heapBlockDisk;
    }

    public static BlockDisk<ByteBuffer> getDirectBlockDisk() {
        return directBlockDisk;
    }

    @Override
    public boolean isDirectBufferPooled() {
        return directArenas != null;
    }

    public String toString() {
        StringBuilder buf = new StringBuilder();
        buf.append(heapArenas.length);
        buf.append(" heap arena(s):");
        buf.append(StringUtil.NEWLINE);
        for (PoolArena<byte[]> a: heapArenas) {
            buf.append(a);
        }
        buf.append(directArenas.length);
        buf.append(" direct arena(s):");
        buf.append(StringUtil.NEWLINE);
        for (PoolArena<ByteBuffer> a: directArenas) {
            buf.append(a);
        }
        return buf.toString();
    }

}
