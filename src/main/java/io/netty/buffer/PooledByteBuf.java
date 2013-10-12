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

import io.netty.util.Pair;
import io.netty.util.Recycler;
import io.netty.util.ResourceLeak;
import io.netty.util.internal.chmv8.ConcurrentHashMapV8;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.atomic.AtomicLong;

abstract class PooledByteBuf<T> extends AbstractReferenceCountedByteBuf {

    private static final AtomicLong nextId = new AtomicLong(1);
    private static final ConcurrentHashMapV8<Pair<Long, Long>, Long> inMemoryMap =
        new ConcurrentHashMapV8<Pair<Long, Long>, Long>();
    private static final ConcurrentHashMapV8<Long, int[]> onDiskMap =
        new ConcurrentHashMapV8<Long, int[]>();

    private final ResourceLeak leak;
    private final Recycler.Handle recyclerHandle;

    protected Pair<Long, Long> inMemoryKey;
    protected long id;
    protected PoolChunk<T> chunk;
    protected long handle;
    protected T memory;
    protected int offset;
    protected int length;
    private int maxLength;

    private ByteBuffer tmpNioBuf;

    protected PooledByteBuf(Recycler.Handle recyclerHandle, int maxCapacity) {
        super(maxCapacity);
        leak = leakDetector.open(this);
        this.recyclerHandle = recyclerHandle;
        this.id = nextId.getAndIncrement();
    }

    void init(PoolChunk<T> chunk, long handle, int offset, int length, int maxLength) {
        assert handle >= 0;
        assert chunk != null;

        this.chunk = chunk;
        this.handle = handle;
        memory = chunk.memory;
        this.offset = offset;
        this.length = length;
        this.maxLength = maxLength;
        setIndex(0, 0);
        tmpNioBuf = null;

        inMemoryKey = new Pair<Long, Long>(this.chunk.getId(), this.handle);
        inMemoryMap.put(inMemoryKey, id);
    }

    void initUnpooled(PoolChunk<T> chunk, int length) {
        assert chunk != null;

        this.chunk = chunk;
        handle = 0;
        memory = chunk.memory;
        offset = 0;
        this.length = maxLength = length;
        setIndex(0, 0);
        tmpNioBuf = null;

        inMemoryKey = new Pair<Long, Long>(this.chunk.getId(), this.handle);
        inMemoryMap.put(inMemoryKey, id);
    }

    @Override
    public final int capacity() {
        return length;
    }

    @Override
    public final ByteBuf capacity(int newCapacity) {
        ensureAccessible();

        // If the request capacity does not require reallocation, just update the length of the memory.
        if (chunk.unpooled) {
            if (newCapacity == length) {
                return this;
            }
        } else {
            if (newCapacity > length) {
                if (newCapacity <= maxLength) {
                    length = newCapacity;
                    return this;
                }
            } else if (newCapacity < length) {
                if (newCapacity > maxLength >>> 1) {
                    if (maxLength <= 512) {
                        if (newCapacity > maxLength - 16) {
                            length = newCapacity;
                            setIndex(Math.min(readerIndex(), newCapacity), Math.min(writerIndex(), newCapacity));
                            return this;
                        }
                    } else { // > 512 (i.e. >= 1024)
                        length = newCapacity;
                        setIndex(Math.min(readerIndex(), newCapacity), Math.min(writerIndex(), newCapacity));
                        return this;
                    }
                }
            } else {
                return this;
            }
        }

        // Reallocation required.
        chunk.arena.reallocate(this, newCapacity, true);
        return this;
    }

    @Override
    public final ByteBufAllocator alloc() {
        return chunk.arena.parent;
    }

    @Override
    public final ByteOrder order() {
        return ByteOrder.BIG_ENDIAN;
    }

    @Override
    public final ByteBuf unwrap() {
        return null;
    }

    protected final ByteBuffer internalNioBuffer() {
        ByteBuffer tmpNioBuf = this.tmpNioBuf;
        if (tmpNioBuf == null || isOnDisk()) {
            swapInIfNeeded();
            this.tmpNioBuf = tmpNioBuf = newInternalNioBuffer(memory);
        }
        return tmpNioBuf;
    }

    protected abstract ByteBuffer newInternalNioBuffer(T memory);

    @Override
    protected final void deallocate() {
        if (handle >= 0) {
            final long handle = this.handle;
            this.handle = -1;
            memory = null;
            chunk.arena.freeAll(chunk, handle, id);

            if (leak != null) {
                leak.close();
            } else {
                recycle();
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void recycle() {
        Recycler.Handle recyclerHandle = this.recyclerHandle;
        if (recyclerHandle != null) {
            ((Recycler<Object>) recycler()).recycle(this, recyclerHandle);
        }
    }

    protected abstract Recycler<?> recycler();

    protected final int idx(int index) {
        return offset + index;
    }

    public static ConcurrentHashMapV8<Pair<Long, Long>, Long> getInMemoryMap() {
        return inMemoryMap;
    }

    public static ConcurrentHashMapV8<Long, int[]> getOnDiskMap() {
        return onDiskMap;
    }

    protected boolean isOnDisk() {
        return onDiskMap.containsKey(id);
    }

    protected void swapInIfNeeded() {
        if (!isOnDisk()) {
            return;
        }

        synchronized (this) {
            if (!isOnDisk()) {
                return;
            }

            long oldId = id;
            int oldRefCnt = refCnt();
            int oldLength = length;
            int oldMaxLength = maxLength;

            try {
                chunk.arena.swapIn(this, oldLength, oldMaxLength);
            } catch (IOException iox) {
                throw new RuntimeException(iox);
            }

            assert oldId == this.id;
            assert oldRefCnt == refCnt();
            assert oldLength == this.length;
            assert oldMaxLength == this.maxLength;
        }
    }
}
