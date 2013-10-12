package io.netty.disk;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class manages reading and writing data to disk. When asked to write a value, it returns a
 * block array. It can read an object from the block numbers in a byte array.
 */
public abstract class BlockDisk<T> {

    /** The size of the header that indicates the amount of data stored in an occupied block. */
    private static final byte HEADER_SIZE_BYTES = 2;

    /** defaults to 4kb */
    private static final short DEFAULT_BLOCK_SIZE_BYTES = 4 << 10;

    /** Size of the block */
    protected final short blockSizeBytes;

    /**
     * the total number of blocks that have been used. If there are no free, we will use this to
     * calculate the position of the next block.
     */
    private final AtomicInteger numberOfBlocks = new AtomicInteger(0);

    /** Empty blocks that can be reused. */
    private final SingleLinkedList<Integer> emptyBlocks = new SingleLinkedList<Integer>();

    /** Location of the spot on disk */
    private final String filepath;

    /** File channel for multiple concurrent reads and writes */
    protected final FileChannel fileChannel;

    protected BlockDisk(String filepath) throws IOException {
        this(filepath, DEFAULT_BLOCK_SIZE_BYTES);
    }

    protected BlockDisk(String filepath, short blockSizeBytes) throws IOException {
        validateBlockSize(blockSizeBytes);
        this.filepath = filepath;
        RandomAccessFile raf = new RandomAccessFile(filepath, "rw");
        this.fileChannel = raf.getChannel();
        this.fileChannel.truncate(0);
        this.blockSizeBytes = blockSizeBytes;
    }

    public static void validateBlockSize(short blockSizeBytes) {
        if (blockSizeBytes < 1024 || blockSizeBytes > (31 << 10)) {
            throw new IllegalArgumentException("block size must be between 1024 and 31744");
        }

        // Ensure block size is power of 2
        if ((blockSizeBytes & (blockSizeBytes - 1)) != 0) {
            throw new IllegalArgumentException("block size: " + blockSizeBytes + " (expected: power of 2)");
        }
    }

    public abstract int[] write(T data) throws IOException;
    public abstract T read(int[] blocks) throws IOException;

    protected int[] allocateBlocks(int numBlocksNeeded) {
        assert numBlocksNeeded >= 1;

        int[] blocks = new int[numBlocksNeeded];
        // get them from the empty list or take the next one
        for (int i = 0; i < numBlocksNeeded; i++) {
            Integer emptyBlock = emptyBlocks.takeFirst();
            if (emptyBlock == null) {
                emptyBlock = numberOfBlocks.getAndIncrement();
            }
            blocks[i] = emptyBlock;
        }

        return blocks;
    }

    public void freeBlocks(int[] blocksToFree) {
        if (blocksToFree != null) {
            for (int aBlocksToFree : blocksToFree) {
                emptyBlocks.addLast(aBlocksToFree);
            }
        }
    }

    protected long calculateByteOffsetForBlock(int block) {
        return (long)block * (long)blockSizeBytes;
    }

    protected int calculateTheNumberOfBlocksNeeded(int numBytes) {
        int oneBlock = blockSizeBytes - HEADER_SIZE_BYTES;

        if (numBytes <= oneBlock) {
            return 1;
        }

        int divided = numBytes / oneBlock;
        if (numBytes % oneBlock != 0) {
            divided++;
        }

        return divided;
    }

    public long length() throws IOException {
        return fileChannel.size();
    }

    public void close() throws IOException {
        fileChannel.close();
    }

    /**
     * for test
     */
    public short getBlockSizeBytes() {
        return blockSizeBytes;
    }

    public int getEmptyBlocks() {
        return emptyBlocks.size();
    }

    public int getNumOfBlocks() {
        return numberOfBlocks.get();
    }

    /**
     * For debugging only.
     * @return String with details.
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("\nBlock Disk ");
        sb.append("\n  Filepath [" + filepath + "]");
        sb.append("\n  NumberOfBlocks [" + getNumOfBlocks() + "]");
        sb.append("\n  BlockSizeBytes [" + getBlockSizeBytes() + "]");
        sb.append("\n  Empty Blocks [" + getEmptyBlocks() + "]");
        try {
            sb.append("\n  Length [" + length() + "]");
        } catch ( IOException e ){
            // swallow
        }

        return sb.toString();
    }

    public static final class HeapBlockDisk extends BlockDisk<byte[]> {

        public HeapBlockDisk(String filepath) throws IOException {
            super(filepath);
        }

        public HeapBlockDisk(String filepath, short blockSizeBytes) throws IOException {
            super(filepath, blockSizeBytes);
        }

        @Override
        public int[] write(byte[] data) throws IOException {
            final int numBlocksNeeded = calculateTheNumberOfBlocksNeeded(data.length);
            final int[] blocks = allocateBlocks(numBlocksNeeded);

            // write data to each block
            final int maxChunkSize = blockSizeBytes - HEADER_SIZE_BYTES;
            int offset = 0;

            ByteBuffer headerBuffer = ByteBuffer.allocate(HEADER_SIZE_BYTES);
            for (int i = 0; i < numBlocksNeeded; i++) {
                headerBuffer.clear();
                int length = Math.min(maxChunkSize, data.length - offset);
                headerBuffer.putShort((short)length);
                ByteBuffer dataBuffer = ByteBuffer.wrap(data, offset, length);

                long position = calculateByteOffsetForBlock(blocks[i]);
                headerBuffer.flip();
                int written = fileChannel.write(headerBuffer, position);
                assert written == HEADER_SIZE_BYTES;

                written = fileChannel.write(dataBuffer, position + HEADER_SIZE_BYTES);
                assert written == length;

                offset += length;
            }
            fileChannel.force(false);

            return blocks;
        }

        @Override
        public byte[] read(int[] blocks) throws IOException {
            assert blocks != null && blocks.length >= 1;

            ByteBuffer blockBuffer = ByteBuffer.allocate(blockSizeBytes);
            int initialSize = blocks.length == 1 ? blockSizeBytes : blockSizeBytes * (blocks.length - 1);
            ByteArrayOutputStream baos = new ByteArrayOutputStream(initialSize);

            for (int i = 0; i < blocks.length; i++) {
                blockBuffer.clear();

                long position = calculateByteOffsetForBlock(blocks[i]);
                int nRead = fileChannel.read(blockBuffer, position);
                short length = blockBuffer.getShort(0);
                assert nRead == HEADER_SIZE_BYTES + length;

                baos.write(blockBuffer.array(), HEADER_SIZE_BYTES, length);
            }

            byte[] data = baos.toByteArray();
            baos.close();

            return data;
        }
    }

    public static final class DirectBlockDisk extends BlockDisk<ByteBuffer> {

        public DirectBlockDisk(String filepath) throws IOException {
            super(filepath);
        }

        public DirectBlockDisk(String filepath, short blockSizeBytes) throws IOException {
            super(filepath, blockSizeBytes);
        }

        @Override
        public int[] write(ByteBuffer data) throws IOException {
            final int totalBytes = data.remaining();
            final int numBlocksNeeded = calculateTheNumberOfBlocksNeeded(totalBytes);
            final int[] blocks = allocateBlocks(numBlocksNeeded);

            final int maxChunkSize = blockSizeBytes - HEADER_SIZE_BYTES;
            final int bufferLimit = data.limit();

            ByteBuffer headerBuffer = ByteBuffer.allocate(HEADER_SIZE_BYTES);
            for (int i = 0; i < numBlocksNeeded; i++) {
                int bufferPosition = data.position();
                headerBuffer.clear();

                int length = Math.min(maxChunkSize, bufferLimit - bufferPosition);
                headerBuffer.putShort((short)length);
                headerBuffer.flip();
                long position = calculateByteOffsetForBlock(blocks[i]);
                int written = fileChannel.write(headerBuffer, position);
                assert written == HEADER_SIZE_BYTES;

                data.limit(bufferPosition + length);
                written = fileChannel.write(data, position + HEADER_SIZE_BYTES);
                assert written == length;
            }
            fileChannel.force(false);

            return blocks;
        }

        @Override
        public ByteBuffer read(int[] blocks) throws IOException {
            assert blocks != null && blocks.length >= 1;

            int offset = 0;
            ByteBuffer dataBuffer = ByteBuffer.allocateDirect(blockSizeBytes * blocks.length);
            for (int i = 0; i < blocks.length; i++) {
                long position = calculateByteOffsetForBlock(blocks[i]);

                dataBuffer.limit(offset + HEADER_SIZE_BYTES);
                int nRead = fileChannel.read(dataBuffer, position);
                assert nRead == HEADER_SIZE_BYTES;
                short length = dataBuffer.getShort(offset);

                dataBuffer.position(offset);
                dataBuffer.limit(offset + length);
                nRead = fileChannel.read(dataBuffer, position + HEADER_SIZE_BYTES);
                assert nRead == length;

                offset += length;
            }
            dataBuffer.position(0);

            return dataBuffer;
        }
    }

}
