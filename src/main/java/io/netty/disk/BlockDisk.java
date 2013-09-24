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
public class BlockDisk {

    /** The size of the header that indicates the amount of data stored in an occupied block. */
    private static final byte HEADER_SIZE_BYTES = 2;

    /** defaults to 4kb */
    private static final short DEFAULT_BLOCK_SIZE_BYTES = 4 << 10;

    /** Size of the block */
    private final short blockSizeBytes;

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
    private final FileChannel fileChannel;

    public BlockDisk(String filepath) throws FileNotFoundException {
        this(filepath, DEFAULT_BLOCK_SIZE_BYTES);
    }

    public BlockDisk(String filepath, short blockSizeBytes) throws FileNotFoundException {
        validateBlockSize(blockSizeBytes);
        this.filepath = filepath;
        RandomAccessFile raf = new RandomAccessFile(filepath, "rw");
        this.fileChannel = raf.getChannel();
        this.blockSizeBytes = blockSizeBytes;
    }

    public static void validateBlockSize(short blockSizeBytes) {
        if (blockSizeBytes < 1024 || blockSizeBytes > (31 << 10)) {
            throw new IllegalArgumentException("block size must be between 1024 and 31744");
        }

        // Ensure block size is power of 2
        for (short i = blockSizeBytes; i >= 2 ; i >>= 1) {
            if ((i & 1) != 0) {
                throw new IllegalArgumentException("block size: " + blockSizeBytes + " (expected: power of 2)");
            }
        }
    }

    public int[] write(byte[] data) throws IOException {
        // figure out how many blocks we need.
        int numBlocksNeeded = calculateTheNumberOfBlocksNeeded(data);

        int[] blocks = new int[numBlocksNeeded];
        // get them from the empty list or take the next one
        for (int i = 0; i < numBlocksNeeded; i++) {
            Integer emptyBlock = emptyBlocks.takeFirst();
            if (emptyBlock == null) {
                emptyBlock = numberOfBlocks.getAndIncrement();
            }
            blocks[i] = emptyBlock;
        }

        // get the individual sub arrays.
        byte[][] chunks = getBlockChunks(data, numBlocksNeeded);

        // write the blocks
        for (int i = 0; i < numBlocksNeeded; i++) {
            long position = calculateByteOffsetForBlock(blocks[i]);
            boolean success = write(position, chunks[i]);
            if (!success) {
                System.err.println("write failed?");
            }
        }

        return blocks;
    }

    private byte[][] getBlockChunks(byte[] complete, int numBlocksNeeded) {
        byte[][] chunks = new byte[numBlocksNeeded][];
        if (numBlocksNeeded == 1) {
            chunks[0] = complete;
        } else {
            int maxChunkSize = blockSizeBytes - HEADER_SIZE_BYTES;
            int totalBytes = complete.length;
            int totalUsed = 0;
            for (int i = 0; i < numBlocksNeeded; i++ ) {
                // use the max that can be written to a block or whatever is left in the original array
                int chunkSize = Math.min(maxChunkSize, totalBytes - totalUsed);
                byte[] chunk = new byte[chunkSize];
                // copy from the used position to the chunk size on the complete array to the chunk array.
                System.arraycopy(complete, totalUsed, chunk, 0, chunkSize);
                chunks[i] = chunk;
                totalUsed += chunkSize;
            }
        }

        return chunks;
    }

    private boolean write(long position, byte[] data) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(HEADER_SIZE_BYTES + data.length);
        buffer.putShort((short) data.length);
        buffer.put(data);
        buffer.flip();
        int written = fileChannel.write(buffer, position);
        fileChannel.force(false);

        return written == data.length + HEADER_SIZE_BYTES;
    }

    public byte[] read(int[] blockNumbers) throws IOException {
        byte[] data = null;

        if (blockNumbers.length == 1) {
            data = readBlock(blockNumbers[0]);
        } else {
            ByteArrayOutputStream baos = new ByteArrayOutputStream(blockSizeBytes);
            // get all the blocks into data
            for (int blockNumber : blockNumbers) {
                byte[] chunk = readBlock(blockNumber);
                baos.write(chunk);
            }

            data = baos.toByteArray();
            baos.close();
        }

        return data;
    }

    private byte[] readBlock(int block) throws IOException {
        short dataLength = 0;
        String message = null;
        boolean corrupted = false;
        long position = calculateByteOffsetForBlock(block);

        ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE_BYTES);
        fileChannel.read(header, position);
        header.flip();
        dataLength = header.getShort();

        if (position + dataLength + HEADER_SIZE_BYTES > length()) {
            corrupted = true;
            message = "Record " + position + " exceeds file length.";
        }

        if (corrupted) {
            System.err.println("\n The file is corrupt: " + "\n " + message);
            throw new IOException("The File Is Corrupt.");
        }

        ByteBuffer data = ByteBuffer.allocate(dataLength);
        fileChannel.read(data, position + HEADER_SIZE_BYTES);
        data.flip();

        return data.array();
    }

    public void freeBlocks(int[] blocksToFree) {
        if (blocksToFree != null) {
            for (int aBlocksToFree : blocksToFree) {
                emptyBlocks.addLast(aBlocksToFree);
            }
        }
    }

    private long calculateByteOffsetForBlock(int block) {
        return (long)block * (long)blockSizeBytes;
    }

    private int calculateTheNumberOfBlocksNeeded(byte[] data) {
        int dataLength = data.length;
        int oneBlock = blockSizeBytes - HEADER_SIZE_BYTES;

        if (dataLength <= oneBlock) {
            return 1;
        }

        int divided = dataLength / oneBlock;
        if (dataLength % oneBlock != 0) {
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

}
