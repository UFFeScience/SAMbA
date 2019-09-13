package br.uff.spark.vfs;

import jnr.ffi.Pointer;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * @author Thaylon Guedes Santos
 * @email thaylongs@gmail.com
 * <p>
 * Part of code of this class is copy/based from:
 * https://github.com/google/jimfs/blob/master/jimfs/src/main/java/com/google/common/jimfs/RegularFile.java
 * https://github.com/google/jimfs/blob/master/jimfs/src/main/java/com/google/common/jimfs/HeapDisk.java
 */
public class FileHeap {

    public static final int BLOCK_SIZE = 256 * 1024; // ~ 256 kb
    public static final int MAX_BLOCK_COUNT = 40_000; // ~ 10 gb
    private static final byte[] ZERO_ARRAY = new byte[BLOCK_SIZE];

    public long size = 0;
    private byte blocks[][] = null;
    private boolean[] changed;

    public FileHeap() {
        blocks = new byte[0][];
        changed = new boolean[0];
    }

    //TODO improve the performance of itF
    public FileHeap(InputStream inputStream) throws IOException {
        try {
            blocks = new byte[0][];
            changed = new boolean[0];
            byte[] block = new byte[8192];
            int len;
            while ((len = inputStream.read(block)) > 0) {
                write(size, block, 0, len);
            }
        } finally {
            inputStream.close();
        }
    }

    public FileHeap(byte[] contents) {
        int blocksCount = getNumsOfBlocksNeededForLength(contents.length);
        this.blocks = new byte[blocksCount][BLOCK_SIZE];
        this.changed = new boolean[blocksCount];
        Arrays.fill(changed, true);
        try {
            write(0, contents, 0, contents.length);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static int getNumsOfBlocksNeededForLength(long contentSize) {
        float blocksCount = contentSize / BLOCK_SIZE;
        if (blocksCount == 0 || blocksCount - Math.abs(blocksCount) != 0)
            return (int) (blocksCount + 1);
        return (int) blocksCount;
    }

    /**
     * Reads the byte at position {@code pos} in this file as an unsigned integer in the range 0-255.
     * If {@code pos} is greater than or equal to the size of this file, returns -1 instead.
     */
    public byte read(long pos) {
        if (pos >= size) {
            return -1;
        }

        byte[] block = blocks[blockIndex(pos)];
        int off = offsetInBlock(pos);
        return block[off];
    }

    /**
     * Reads up to {@code len} bytes starting at position {@code pos} in this file to the given byte
     * array starting at offset {@code off}. Returns the number of bytes actually read or -1 if {@code
     * pos} is greater than or equal to the size of this file.
     */
    public int read(long pos, byte[] b, int off, int len) {
        // since max is len (an int), result is guaranteed to be an int
        int bytesToRead = (int) bytesToRead(pos, len);

        if (bytesToRead > 0) {
            int remaining = bytesToRead;

            int blockIndex = blockIndex(pos);
            byte[] block = blocks[blockIndex];
            int offsetInBlock = offsetInBlock(pos);

            int read = get(block, offsetInBlock, b, off, length(offsetInBlock, remaining));
            remaining -= read;
            off += read;

            while (remaining > 0) {
                int index = ++blockIndex;
                block = blocks[index];

                read = get(block, 0, b, off, length(remaining));
                remaining -= read;
                off += read;
            }
        }

        return bytesToRead;
    }


    public int read(Pointer buffer, long size, long offset) {
        int bytesToRead = (int) bytesToRead(offset, size);
        if (bytesToRead > 0) {
            int remaining = bytesToRead;

            int blockIndex = blockIndex(offset);
            byte[] block = blocks[blockIndex];
            int offsetInBlock = offsetInBlock(offset);
            long off = 0;

            int read = get(block, offsetInBlock, buffer, off, length(offsetInBlock, remaining));
            remaining -= read;
            off += read;

            while (remaining > 0) {
                int index = ++blockIndex;
                block = blocks[index];
                read = get(block, 0, buffer, off, length(remaining));
                remaining -= read;
                off += read;
            }

        }
        return bytesToRead;
    }

    /**
     * Reads up to {@code buf.remaining()} bytes starting at position {@code pos} in this file to the
     * given buffer. Returns the number of bytes read or -1 if {@code pos} is greater than or equal to
     * the size of this file.
     */
    public int read(long pos, ByteBuffer buf) {
        // since max is buf.remaining() (an int), result is guaranteed to be an int
        int bytesToRead = (int) bytesToRead(pos, buf.remaining());

        if (bytesToRead > 0) {
            int remaining = bytesToRead;

            int blockIndex = blockIndex(pos);
            byte[] block = blocks[blockIndex];
            int off = offsetInBlock(pos);

            remaining -= get(block, off, buf, length(off, remaining));

            while (remaining > 0) {
                int index = ++blockIndex;
                block = blocks[index];
                remaining -= get(block, 0, buf, length(remaining));
            }
        }

        return bytesToRead;
    }

    /**
     * Reads up to the total {@code remaining()} number of bytes in each of {@code bufs} starting at
     * position {@code pos} in this file to the given buffers, in order. Returns the number of bytes
     * read or -1 if {@code pos} is greater than or equal to the size of this file.
     */
    public long read(long pos, Iterable<ByteBuffer> bufs) {
        if (pos >= size) {
            return -1;
        }

        long start = pos;
        for (ByteBuffer buf : bufs) {
            int read = read(pos, buf);
            if (read == -1) {
                break;
            } else {
                pos += read;
            }
        }

        return pos - start;
    }

    /**
     * Prepares for a write of len bytes starting at position pos.
     */
    private void prepareForWrite(long pos, long len) throws IOException {
        long end = pos + len;

        // allocate any additional blocks needed
        int lastBlockIndex = blocks.length - 1;
        int endBlockIndex = blockIndex(end - 1);

        if (lastBlockIndex < 0 || endBlockIndex > lastBlockIndex) {
            int additionalBlocksNeeded = endBlockIndex - lastBlockIndex;
            allocate(additionalBlocksNeeded);
        }

        // zero bytes between current size and pos
        if (pos > size) {
            long remaining = pos - size;

            int blockIndex = blockIndex(size);
            byte[] block = blocks[blockIndex];
            int off = offsetInBlock(size);

            remaining -= zero(block, off, length(off, remaining));

            while (remaining > 0) {
                block = blocks[++blockIndex];

                remaining -= zero(block, 0, length(remaining));
            }

            size = pos;
        }
    }

    /**
     * Allocates the given number of blocks and adds them to the given file.
     */
    public synchronized void allocate(int count) throws IOException {
        int oldSize = blocks.length;
        int newAllocatedBlockCount = blocks.length + count;
        if (newAllocatedBlockCount > MAX_BLOCK_COUNT) {
            throw new IOException("out of disk space");
        }
        this.blocks = Arrays.copyOf(blocks, (int) (newAllocatedBlockCount * 1.5));
        this.changed = Arrays.copyOf(changed, blocks.length);
        for (int i = oldSize; i < blocks.length; i++) {
            blocks[i] = new byte[BLOCK_SIZE];
            changed[i] = true;
        }
    }

    /**
     * Zeroes len bytes in the given block starting at the given offset. Returns len.
     */
    private static int zero(byte[] block, int offset, int len) {
        // this is significantly faster than looping or Arrays.fill (which loops), particularly when
        // the length of the slice to be zeroed is <= to ARRAY_LEN (in that case, it's faster by a
        // factor of 2)
        int remaining = len;
        while (remaining > BLOCK_SIZE) {
            System.arraycopy(ZERO_ARRAY, 0, block, offset, BLOCK_SIZE);
            offset += BLOCK_SIZE;
            remaining -= BLOCK_SIZE;
        }
        System.arraycopy(ZERO_ARRAY, 0, block, offset, remaining);
        return len;
    }

    /**
     * This function checks is the block is enable for write the new content.
     * If not, this function will overwrite the block with a mutable byte array
     * that contents the already content
     *
     * @param blockIndex
     */
    private void checkAndEnableBlockForWrite(int blockIndex) {
        if (!changed[blockIndex]) {
            blocks[blockIndex] = Arrays.copyOf(blocks[blockIndex], BLOCK_SIZE);
            changed[blockIndex] = true;
        }
    }

    /**
     * Writes the given byte to this file at position {@code pos}. {@code pos} may be greater than
     * the current size of this file, in which case this file is resized and all bytes between the
     * current size and {@code pos} are set to 0. Returns the number of bytes written.
     *
     * @throws IOException if the file needs more blocks but the disk is full
     */
    public int write(long pos, byte b) throws IOException {
        prepareForWrite(pos, 1);

        int blockIndex = blockIndex(pos);
        checkAndEnableBlockForWrite(blockIndex);
        byte[] block = blocks[blockIndex];
        int off = offsetInBlock(pos);
        block[off] = b;

        if (pos >= size) {
            size = pos + 1;
        }

        return 1;
    }

    /**
     * Writes {@code len} bytes starting at offset {@code off} in the given byte array to this file
     * starting at position {@code pos}. {@code pos} may be greater than the current size of this
     * file, in which case this file is resized and all bytes between the current size and {@code
     * pos} are set to 0. Returns the number of bytes written.
     *
     * @throws IOException if the file needs more blocks but the disk is full
     */
    public int write(long pos, byte[] b, int off, int len) throws IOException {
        prepareForWrite(pos, len);

        if (len == 0) {
            return 0;
        }

        int remaining = len;

        int blockIndex = blockIndex(pos);
        checkAndEnableBlockForWrite(blockIndex);

        byte[] block = blocks[blockIndex];
        int offInBlock = offsetInBlock(pos);

        int written = put(block, offInBlock, b, off, length(offInBlock, remaining));
        remaining -= written;
        off += written;

        while (remaining > 0) {
            int currentBlockIndex = ++blockIndex;
            block = blocks[currentBlockIndex];
            checkAndEnableBlockForWrite(currentBlockIndex);
            written = put(block, 0, b, off, length(remaining));
            remaining -= written;
            off += written;
        }

        long endPos = pos + len;
        if (endPos > size) {
            size = endPos;
        }

        return len;
    }

    public int write(Pointer buffer, long bufSize, long writeOffset) throws IOException {
        prepareForWrite(writeOffset, bufSize);
        if (bufSize == 0) {
            return 0;
        }

        int remaining = (int) bufSize;
        int blockIndex = blockIndex(writeOffset);
        checkAndEnableBlockForWrite(blockIndex);

        byte[] block = blocks[blockIndex];
        int offInBlock = offsetInBlock(writeOffset);

        int off = 0;//pointer buffer started in 0

        int written = put(block, offInBlock, buffer, off, length(offInBlock, remaining));
        remaining -= written;
        off += written;

        while (remaining > 0) {
            int currentBlockIndex = ++blockIndex;
            block = blocks[currentBlockIndex];
            checkAndEnableBlockForWrite(currentBlockIndex);
            written = put(block, 0, buffer, off, length(remaining));
            remaining -= written;
            off += written;
        }

        long endPos = writeOffset + bufSize;
        if (endPos > size) {
            size = endPos;
        }
        return (int) bufSize;
    }

    /**
     * Puts the contents of the given byte buffer at the given offset in the given block.
     */
    private static int put(byte[] block, int offset, ByteBuffer buf) {
        int len = Math.min(block.length - offset, buf.remaining());
        buf.get(block, offset, len);
        return len;
    }

    /**
     * Reads len bytes starting at the given offset in the given block into the given slice of the
     * given byte array.
     */
    private static int get(byte[] block, int offset, byte[] b, int off, int len) {
        System.arraycopy(block, offset, b, off, len);
        return len;
    }

    private int get(byte[] block, int offsetInBlock, Pointer buffer, long offset, int length) {
        buffer.put(offset, block, offsetInBlock, length);
        return length;
    }

    /**
     * Reads len bytes starting at the given offset in the given block into the given byte buffer.
     */
    private static int get(byte[] block, int offset, ByteBuffer buf, int len) {
        buf.put(block, offset, len);
        return len;
    }

    /**
     * Puts the given slice of the given array at the given offset in the given block.
     */
    private static int put(byte[] block, int offset, byte[] b, int off, int len) {
        System.arraycopy(b, off, block, offset, len);
        return len;
    }

    /**
     * Puts the given slice of the given pointer at the given offset in the given block.
     */
    private static int put(byte[] block, int offset, Pointer buffer, int off, int len) {
        buffer.get(off, block, offset, len);
        return len;
    }


    private int length(long max) {
        return (int) Math.min(BLOCK_SIZE, max);
    }

    private int blockIndex(long position) {
        return (int) (position / BLOCK_SIZE);
    }

    private int offsetInBlock(long position) {
        return (int) (position % BLOCK_SIZE);
    }

    private int length(int off, long max) {
        return (int) Math.min(BLOCK_SIZE - off, max);
    }

    /**
     * Returns the number of bytes that can be read starting at position {@code pos} (up to a maximum
     * of {@code max}) or -1 if {@code pos} is greater than or equal to the current size.
     */
    private long bytesToRead(long pos, long max) {
        long available = size - pos;
        if (available <= 0) {
            return -1;
        }
        return Math.min(available, max);
    }

    /**
     * Truncates this file to the given {@code size}. If the given size is less than the current size
     * of this file, the size of the file is reduced to the given size and any bytes beyond that
     * size are lost. If the given size is greater than the current size of the file, this method
     * does nothing. Returns {@code true} if this file was modified by the call (its size changed)
     * and {@code false} otherwise.
     */
    public boolean truncate(long size) {
        if (size >= this.size) {
            return false;
        }

        long lastPosition = size - 1;
        this.size = size;

        int newBlockCount = blockIndex(lastPosition) + 1;
        int blocksToRemove = blocks.length - newBlockCount;
        if (blocksToRemove > 0) {
            this.blocks = Arrays.copyOf(blocks, newBlockCount);
        }
        return true;
    }

    public void freeAll() {
        this.blocks = null;
        this.changed = null;
    }

    public FileHeapInputStream toInputStream() {
        return new FileHeapInputStream(this);
    }

    public FileHeap copy() {
        FileHeap result = new FileHeap();
        result.blocks = Arrays.copyOf(blocks, blocks.length);
        for (int i = 0; i < blocks.length; i++) {
            result.blocks[i] = blocks[i];
        }
        result.size = size;
        result.changed = new boolean[blocks.length];
        return result;
    }

}
