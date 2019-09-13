package br.uff.spark.vfs;

import com.google.common.primitives.Ints;

import java.io.IOException;
import java.io.InputStream;

import static com.google.common.base.Preconditions.checkPositionIndexes;

/**
 * @author Thaylon Guedes Santos
 * @email thaylongs@gmail.com
 *
 * Part of code of this class is copy/based from:
 * https://github.com/google/jimfs/blob/master/jimfs/src/main/java/com/google/common/jimfs/JimfsInputStream.java
 */
public class FileHeapInputStream extends InputStream {

    private FileHeap file;

    private long pos;

    private boolean finished;

    public FileHeapInputStream(FileHeap fileHeap) {
        this.file = fileHeap;
    }

    @Override
    public synchronized int read() throws IOException {
        checkNotClosed();
        if (finished) {
            return -1;
        }

        int b = file.read(pos++); // it's ok for pos to go beyond size()
        if (b == -1) {
            finished = true;
        }
        return b;
    }

    @Override
    public int read(byte[] b) throws IOException {
        return readInternal(b, 0, b.length);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        checkPositionIndexes(off, off + len, b.length);
        return readInternal(b, off, len);
    }

    private synchronized int readInternal(byte[] b, int off, int len) throws IOException {
        checkNotClosed();
        if (finished) {
            return -1;
        }

        int read = file.read(pos, b, off, len);
        if (read == -1) {
            finished = true;
        } else {
            pos += read;
        }
        return read;
    }

    @Override
    public long skip(long n) throws IOException {
        if (n <= 0) {
            return 0;
        }

        synchronized (this) {
            checkNotClosed();
            if (finished) {
                return 0;
            }

            // available() must be an int, so the min must be also
            int skip = (int) Math.min(Math.max(file.size - pos, 0), n);
            pos += skip;
            return skip;
        }
    }

    @Override
    public synchronized int available() throws IOException {
        checkNotClosed();
        if (finished) {
            return 0;
        }
        long available = Math.max(file.size - pos, 0);
        return Ints.saturatedCast(available);
    }


    private void checkNotClosed() throws IOException {
        if (file == null) {
            throw new IOException("stream is closed");
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if (isOpen()) {
            // file is set to null here and only here
            file = null;
        }
    }

    private boolean isOpen() {
        return file != null;
    }
}
