package br.uff.spark.vfs;

import jnr.ffi.Pointer;
import ru.serce.jnrfuse.struct.FileStat;

import java.io.IOException;

/**
 * Based from: https://github.com/SerCeMan/jnr-fuse/blob/master/src/main/java/ru/serce/jnrfuse/examples/MemoryFS.java
 */
enum MemoryFileStatus {
    NEW, ALREADY_EXIST, MODIFIED
}

public class MemoryFile extends MemoryPath {

    protected MemoryFileStatus status = null;
    protected FileHeap contents;

    protected MemoryFile(MemoryFS memoryFS, String name, MemoryDirectory parent) {
        super(memoryFS, name, parent);
        contents = new FileHeap();
        this.status = MemoryFileStatus.NEW;
    }

    protected MemoryFile(MemoryFS memoryFS, String name, FileHeap data, MemoryDirectory parent, MemoryFileStatus status) throws IOException {
        super(memoryFS, name, parent);
        contents = data;
        this.status = status;
    }

    protected MemoryFile(MemoryFS memoryFS, String name, byte[] data, MemoryDirectory parent, MemoryFileStatus status) throws IOException {
        super(memoryFS, name, parent);
        contents = new FileHeap(data);
        this.status = status;
    }

    @Override
    protected void getattr(FileStat stat) {
        stat.st_mode.set(FileStat.S_IFREG | 0777);
        stat.st_size.set(contents.size);
        stat.st_uid.set(memoryFS.getContext().uid.get());
        stat.st_gid.set(memoryFS.getContext().pid.get());
    }

    protected int read(Pointer buffer, long size, long offset) {
        synchronized (this) {
            return contents.read(buffer, size, offset);
        }
    }

    @Override
    protected synchronized void delete() {
        super.delete();
        contents.freeAll();
    }

    protected synchronized void truncate(long size) {
        contents.truncate(size);
    }

    protected int write(Pointer buffer, long bufSize, long writeOffset) throws IOException {
        synchronized (this) {
            contents.write(buffer, bufSize, writeOffset);
            if (status == MemoryFileStatus.ALREADY_EXIST)
                status = MemoryFileStatus.MODIFIED;
            return (int) bufSize;
        }
    }

    public void trim() {
        truncate(contents.size);
    }
}