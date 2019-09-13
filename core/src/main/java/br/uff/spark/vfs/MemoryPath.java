package br.uff.spark.vfs;

import ru.serce.jnrfuse.struct.FileStat;

/**
 * Based from: https://github.com/SerCeMan/jnr-fuse/blob/master/src/main/java/ru/serce/jnrfuse/examples/MemoryFS.java
 */
public abstract class MemoryPath {

    protected MemoryFS memoryFS;
    protected String name;
    protected MemoryDirectory parent;

    protected MemoryPath(MemoryFS memoryFS, String name) {
        this(memoryFS, name, null);
    }

    protected MemoryPath(MemoryFS memoryFS, String name, MemoryDirectory parent) {
        this.memoryFS = memoryFS;
        this.name = name;
        this.parent = parent;
    }

    protected synchronized void delete() {
        if (parent != null) {
            parent.deleteChild(this);
            parent = null;
        }
    }

    protected MemoryPath find(String path) {
        while (path.startsWith("/")) {
            path = path.substring(1);
        }
        if (path.equals(name) || path.isEmpty()) {
            return this;
        }
        return null;
    }

    protected abstract void getattr(FileStat stat);

    protected void rename(String newName) {
        while (newName.startsWith("/")) {
            newName = newName.substring(1);
        }
        name = newName;
    }
}