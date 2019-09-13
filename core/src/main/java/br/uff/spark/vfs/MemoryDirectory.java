package br.uff.spark.vfs;

import jnr.ffi.Pointer;
import ru.serce.jnrfuse.FuseFillDir;
import ru.serce.jnrfuse.struct.FileStat;

import java.util.ArrayList;
import java.util.List;

/**
 * Based from: https://github.com/SerCeMan/jnr-fuse/blob/master/src/main/java/ru/serce/jnrfuse/examples/MemoryFS.java
 */
public class MemoryDirectory extends MemoryPath {

    protected List<MemoryPath> contents = new ArrayList<>();

    protected MemoryDirectory(MemoryFS memoryFS, String name) {
        super(memoryFS, name);
    }

    protected MemoryDirectory(MemoryFS memoryFS, String name, MemoryDirectory parent) {
        super(memoryFS, name, parent);
    }

    public synchronized void add(MemoryPath p) {
        contents.add(p);
        p.parent = this;
    }

    protected synchronized void deleteChild(MemoryPath child) {
        contents.remove(child);
    }

    @Override
    protected MemoryPath find(String path) {
        if (super.find(path) != null) {
            return super.find(path);
        }
        while (path.startsWith("/")) {
            path = path.substring(1);
        }
        synchronized (this) {
            if (!path.contains("/")) {
                for (MemoryPath p : contents) {
                    if (p.name.equals(path)) {
                        return p;
                    }
                }
                return null;
            }
            String nextName = path.substring(0, path.indexOf("/"));
            String rest = path.substring(path.indexOf("/"));
            for (MemoryPath p : contents) {
                if (p.name.equals(nextName)) {
                    return p.find(rest);
                }
            }
        }
        return null;
    }

    @Override
    protected void getattr(FileStat stat) {
        stat.st_mode.set(FileStat.S_IFDIR | 0777);
        stat.st_uid.set(memoryFS.getContext().uid.get());
        stat.st_gid.set(memoryFS.getContext().pid.get());
    }

    protected synchronized void mkdir(String lastComponent) {
        contents.add(new MemoryDirectory(memoryFS, lastComponent, this));
    }

    public synchronized void mkfile(String lastComponent) {
        contents.add(new MemoryFile(memoryFS, lastComponent, this));
    }

    public synchronized void read(Pointer buf, FuseFillDir filler) {
        for (MemoryPath p : contents) {
            filler.apply(buf, p.name, null, 0);
        }
    }

    @Override
    public String toString() {
        return "MemoryDirectory{" +
                "contents=" + contents +
                ", name='" + name + '\'' +
                '}';
    }
}