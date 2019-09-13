package br.uff.spark.vfs;


import br.uff.spark.advancedpipe.FileElement;
import br.uff.spark.advancedpipe.FileGroup;
import jnr.ffi.Platform;
import jnr.ffi.Pointer;
import jnr.ffi.types.mode_t;
import jnr.ffi.types.off_t;
import jnr.ffi.types.size_t;
import org.apache.log4j.Logger;
import ru.serce.jnrfuse.ErrorCodes;
import ru.serce.jnrfuse.FuseFillDir;
import ru.serce.jnrfuse.FuseStubFS;
import ru.serce.jnrfuse.struct.FileStat;
import ru.serce.jnrfuse.struct.FuseFileInfo;
import ru.serce.jnrfuse.struct.Statvfs;
import scala.Function1;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;

import static jnr.ffi.Platform.OS.WINDOWS;

/**
 * Based from: https://github.com/SerCeMan/jnr-fuse/blob/master/src/main/java/ru/serce/jnrfuse/examples/MemoryFS.java
 */
public class MemoryFS extends FuseStubFS {

    public static Logger log = org.apache.log4j.Logger.getLogger(MemoryFS.class);

    private MemoryDirectory rootDirectory = new MemoryDirectory(this, "");
    private FileGroup fileGroup;

    public MemoryFS(FileGroup fileGroup) {
        this.fileGroup = fileGroup;
    }

    public MemoryFS() {
        super();
    }

    @Override
    public void mount(Path mountPoint, boolean blocking, boolean debug, String[] fuseOpts) {
        if (fileGroup != null)
            try {
                loadFileSystem();
            } catch (IOException e) {
                log.error("Fail on mount disk", e);
                e.printStackTrace();
            }
        super.mount(mountPoint, blocking, debug, fuseOpts);
    }

    @Override
    public int flush(String path, FuseFileInfo fi) {
        return super.flush(path, fi);
    }

    private void loadFileSystem() throws IOException {
        for (FileElement fileElement : fileGroup.getFileElements()) {
            MemoryDirectory current = rootDirectory;
            if (!fileElement.getFilePath().isEmpty()) {
                for (Path folder : Paths.get(fileElement.getFilePath())) {
                    MemoryDirectory found = (MemoryDirectory) current.find(folder.toString());
                    if (found == null) {
                        MemoryDirectory newFolder = new MemoryDirectory(this, folder.toString(), current);
                        current.add(newFolder);
                        current = newFolder;
                    } else {
                        current = found;
                    }
                }
            }
            current.add(new MemoryFile(this, fileElement.getFileName(), fileElement.getContents().copy(), current, MemoryFileStatus.ALREADY_EXIST));
        }
    }

    @Override
    public int create(String path, @mode_t long mode, FuseFileInfo fi) {
        if (getPath(path) != null) {
            return -ErrorCodes.EEXIST();
        }
        MemoryPath parent = getParentPath(path);
        if (parent instanceof MemoryDirectory) {
            ((MemoryDirectory) parent).mkfile(getLastComponent(path));
            return 0;
        }
        return -ErrorCodes.ENOENT();
    }

    @Override
    public int getattr(String path, FileStat stat) {
        MemoryPath p = getPath(path);
        if (p != null) {
            p.getattr(stat);
            return 0;
        }
        return -ErrorCodes.ENOENT();
    }

    private String getLastComponent(String path) {
        while (path.substring(path.length() - 1).equals("/")) {
            path = path.substring(0, path.length() - 1);
        }
        if (path.isEmpty()) {
            return "";
        }
        return path.substring(path.lastIndexOf("/") + 1);
    }

    private MemoryPath getParentPath(String path) {
        return rootDirectory.find(path.substring(0, path.lastIndexOf("/")));
    }

    private MemoryPath getPath(String path) {
        return rootDirectory.find(path);
    }

    @Override
    public int mkdir(String path, @mode_t long mode) {
        if (getPath(path) != null) {
            return -ErrorCodes.EEXIST();
        }
        MemoryPath parent = getParentPath(path);
        if (parent instanceof MemoryDirectory) {
            ((MemoryDirectory) parent).mkdir(getLastComponent(path));
            return 0;
        }
        return -ErrorCodes.ENOENT();
    }


    @Override
    public int read(String path, Pointer buf, @size_t long size, @off_t long offset, FuseFileInfo fi) {
        MemoryPath p = getPath(path);
        if (p == null) {
            return -ErrorCodes.ENOENT();
        }
        if (!(p instanceof MemoryFile)) {
            return -ErrorCodes.EISDIR();
        }
        return ((MemoryFile) p).read(buf, size, offset);
    }

    @Override
    public int readdir(String path, Pointer buf, FuseFillDir filter, @off_t long offset, FuseFileInfo fi) {
        MemoryPath p = getPath(path);
        if (p == null) {
            return -ErrorCodes.ENOENT();
        }
        if (!(p instanceof MemoryDirectory)) {
            return -ErrorCodes.ENOTDIR();
        }
        filter.apply(buf, ".", null, 0);
        filter.apply(buf, "..", null, 0);
        ((MemoryDirectory) p).read(buf, filter);
        return 0;
    }


    @Override
    public int statfs(String path, Statvfs stbuf) {
        if (Platform.getNativePlatform().getOS() == WINDOWS) {
            // statfs needs to be implemented on Windows in order to allow for copying
            // data from other devices because winfsp calculates the volume size based
            // on the statvfs call.
            // see https://github.com/billziss-gh/winfsp/blob/14e6b402fe3360fdebcc78868de8df27622b565f/src/dll/fuse/fuse_intf.c#L654
            if ("/".equals(path)) {
                stbuf.f_blocks.set(1024 * 1024); // total data blocks in file system
                stbuf.f_frsize.set(1024);        // fs block size
                stbuf.f_bfree.set(1024 * 1024);  // free blocks in fs
            }
        }
        return super.statfs(path, stbuf);
    }

    @Override
    public int rename(String path, String newName) {
        MemoryPath p = getPath(path);
        if (p == null) {
            return -ErrorCodes.ENOENT();
        }
        MemoryPath newParent = getParentPath(newName);
        if (newParent == null) {
            return -ErrorCodes.ENOENT();
        }
        if (!(newParent instanceof MemoryDirectory)) {
            return -ErrorCodes.ENOTDIR();
        }
        p.delete();
        p.rename(newName.substring(newName.lastIndexOf("/")));
        ((MemoryDirectory) newParent).add(p);
        return 0;
    }

    @Override
    public int rmdir(String path) {
        MemoryPath p = getPath(path);
        if (p == null) {
            return -ErrorCodes.ENOENT();
        }
        if (!(p instanceof MemoryDirectory)) {
            return -ErrorCodes.ENOTDIR();
        }
        p.delete();
        return 0;
    }

    @Override
    public int truncate(String path, long offset) {
        MemoryPath p = getPath(path);
        if (p == null) {
            return -ErrorCodes.ENOENT();
        }
        if (!(p instanceof MemoryFile)) {
            return -ErrorCodes.EISDIR();
        }
        ((MemoryFile) p).truncate(offset);
        return 0;
    }

    @Override
    public int unlink(String path) {
        MemoryPath p = getPath(path);
        if (p == null) {
            return -ErrorCodes.ENOENT();
        }
        p.delete();
        return 0;
    }

    @Override
    public int open(String path, FuseFileInfo fi) {
        return 0;
    }

    @Override
    public int write(String path, Pointer buf, @size_t long size, @off_t long offset, FuseFileInfo fi) {
        MemoryPath p = getPath(path);
        if (p == null) {
            return -ErrorCodes.ENOENT();
        }
        if (!(p instanceof MemoryFile)) {
            return -ErrorCodes.EISDIR();
        }
        try {
            return ((MemoryFile) p).write(buf, size, offset);
        } catch (Exception e) {
            log.error("There isn't space for write this content, the max size for a File Element was Reached:  " + path, e);
            e.printStackTrace();
        }
        return -ErrorCodes.ENOSPC();
    }

    //TODO
    @Override
    public int chmod(String path, long mode) {
        return super.chmod(path, mode);
    }

    public List<FileElement> toFileElementList(Function1<FileElement, Boolean> filterFiles) {
        List<FileElement> result = new LinkedList<>();
        buildFileGroup(result, filterFiles, "", rootDirectory);
        return result;
    }

    //TODO Optimize it
    private static void buildFileGroup(List<FileElement> result, Function1<FileElement, Boolean> filterFiles, String currentPath, MemoryDirectory current) {
        currentPath = currentPath + "/" + current.name;
        for (MemoryPath content : current.contents) {
            if (content instanceof MemoryDirectory) {
                buildFileGroup(result, filterFiles, currentPath, (MemoryDirectory) content);
            } else {
                MemoryFile file = (MemoryFile) content;
                file.trim();
                FileElement fileElement = new FileElement(currentPath, file.name, file.contents);
                fileElement.setModified(file.status != MemoryFileStatus.ALREADY_EXIST);
                log.debug(fileElement);
                if (filterFiles.apply(fileElement))
                    result.add(fileElement);
            }
        }
    }

}
