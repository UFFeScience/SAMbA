package br.uff.spark.advancedpipe;

import br.uff.spark.vfs.FileHeap;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;

/**
 * @author Thaylon Guedes Santos
 * @email thaylongs@gmail.com
 */
public class FileElement implements Serializable {

    private String filePath;
    private String fileName;
    private boolean modified = false;
    private FileHeap contents = null;

    public FileElement(String filePath, String fileName, InputStream contents) throws IOException {
        if (fileName == null)
            throw new NullPointerException("The file name is  null");
        if (fileName.trim().isEmpty())
            throw new IllegalArgumentException("The file name can't be empty");
        this.fileName = fileName;
        this.filePath = filePath;
        this.contents = new FileHeap(contents);
    }

    public FileElement(String filePath, String fileName, byte[] content) {
        if (fileName == null)
            throw new NullPointerException("The file name is  null");
        if (fileName.trim().isEmpty())
            throw new IllegalArgumentException("The file name can't be empty");
        this.fileName = fileName;
        this.filePath = filePath;
        this.contents = new FileHeap(content);
    }


    public FileElement(String filePath, String fileName, FileHeap content) {
        if (fileName == null)
            throw new NullPointerException("The file name is  null");
        if (fileName.trim().isEmpty())
            throw new IllegalArgumentException("The file name can't be empty");
        this.fileName = fileName;
        this.filePath = filePath;
        this.contents = content;
    }

    @Override
    public String toString() {
        return "FileElement{" +
                "filePath='" + filePath + '\'' +
                ", fileName='" + fileName + '\'' +
                ", fileSize=" + contents.size +
                ", modified=" + modified +
                '}';
    }

    public FileElement copy() {
        return new FileElement(filePath, fileName, contents.copy());
    }

    public String getFileName() {
        return fileName;
    }

    public String getFilePath() {
        return filePath;
    }

    public long getFileSize() {
        return contents.size;
    }

    public FileHeap getContents() {
        return contents;
    }

    public boolean isModified() {
        return modified;
    }

    public void setModified(boolean modified) {
        this.modified = modified;
    }

}
