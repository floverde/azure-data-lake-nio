package com.github.floverde.azure.datalake.nio;

import com.azure.storage.file.datalake.models.PathItem;
import com.azure.storage.file.datalake.models.PathProperties;

import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.time.OffsetDateTime;

public class AzureDataLakeFileAttributes implements BasicFileAttributes
{
    private final long size;
    private final boolean directory;
    private final FileTime creationTime;
    private final FileTime lastModifiedTime;

    AzureDataLakeFileAttributes(PathProperties props, boolean isDirectory) {
        this.creationTime = toFileTime(props.getCreationTime());
        this.lastModifiedTime = toFileTime(props.getLastModified());
        this.directory = isDirectory;
        this.size = props.getFileSize();
    }

    AzureDataLakeFileAttributes(PathItem item) {
        this.creationTime = toFileTime(item.getCreationTime());
        this.lastModifiedTime = toFileTime(item.getLastModified());
        this.directory = item.isDirectory();
        this.size = item.getContentLength();
    }

    /** Constructor for root directory. */
    AzureDataLakeFileAttributes() {
        this.creationTime = FileTime.from(Instant.EPOCH);
        this.lastModifiedTime = FileTime.from(Instant.EPOCH);
        this.directory = true;
        this.size = 0L;
    }

    private static FileTime toFileTime(OffsetDateTime odt) {
        if (odt == null) {
            return FileTime.from(Instant.EPOCH);
        }
        return FileTime.from(odt.toInstant());
    }

    @Override
    public FileTime lastModifiedTime() {
        return lastModifiedTime;
    }

    @Override
    public FileTime lastAccessTime() {
        return lastModifiedTime;
    }

    @Override
    public FileTime creationTime() {
        return creationTime;
    }

    @Override
    public boolean isRegularFile() {
        return !directory;
    }

    @Override
    public boolean isDirectory() {
        return directory;
    }

    @Override
    public boolean isSymbolicLink() {
        return false;
    }

    @Override
    public boolean isOther() {
        return false;
    }

    @Override
    public Object fileKey() {
        return null;
    }

    @Override
    public long size() {
        return size;
    }
}
