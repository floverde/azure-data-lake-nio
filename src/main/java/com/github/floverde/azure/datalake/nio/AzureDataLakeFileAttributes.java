package com.github.floverde.azure.datalake.nio;

import com.azure.storage.file.datalake.models.PathItem;
import com.azure.storage.file.datalake.models.PathProperties;

import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.time.OffsetDateTime;

/**
 * Implementation of {@link BasicFileAttributes} for Azure Data Lake Storage Gen2.
 * <p>File attributes can be constructed from:</p>
 * <ul>
 *   <li>{@link PathProperties} — for individual file or directory property lookups.</li>
 *   <li>{@link PathItem} — for entries returned by a directory listing.</li>
 *   <li>The no-arg constructor — for the root directory, which always exists.</li>
 * </ul>
 * <p>Azure Data Lake Storage Gen2 does not support symbolic links or other special
 * file types; {@link #isSymbolicLink()} and {@link #isOther()} always return
 * {@code false}.</p>
 * <p>Since ADLS Gen2 does not track last access time separately,
 * {@link #lastAccessTime()} returns the same value as {@link #lastModifiedTime()}.</p>
 * <p>If a creation or modification timestamp is {@code null} it defaults to
 * {@link java.time.Instant#EPOCH}.</p>
 *
 * @see BasicFileAttributes
 */
public class AzureDataLakeFileAttributes implements BasicFileAttributes
{
    private final long size;
    private final boolean directory;
    private final FileTime creationTime;
    private final FileTime lastModifiedTime;

    /**
     * Creates file attributes from a {@link PathProperties} object.
     *
     * @param props       the path properties returned by the Azure SDK; must not be {@code null}.
     * @param isDirectory {@code true} if the path represents a directory.
     */
    AzureDataLakeFileAttributes(PathProperties props, boolean isDirectory) {
        this.creationTime = toFileTime(props.getCreationTime());
        this.lastModifiedTime = toFileTime(props.getLastModified());
        this.directory = isDirectory;
        this.size = props.getFileSize();
    }

    /**
     * Creates file attributes from a {@link PathItem} returned by a directory listing.
     *
     * @param item the path item from the Azure SDK directory listing; must not be {@code null}.
     */
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
