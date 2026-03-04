package com.github.floverde.azure.datalake.nio;

import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.models.ListPathsOptions;
import com.azure.storage.file.datalake.models.PathItem;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A {@link DirectoryStream} implementation for Azure Data Lake Storage Gen2.
 * <p>Lists the entries of a directory in an ADLS Gen2 container using the
 * Azure SDK's {@link DataLakeFileSystemClient#listPaths} API.
 * Listing is non-recursive and results are filtered by the provided
 * {@link java.nio.file.DirectoryStream.Filter}.</p>
 * <p>This stream is lazy: entries are fetched from the Azure SDK on demand
 * as the iterator is advanced. Only one iterator may be obtained from a
 * single stream instance.</p>
 *
 * @see java.nio.file.Files#newDirectoryStream(java.nio.file.Path, DirectoryStream.Filter)
 */
public class AzureDataLakeDirectoryStream implements DirectoryStream<Path> {

    private final AzureDataLakePath dir;
    private final DataLakeFileSystemClient client;
    private final Filter<? super Path> filter;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicBoolean iteratorCreated = new AtomicBoolean(false);

    /**
     * Creates a new directory stream for the given directory path.
     *
     * @param dir    the directory path to list; must not be {@code null}.
     * @param client the Azure Data Lake file system client; must not be {@code null}.
     * @param filter an optional filter to restrict the entries returned; may be {@code null}.
     */
    AzureDataLakeDirectoryStream(AzureDataLakePath dir, DataLakeFileSystemClient client,
                                  Filter<? super Path> filter) {
        this.dir = dir;
        this.client = client;
        this.filter = filter;
    }

    /**
     * Returns a lazy iterator over the directory entries.
     *
     * @throws IllegalStateException if this stream has already been closed or
     *         if an iterator has already been obtained from this stream.
     * @return an iterator over the {@link Path} entries of the directory.
     */
    @Override
    public Iterator<Path> iterator() {
        if (closed.get()) {
            throw new IllegalStateException("DirectoryStream is closed");
        }
        if (!iteratorCreated.compareAndSet(false, true)) {
            throw new IllegalStateException("Iterator already obtained");
        }

        String prefix = dir.toAzurePathString();
        ListPathsOptions options = new ListPathsOptions().setRecursive(false);
        if (prefix != null && !prefix.isEmpty()) {
            options.setPath(prefix);
        }

        // Lazy iterator wrapping the SDK PagedIterable to avoid loading all entries into memory.
        Iterator<PathItem> source = client.listPaths(options, Duration.ofSeconds(30)).iterator();

        return new Iterator<>() {
            private Path next = null;
            private boolean done = false;

            @Override
            public boolean hasNext() {
                if (next != null) return true;
                if (done) return false;
                while (source.hasNext()) {
                    PathItem item = source.next();
                    AzureDataLakePath p = new AzureDataLakePath(dir.getFileSystem(), "/" + item.getName());
                    try {
                        if (filter == null || filter.accept(p)) {
                            next = p;
                            return true;
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
                done = true;
                return false;
            }

            @Override
            public Path next() {
                if (!hasNext()) throw new NoSuchElementException();
                Path result = next;
                next = null;
                return result;
            }
        };
    }

    /**
     * Closes this directory stream. After this method returns,
     * any further call to {@link #iterator()} will throw {@link IllegalStateException}.
     */
    @Override
    public void close() {
        closed.set(true);
    }
}
