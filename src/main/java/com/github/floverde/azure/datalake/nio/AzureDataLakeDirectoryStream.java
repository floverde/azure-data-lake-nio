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

public class AzureDataLakeDirectoryStream implements DirectoryStream<Path> {

    private final AzureDataLakePath dir;
    private final DataLakeFileSystemClient client;
    private final Filter<? super Path> filter;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicBoolean iteratorCreated = new AtomicBoolean(false);

    AzureDataLakeDirectoryStream(AzureDataLakePath dir, DataLakeFileSystemClient client,
                                  Filter<? super Path> filter) {
        this.dir = dir;
        this.client = client;
        this.filter = filter;
    }

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

        return new Iterator<Path>() {
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

    @Override
    public void close() {
        closed.set(true);
    }
}
