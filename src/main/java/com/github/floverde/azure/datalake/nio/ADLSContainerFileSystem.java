package com.github.floverde.azure.datalake.nio;

import com.azure.storage.file.datalake.DataLakeDirectoryClient;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import io.netty.util.internal.StringUtil;
import java.util.Collections;
import java.util.Objects;
import java.nio.file.*;
import java.net.URI;

public final class ADLSContainerFileSystem extends AzureDataLakeFileSystem
{
    private final URI rootURI;
    private volatile boolean open;
    private final DataLakeFileSystemClient fileSystemClient;

    ADLSContainerFileSystem(final ADLSAccountFileSystem parent, final DataLakeFileSystemClient client) {
        super(parent.provider());
        this.fileSystemClient = Objects.requireNonNull(client, "fileSystemClient must not be null");
        this.rootURI = ADLSContainerFileSystem.toRootURI(parent.getRootURI(), client.getFileSystemName());
        this.open = true;
    }

    private static URI toRootURI(final URI uri, final String containerName) {
        try {
            return new URI(uri.getScheme(), containerName, uri.getHost(),
                    uri.getPort(), null, null, null);
        } catch (final Exception ex) {
            throw new IllegalArgumentException(String.format(
                    "Invalid URI: %s", uri), ex);
        }
    }

    @Override
    public Iterable<Path> getRootDirectories() {
        return Collections.singletonList(this.getRootDirectory());
    }

    @Override
    public Path getPath(final String first, final String... more) {
        final StringBuilder sb = new StringBuilder(first);
        for (final String s : more) {
            if (sb.length() > 0 && !StringUtil.endsWith(sb, '/') && !s.startsWith("/")) {
                sb.append('/');
            }
            sb.append(s);
        }
        return new AzureDataLakePath(this, sb.toString());
    }

    DataLakeDirectoryClient getDirectoryClient(final AzureDataLakePath path) {
        return this.fileSystemClient.getDirectoryClient(path.toAzurePathString());
    }

    DataLakeFileClient getFileClient(final AzureDataLakePath path) {
        return this.fileSystemClient.getFileClient(path.toAzurePathString());
    }

    public DataLakeFileSystemClient getFileSystemClient() {
        return this.fileSystemClient;
    }

    public AzureDataLakePath getRootDirectory() {
        return new AzureDataLakePath(this, "/");
    }

    @Override
    public URI getRootURI() {
        return this.rootURI;
    }

    @Override
    public boolean isOpen() {
        return this.open;
    }

    @Override
    public void close() {
        this.open = false;
    }
}
