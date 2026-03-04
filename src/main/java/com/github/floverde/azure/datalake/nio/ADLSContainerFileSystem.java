package com.github.floverde.azure.datalake.nio;

import com.azure.storage.file.datalake.DataLakeDirectoryClient;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import io.netty.util.internal.StringUtil;
import java.util.Collections;
import java.util.Objects;
import java.nio.file.*;
import java.net.URI;

/**
 * Container-level file system for Azure Data Lake Storage Gen2.
 * <p>This file system represents a single storage container within an Azure Data Lake
 * Storage Gen2 account. It provides access to files and directories within the container
 * using the standard NIO.2 {@link java.nio.file.Path} API.</p>
 * <p>The root URI of a container file system has the form
 * {@code abfss://<container>@<account>.dfs.core.windows.net}.</p>
 *
 * @see ADLSAccountFileSystem
 * @see AzureDataLakeFileSystemProvider
 */
public final class ADLSContainerFileSystem extends AzureDataLakeFileSystem
{
    private final URI rootURI;
    private volatile boolean open;
    private final DataLakeFileSystemClient fileSystemClient;

    /**
     * Creates a new container-level file system.
     *
     * @param parent the parent account-level file system; must not be {@code null}.
     * @param client the Azure Data Lake file system client for the container; must not be {@code null}.
     */
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

    /**
     * Returns a singleton list containing the root directory path {@code "/"} of this container.
     *
     * @return an iterable containing the single root path {@code "/"}.
     */
    @Override
    public Iterable<Path> getRootDirectories() {
        return Collections.singletonList(this.getRootDirectory());
    }

    /**
     * Returns a path by joining the given path components with the {@code "/"} separator.
     *
     * @param first the first component of the path.
     * @param more  additional components to join.
     * @return an {@link AzureDataLakePath} for the given components.
     */
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

    /**
     * Returns the underlying Azure Data Lake file system client for this container.
     *
     * @return the {@link DataLakeFileSystemClient} for this container.
     */
    public DataLakeFileSystemClient getFileSystemClient() {
        return this.fileSystemClient;
    }

    /**
     * Returns the root directory path ({@code "/"}) of this container file system.
     *
     * @return an {@link AzureDataLakePath} representing the root directory.
     */
    public AzureDataLakePath getRootDirectory() {
        return new AzureDataLakePath(this, "/");
    }

    /**
     * Returns the root URI for this container-level file system.
     * <p>The root URI has the form
     * {@code abfss://<container>@<account>.dfs.core.windows.net}.</p>
     *
     * @return the root {@link URI} for this file system.
     */
    @Override
    public URI getRootURI() {
        return this.rootURI;
    }

    /**
     * Returns {@code true} if this file system is open.
     *
     * @return {@code true} if this file system has not been closed; {@code false} otherwise.
     */
    @Override
    public boolean isOpen() {
        return this.open;
    }

    /**
     * Closes this container-level file system.
     * <p>After this method returns, {@link #isOpen()} will return {@code false}.
     * Any further I/O operations on paths associated with this file system will throw
     * {@link java.nio.file.ClosedFileSystemException}.</p>
     */
    @Override
    public void close() {
        this.open = false;
    }
}
