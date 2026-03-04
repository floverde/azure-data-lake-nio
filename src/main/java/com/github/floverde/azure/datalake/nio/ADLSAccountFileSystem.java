package com.github.floverde.azure.datalake.nio;

import com.azure.storage.file.datalake.DataLakeServiceClient;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;
import java.io.Closeable;
import java.util.Map;
import java.net.URI;

/**
 * Account-level file system for Azure Data Lake Storage Gen2.
 * <p>This file system represents an Azure Data Lake Storage Gen2 account and manages
 * container-level file systems ({@link ADLSContainerFileSystem}) as named sub-filesystems.
 * Container file systems are created lazily on first access and cached by container name
 * for efficient reuse.</p>
 * <p>Note that {@link #getPath(String, String...)} is not supported at the account level;
 * paths must be obtained from a container-level file system via
 * {@link #getContainerFileSystem(String)}.</p>
 *
 * @see ADLSContainerFileSystem
 * @see AzureDataLakeFileSystemProvider
 */
public final class ADLSAccountFileSystem extends AzureDataLakeFileSystem
{
    private final URI rootURI;
    private final AtomicBoolean open;
    private final DataLakeServiceClient serviceClient;
    private final Map<String, ADLSContainerFileSystem> containers;

    private static final String GET_PATH_UNSUPPORTED_MESSAGE = "getPath() is not supported on the " +
            "account-level file system. Use getPath() on the container-level file system instead.";

    /**
     * Creates a new account-level file system.
     *
     * @param provider      the provider that owns this file system; must not be {@code null}.
     * @param serviceClient the Azure Data Lake service client for the account; must not be {@code null}.
     * @param rootURI       the root URI for this account (scheme + authority); must not be {@code null}.
     */
    ADLSAccountFileSystem(final AzureDataLakeFileSystemProvider provider, final DataLakeServiceClient serviceClient, final URI rootURI) {
        super(provider);
        this.serviceClient = Objects.requireNonNull(serviceClient, "serviceClient must not be null");
        this.rootURI = Objects.requireNonNull(rootURI, "rootURI must not be null");
        this.open = new AtomicBoolean(true);
        this.containers = new ConcurrentHashMap<>();
    }

    private ADLSContainerFileSystem createContainerFileSystem(final String containerName) {
        return new ADLSContainerFileSystem(this, this.serviceClient.getFileSystemClient(containerName));
    }

    /**
     * Returns the container-level file system for the given container name,
     * creating it lazily if it does not already exist.
     *
     * @param containerName the name of the container; must not be {@code null}.
     * @return the {@link ADLSContainerFileSystem} for the given container.
     */
    public ADLSContainerFileSystem getContainerFileSystem(final String containerName) {
        return this.containers.computeIfAbsent(containerName, this::createContainerFileSystem);
    }

    private synchronized void closeContainers() throws IOException {
        for (final Closeable containerFs : this.containers.values()) {
            containerFs.close();
        }
        this.containers.clear();
    }

    /**
     * Closes this file system and all container-level file systems it manages.
     * <p>Removes this instance from the provider's cache. Subsequent calls to this method
     * have no effect.</p>
     *
     * @throws IOException if an I/O error occurs closing one of the container file systems.
     */
    @Override
    @SuppressWarnings("resource")
    public void close() throws IOException {
        if (this.open.compareAndSet(true, false)) {
            this.provider.fileSystems.remove(this.rootURI);
            this.closeContainers();
        }
    }

    /**
     * Throws {@link UnsupportedOperationException} because path creation is not supported
     * at the account level. Use {@link #getContainerFileSystem(String)} to obtain a
     * container-level file system and call {@link ADLSContainerFileSystem#getPath(String, String...)}
     * on it instead.
     *
     * @throws UnsupportedOperationException always.
     */
    @Override
    public Path getPath(final String first, final String... more) {
        throw new UnsupportedOperationException(ADLSAccountFileSystem.GET_PATH_UNSUPPORTED_MESSAGE);
    }

    /**
     * Returns the root directories of all container-level file systems currently managed
     * by this account file system.
     *
     * @return an iterable of root {@link Path} objects, one per open container.
     */
    @Override
    public Iterable<Path> getRootDirectories() {
        return this.containers.values().stream().map(ADLSContainerFileSystem::
                getRootDirectory).collect(Collectors.toList());
    }

    /**
     * Returns {@code true} if this file system is open.
     *
     * @return {@code true} if this file system has not been closed; {@code false} otherwise.
     */
    @Override
    public boolean isOpen() {
        return this.open.get();
    }

    /**
     * Returns the root URI for this account-level file system.
     * <p>The root URI has the form {@code abfss://account.dfs.core.windows.net}.</p>
     *
     * @return the root {@link URI} for this file system.
     */
    @Override
    public URI getRootURI() {
        return this.rootURI;
    }
}
