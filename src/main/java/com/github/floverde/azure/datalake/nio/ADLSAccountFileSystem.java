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

public final class ADLSAccountFileSystem extends AzureDataLakeFileSystem
{
    private final URI rootURI;
    private final AtomicBoolean open;
    private final DataLakeServiceClient serviceClient;
    private final Map<String, ADLSContainerFileSystem> containers;

    private static final String GET_PATH_UNSUPPORTED_MESSAGE = "getPath() is not supported on the " +
            "account-level file system. Use getPath() on the container-level file system instead.";

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

    public ADLSContainerFileSystem getContainerFileSystem(final String containerName) {
        return this.containers.computeIfAbsent(containerName, this::createContainerFileSystem);
    }

    private synchronized void closeContainers() throws IOException {
        for (final Closeable containerFs : this.containers.values()) {
            containerFs.close();
        }
        this.containers.clear();
    }

    @Override
    @SuppressWarnings("resource")
    public void close() throws IOException {
        if (this.open.compareAndSet(true, false)) {
            this.provider.fileSystems.remove(this.rootURI);
            this.closeContainers();
        }
    }

    @Override
    public Path getPath(final String first, final String... more) {
        throw new UnsupportedOperationException(ADLSAccountFileSystem.GET_PATH_UNSUPPORTED_MESSAGE);
    }

    @Override
    public Iterable<Path> getRootDirectories() {
        return this.containers.values().stream().map(ADLSContainerFileSystem::
                getRootDirectory).collect(Collectors.toList());
    }

    @Override
    public boolean isOpen() {
        return this.open.get();
    }

    @Override
    public URI getRootURI() {
        return this.rootURI;
    }
}
