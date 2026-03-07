package com.github.floverde.azure.datalake.nio;

import org.mockito.Mockito;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.function.Executable;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import java.io.IOException;
import java.nio.file.*;
import java.util.Map;
import java.net.URI;

public final class AzureDataLakeFileSystemProviderTest extends AzureDataLakeFileSystemProvider
{
    @Test
    public void testGetScheme() {
        final AzureDataLakeFileSystemProvider provider;
        provider = new AzureDataLakeFileSystemProvider();
        assertEquals("abfss", provider.getScheme());
    }

    @Test
    public void testGetFileSystemThrowsWhenNotFound() {
        final URI uri;
        final AzureDataLakeFileSystemProvider provider;
        provider = new AzureDataLakeFileSystemProvider();
        uri = URI.create("abfss://container@account.dfs.core.windows.net/path");
        assertThrows(FileSystemNotFoundException.class, () -> provider.getFileSystem(uri));
    }

    @Test
    @SuppressWarnings("resource")
    public void testFileSystemAlreadyExistsException() {
        final URI rootUri, uri;
        final Executable executable;
        final ADLSAccountFileSystem accountFs;
        final DataLakeServiceClient mockServiceClient;
        final TestableProvider provider = new TestableProvider();
        rootUri = URI.create("abfss://account.dfs.core.windows.net");
        mockServiceClient = Mockito.mock(DataLakeServiceClient.class);
        uri = URI.create("abfss://container@account.dfs.core.windows.net/path");
        accountFs = new ADLSAccountFileSystem(provider, mockServiceClient, rootUri);
        executable = () -> provider.newFileSystem(uri, Map.of());
        provider.injectFileSystem(rootUri, accountFs);

        assertThrows(FileSystemAlreadyExistsException.class, executable);
    }

    @Test
    public void testGetFileSystemReturnsExisting() {
        final URI rootUri, uri;
        final ADLSAccountFileSystem accountFs;
        final DataLakeServiceClient mockServiceClient;
        final TestableProvider provider = new TestableProvider();
        rootUri = URI.create("abfss://account.dfs.core.windows.net");
        mockServiceClient = Mockito.mock(DataLakeServiceClient.class);
        uri = URI.create("abfss://account.dfs.core.windows.net/somepath");
        accountFs = new ADLSAccountFileSystem(provider, mockServiceClient, rootUri);
        provider.injectFileSystem(rootUri, accountFs);

        assertSame(accountFs, provider.getFileSystem(uri));
    }

    @Test
    public void testGetFileSystemForContainer() {
        final FileSystem fs;
        final ADLSAccountFileSystem accountFs;
        final ADLSContainerFileSystem containerFs;
        final DataLakeFileSystemClient mockFsClient;
        final DataLakeServiceClient mockServiceClient;
        final URI accountRootUri, containerRootUri, uri;
        final TestableProvider provider = new TestableProvider();
        mockFsClient = Mockito.mock(DataLakeFileSystemClient.class);
        mockServiceClient = Mockito.mock(DataLakeServiceClient.class);
        accountRootUri = URI.create("abfss://account.dfs.core.windows.net");
        uri = URI.create("abfss://container@account.dfs.core.windows.net/somepath");
        containerRootUri = URI.create("abfss://container@account.dfs.core.windows.net");
        Mockito.when(mockServiceClient.getFileSystemClient("container")).thenReturn(mockFsClient);
        accountFs = new ADLSAccountFileSystem(provider, mockServiceClient, accountRootUri);
        Mockito.when(mockFsClient.getFileSystemName()).thenReturn("container");
        provider.injectFileSystem(accountRootUri, accountFs);
        fs = provider.getFileSystem(uri);

        containerFs = assertInstanceOf(ADLSContainerFileSystem.class, fs);
        assertSame(mockFsClient, containerFs.getFileSystemClient());
        assertEquals(containerRootUri, containerFs.getRootURI());
    }

    @Test
    public void testGetPath() {
        final Path path;
        final URI rootUri, uri;
        final ADLSAccountFileSystem accountFs;
        final DataLakeFileSystemClient mockFsClient;
        final DataLakeServiceClient mockServiceClient;
        final TestableProvider provider = new TestableProvider();
        mockFsClient = Mockito.mock(DataLakeFileSystemClient.class);
        rootUri = URI.create("abfss://account.dfs.core.windows.net");
        mockServiceClient = Mockito.mock(DataLakeServiceClient.class);
        uri = URI.create("abfss://container@account.dfs.core.windows.net/foo/bar");
        Mockito.when(mockServiceClient.getFileSystemClient("container")).thenReturn(mockFsClient);
        accountFs = new ADLSAccountFileSystem(provider, mockServiceClient, rootUri);
        provider.injectFileSystem(rootUri, accountFs);
        path = provider.getPath(uri);

        assertNotNull(path);
        assertEquals("/foo/bar", path.toString());
    }

    @Test
    public void testGetPathWithoutSpecifyContainer() {
        final URI rootUri, uri;
        final ADLSAccountFileSystem accountFs;
        final IllegalArgumentException exception;
        final DataLakeFileSystemClient mockFsClient;
        final DataLakeServiceClient mockServiceClient;
        final TestableProvider provider = new TestableProvider();
        mockFsClient = Mockito.mock(DataLakeFileSystemClient.class);
        rootUri = URI.create("abfss://account.dfs.core.windows.net");
        mockServiceClient = Mockito.mock(DataLakeServiceClient.class);
        uri = URI.create("abfss://account.dfs.core.windows.net/foo/bar");
        Mockito.when(mockServiceClient.getFileSystemClient("container")).thenReturn(mockFsClient);
        accountFs = new ADLSAccountFileSystem(provider, mockServiceClient, rootUri);
        provider.injectFileSystem(rootUri, accountFs);

        exception = assertThrows(IllegalArgumentException.class, () -> provider.getPath(uri));
        assertTrue(exception.getMessage().startsWith("URI must specify the container name"));

    }

    @Test
    public void testRemoveFileSystem() throws IOException {
        final URI rootUri;
        final ADLSAccountFileSystem accountFs;
        final DataLakeServiceClient mockServiceClient;
        final TestableProvider provider = new TestableProvider();
        rootUri = URI.create("abfss://account.dfs.core.windows.net");
        mockServiceClient = Mockito.mock(DataLakeServiceClient.class);
        accountFs = new ADLSAccountFileSystem(provider, mockServiceClient, rootUri);
        provider.injectFileSystem(rootUri, accountFs);

        assertSame(accountFs, provider.getFileSystem(rootUri));
        provider.fileSystems.remove(rootUri).close();
        assertThrows(FileSystemNotFoundException.class, () -> provider.getFileSystem(rootUri));
    }

    /** Testable subclass that allows injecting filesystems without Azure connection. */
    private static final class TestableProvider extends AzureDataLakeFileSystemProvider {
        public void injectFileSystem(URI rootUri, ADLSAccountFileSystem fs) {
            this.fileSystems.put(rootUri, fs);
        }
    }
}
