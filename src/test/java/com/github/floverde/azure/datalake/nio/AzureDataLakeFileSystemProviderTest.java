package com.github.floverde.azure.datalake.nio;

import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.nio.file.FileSystemAlreadyExistsException;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class AzureDataLakeFileSystemProviderTest {

    @Test
    void testGetScheme() {
        AzureDataLakeFileSystemProvider provider = new AzureDataLakeFileSystemProvider();
        assertEquals("abfss", provider.getScheme());
    }

    @Test
    void testGetFileSystemThrowsWhenNotFound() {
        AzureDataLakeFileSystemProvider provider = new AzureDataLakeFileSystemProvider();
        URI uri = URI.create("abfss://container@account.dfs.core.windows.net/path");
        assertThrows(FileSystemNotFoundException.class, () -> provider.getFileSystem(uri));
    }

    @Test
    void testFileSystemAlreadyExistsException() {
        TestableProvider testProvider = new TestableProvider();
        URI rootUri = URI.create("abfss://container@account.dfs.core.windows.net");
        DataLakeFileSystemClient mockFsClient = mock(DataLakeFileSystemClient.class);
        testProvider.injectFileSystem(rootUri,
                new AzureDataLakeFileSystem(testProvider, mockFsClient, rootUri));

        URI uri = URI.create("abfss://container@account.dfs.core.windows.net/path");
        assertThrows(FileSystemAlreadyExistsException.class,
                () -> testProvider.newFileSystem(uri, new HashMap<>()));
    }

    @Test
    void testGetFileSystemReturnsExisting() {
        TestableProvider provider = new TestableProvider();
        URI rootUri = URI.create("abfss://container@account.dfs.core.windows.net");
        DataLakeFileSystemClient mockFsClient = mock(DataLakeFileSystemClient.class);
        AzureDataLakeFileSystem fs = new AzureDataLakeFileSystem(provider, mockFsClient, rootUri);
        provider.injectFileSystem(rootUri, fs);

        URI uri = URI.create("abfss://container@account.dfs.core.windows.net/somepath");
        assertSame(fs, provider.getFileSystem(uri));
    }

    @Test
    void testGetPath() {
        TestableProvider provider = new TestableProvider();
        URI rootUri = URI.create("abfss://container@account.dfs.core.windows.net");
        DataLakeFileSystemClient mockFsClient = mock(DataLakeFileSystemClient.class);
        AzureDataLakeFileSystem fs = new AzureDataLakeFileSystem(provider, mockFsClient, rootUri);
        provider.injectFileSystem(rootUri, fs);

        URI uri = URI.create("abfss://container@account.dfs.core.windows.net/foo/bar");
        Path path = provider.getPath(uri);
        assertNotNull(path);
        assertEquals("/foo/bar", path.toString());
    }

    @Test
    void testRemoveFileSystem() {
        TestableProvider provider = new TestableProvider();
        URI rootUri = URI.create("abfss://container@account.dfs.core.windows.net");
        DataLakeFileSystemClient mockFsClient = mock(DataLakeFileSystemClient.class);
        AzureDataLakeFileSystem fs = new AzureDataLakeFileSystem(provider, mockFsClient, rootUri);
        provider.injectFileSystem(rootUri, fs);

        assertSame(fs, provider.getFileSystem(rootUri));
        provider.removeFileSystem(rootUri);
        assertThrows(FileSystemNotFoundException.class, () -> provider.getFileSystem(rootUri));
    }

    /** Testable subclass that allows injecting filesystems without Azure connection. */
    static class TestableProvider extends AzureDataLakeFileSystemProvider {
        void injectFileSystem(URI rootUri, AzureDataLakeFileSystem fs) {
            fileSystems.put(rootUri, fs);
        }
    }
}
