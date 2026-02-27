package com.github.floverde.azure.datalake.nio;

import com.azure.storage.file.datalake.DataLakeServiceClient;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.nio.file.FileSystemAlreadyExistsException;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.Path;
import java.util.HashMap;

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
        // rootUri is account-level (no container)
        URI rootUri = URI.create("abfss://account.dfs.core.windows.net");
        DataLakeServiceClient mockServiceClient = mock(DataLakeServiceClient.class);
        testProvider.injectFileSystem(rootUri,
                new AzureDataLakeFileSystem(testProvider, mockServiceClient, rootUri));

        // Attempting to create filesystem for any container on the same account should fail
        URI uri = URI.create("abfss://container@account.dfs.core.windows.net/path");
        assertThrows(FileSystemAlreadyExistsException.class,
                () -> testProvider.newFileSystem(uri, new HashMap<>()));
    }

    @Test
    void testGetFileSystemReturnsExisting() {
        TestableProvider provider = new TestableProvider();
        // rootUri is account-level (no container)
        URI rootUri = URI.create("abfss://account.dfs.core.windows.net");
        DataLakeServiceClient mockServiceClient = mock(DataLakeServiceClient.class);
        AzureDataLakeFileSystem fs = new AzureDataLakeFileSystem(provider, mockServiceClient, rootUri);
        provider.injectFileSystem(rootUri, fs);

        // getFileSystem with a container URI resolves to the account-level filesystem
        URI uri = URI.create("abfss://container@account.dfs.core.windows.net/somepath");
        assertSame(fs, provider.getFileSystem(uri));
    }

    @Test
    void testGetPath() {
        TestableProvider provider = new TestableProvider();
        URI rootUri = URI.create("abfss://account.dfs.core.windows.net");
        DataLakeServiceClient mockServiceClient = mock(DataLakeServiceClient.class);
        AzureDataLakeFileSystem fs = new AzureDataLakeFileSystem(provider, mockServiceClient, rootUri);
        provider.injectFileSystem(rootUri, fs);

        URI uri = URI.create("abfss://container@account.dfs.core.windows.net/foo/bar");
        Path path = provider.getPath(uri);
        assertNotNull(path);
        assertEquals("/foo/bar", path.toString());
        assertEquals("container@account.dfs.core.windows.net",
                ((AzureDataLakePath) path).getAuthority());
    }

    @Test
    void testRemoveFileSystem() {
        TestableProvider provider = new TestableProvider();
        URI rootUri = URI.create("abfss://account.dfs.core.windows.net");
        DataLakeServiceClient mockServiceClient = mock(DataLakeServiceClient.class);
        AzureDataLakeFileSystem fs = new AzureDataLakeFileSystem(provider, mockServiceClient, rootUri);
        provider.injectFileSystem(rootUri, fs);

        assertSame(fs, provider.getFileSystem(rootUri));
        provider.removeFileSystem(rootUri);
        assertThrows(FileSystemNotFoundException.class, () -> provider.getFileSystem(rootUri));
    }

    @Test
    void testMultipleContainersOnSameAccount() {
        TestableProvider provider = new TestableProvider();
        URI rootUri = URI.create("abfss://account.dfs.core.windows.net");
        DataLakeServiceClient mockServiceClient = mock(DataLakeServiceClient.class);
        AzureDataLakeFileSystem fs = new AzureDataLakeFileSystem(provider, mockServiceClient, rootUri);
        provider.injectFileSystem(rootUri, fs);

        // Paths from different containers on the same account resolve to the same filesystem
        URI uri1 = URI.create("abfss://container1@account.dfs.core.windows.net/file1.txt");
        URI uri2 = URI.create("abfss://container2@account.dfs.core.windows.net/file2.txt");

        Path path1 = provider.getPath(uri1);
        Path path2 = provider.getPath(uri2);

        assertSame(fs, path1.getFileSystem());
        assertSame(fs, path2.getFileSystem());
        assertEquals("container1@account.dfs.core.windows.net",
                ((AzureDataLakePath) path1).getAuthority());
        assertEquals("container2@account.dfs.core.windows.net",
                ((AzureDataLakePath) path2).getAuthority());
        assertEquals("/file1.txt", path1.toString());
        assertEquals("/file2.txt", path2.toString());
    }

    /** Testable subclass that allows injecting filesystems without Azure connection. */
    static class TestableProvider extends AzureDataLakeFileSystemProvider {
        void injectFileSystem(URI rootUri, AzureDataLakeFileSystem fs) {
            fileSystems.put(rootUri, fs);
        }
    }
}
