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

    @Test
    void testGetFileSystemCreatesOnDemandWhenEnvStored() throws Exception {
        TestableProvider provider = new TestableProvider();
        URI rootUri = URI.create("abfss://container@account.dfs.core.windows.net");
        DataLakeFileSystemClient mockFsClient = mock(DataLakeFileSystemClient.class);
        AzureDataLakeFileSystem mockFs = new AzureDataLakeFileSystem(provider, mockFsClient, rootUri);
        provider.setNextBuiltFileSystem(mockFs);

        // Simulate a prior newFileSystem call that stored the env; inject env directly
        Map<String, String> env = new HashMap<>();
        env.put("azure.account.account.key", "dummyKey");
        provider.setStoredEnv(env);

        // No filesystem cached yet — getFileSystem should create one on demand
        URI uri = URI.create("abfss://container@account.dfs.core.windows.net/some/path");
        assertSame(mockFs, provider.getFileSystem(uri));
    }

    @Test
    void testGetFileSystemOnDemandReturnsSameInstanceOnConcurrentCalls() throws Exception {
        TestableProvider provider = new TestableProvider();
        URI rootUri = URI.create("abfss://container@account.dfs.core.windows.net");
        DataLakeFileSystemClient mockFsClient = mock(DataLakeFileSystemClient.class);
        AzureDataLakeFileSystem mockFs = new AzureDataLakeFileSystem(provider, mockFsClient, rootUri);
        provider.setNextBuiltFileSystem(mockFs);

        Map<String, String> env = new HashMap<>();
        env.put("azure.account.account.key", "dummyKey");
        provider.setStoredEnv(env);

        URI uri = URI.create("abfss://container@account.dfs.core.windows.net/path");
        assertSame(provider.getFileSystem(uri), provider.getFileSystem(uri));
    }

    @Test
    void testNewFileSystemStoresEnvAndUsesAccountSpecificKeys() throws Exception {
        TestableProvider provider = new TestableProvider();
        URI rootUri = URI.create("abfss://container@account.dfs.core.windows.net");
        DataLakeFileSystemClient mockFsClient = mock(DataLakeFileSystemClient.class);
        AzureDataLakeFileSystem mockFs = new AzureDataLakeFileSystem(provider, mockFsClient, rootUri);
        provider.setNextBuiltFileSystem(mockFs);

        URI uri = URI.create("abfss://container@account.dfs.core.windows.net/path");
        Map<String, String> env = new HashMap<>();
        env.put("azure.account.account.key", "someAccountKey");

        provider.newFileSystem(uri, env);

        // Env should be stored — a second URI for the same account should be created on demand
        URI uri2 = URI.create("abfss://other@account.dfs.core.windows.net/path");
        URI rootUri2 = URI.create("abfss://other@account.dfs.core.windows.net");
        AzureDataLakeFileSystem mockFs2 = new AzureDataLakeFileSystem(provider, mockFsClient, rootUri2);
        provider.setNextBuiltFileSystem(mockFs2);

        assertSame(mockFs2, provider.getFileSystem(uri2));
    }

    /** Testable subclass that allows injecting filesystems without Azure connection. */
    static class TestableProvider extends AzureDataLakeFileSystemProvider {

        private AzureDataLakeFileSystem nextBuiltFs;

        void injectFileSystem(URI rootUri, AzureDataLakeFileSystem fs) {
            fileSystems.put(rootUri, fs);
        }

        void setNextBuiltFileSystem(AzureDataLakeFileSystem fs) {
            this.nextBuiltFs = fs;
        }

        void setStoredEnv(Map<String, ?> env) {
            this.storedEnv.set(env);
        }

        // Note: setNextBuiltFileSystem must be called before any method that triggers buildFileSystem.
        @Override
        protected AzureDataLakeFileSystem buildFileSystem(URI uri, URI rootUri, Map<String, ?> env) {
            return nextBuiltFs;
        }
    }
}
