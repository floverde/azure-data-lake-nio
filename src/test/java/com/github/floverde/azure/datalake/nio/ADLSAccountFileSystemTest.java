package com.github.floverde.azure.datalake.nio;

import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link ADLSAccountFileSystem}.
 */
public final class ADLSAccountFileSystemTest
{
    private AzureDataLakeFileSystemProvider provider;
    private DataLakeServiceClient mockServiceClient;
    private ADLSAccountFileSystem accountFs;
    private URI rootUri;

    @BeforeEach
    public void setUp() {
        provider = new AzureDataLakeFileSystemProvider();
        mockServiceClient = mock(DataLakeServiceClient.class);
        rootUri = URI.create("abfss://account.dfs.core.windows.net");
        accountFs = new ADLSAccountFileSystem(provider, mockServiceClient, rootUri);
        provider.fileSystems.put(rootUri, accountFs);
    }

    @Test
    public void testGetRootURI() {
        assertEquals(rootUri, accountFs.getRootURI());
    }

    @Test
    public void testIsOpenInitially() {
        assertTrue(accountFs.isOpen());
    }

    @Test
    public void testCloseMarksAsClosedAndRemovesFromProvider() throws IOException {
        assertTrue(provider.fileSystems.containsKey(rootUri));
        accountFs.close();
        assertFalse(accountFs.isOpen());
        assertFalse(provider.fileSystems.containsKey(rootUri));
    }

    @Test
    public void testCloseIsIdempotent() throws IOException {
        accountFs.close();
        assertFalse(accountFs.isOpen());
        // Second close should not throw
        accountFs.close();
        assertFalse(accountFs.isOpen());
    }

    @Test
    public void testGetPathThrowsUnsupportedOperation() {
        assertThrows(UnsupportedOperationException.class,
                () -> accountFs.getPath("/foo/bar"));
    }

    @Test
    public void testGetContainerFileSystemReturnsSameInstance() {
        final DataLakeFileSystemClient mockFsClient;
        final ADLSContainerFileSystem containerFs1;
        final ADLSContainerFileSystem containerFs2;
        mockFsClient = mock(DataLakeFileSystemClient.class);
        when(mockServiceClient.getFileSystemClient("mycontainer")).thenReturn(mockFsClient);
        when(mockFsClient.getFileSystemName()).thenReturn("mycontainer");
        containerFs1 = accountFs.getContainerFileSystem("mycontainer");
        containerFs2 = accountFs.getContainerFileSystem("mycontainer");
        assertNotNull(containerFs1);
        assertSame(containerFs1, containerFs2);
    }

    @Test
    public void testGetRootDirectoriesReflectsOpenedContainers() {
        final DataLakeFileSystemClient mockFsClient;
        final Iterable<Path> roots;
        mockFsClient = mock(DataLakeFileSystemClient.class);
        when(mockServiceClient.getFileSystemClient("c1")).thenReturn(mockFsClient);
        when(mockFsClient.getFileSystemName()).thenReturn("c1");
        // No containers opened yet
        assertFalse(accountFs.getRootDirectories().iterator().hasNext());
        accountFs.getContainerFileSystem("c1");
        roots = accountFs.getRootDirectories();
        assertTrue(roots.iterator().hasNext());
    }
}
