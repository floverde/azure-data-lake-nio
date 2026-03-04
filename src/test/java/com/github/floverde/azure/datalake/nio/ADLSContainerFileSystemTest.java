package com.github.floverde.azure.datalake.nio;

import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.nio.file.Path;
import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link ADLSContainerFileSystem}.
 */
public final class ADLSContainerFileSystemTest
{
    private ADLSContainerFileSystem containerFs;
    private DataLakeFileSystemClient mockFsClient;

    @BeforeEach
    public void setUp() {
        final URI rootUri;
        final ADLSAccountFileSystem accountFs;
        final DataLakeServiceClient mockServiceClient;
        final AzureDataLakeFileSystemProvider provider;
        provider = new AzureDataLakeFileSystemProvider();
        mockServiceClient = mock(DataLakeServiceClient.class);
        rootUri = URI.create("abfss://account.dfs.core.windows.net");
        accountFs = new ADLSAccountFileSystem(provider, mockServiceClient, rootUri);
        mockFsClient = mock(DataLakeFileSystemClient.class);
        when(mockFsClient.getFileSystemName()).thenReturn("mycontainer");
        containerFs = new ADLSContainerFileSystem(accountFs, mockFsClient);
    }

    @Test
    public void testGetPathSingleComponent() {
        final Path path = containerFs.getPath("/foo/bar");
        assertEquals("/foo/bar", path.toString());
    }

    @Test
    public void testGetPathMultipleComponents() {
        final Path path = containerFs.getPath("/foo", "bar", "baz");
        assertEquals("/foo/bar/baz", path.toString());
    }

    @Test
    public void testGetRootDirectory() {
        final Path root = containerFs.getRootDirectory();
        assertNotNull(root);
        assertEquals("/", root.toString());
    }

    @Test
    public void testGetRootDirectoriesReturnsSingleRoot() {
        final Iterable<Path> roots = containerFs.getRootDirectories();
        final Iterator<Path> it = roots.iterator();
        assertTrue(it.hasNext());
        assertEquals("/", it.next().toString());
        assertFalse(it.hasNext());
    }

    @Test
    public void testGetRootURI() {
        final URI uri = containerFs.getRootURI();
        assertEquals("abfss", uri.getScheme());
        assertEquals("mycontainer", uri.getUserInfo());
        assertEquals("account.dfs.core.windows.net", uri.getHost());
    }

    @Test
    public void testIsOpenInitially() {
        assertTrue(containerFs.isOpen());
    }

    @Test
    public void testCloseMarksAsClosed() {
        assertTrue(containerFs.isOpen());
        containerFs.close();
        assertFalse(containerFs.isOpen());
    }

    @Test
    public void testGetFileSystemClient() {
        assertSame(mockFsClient, containerFs.getFileSystemClient());
    }
}
