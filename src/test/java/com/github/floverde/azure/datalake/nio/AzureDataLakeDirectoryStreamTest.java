package com.github.floverde.azure.datalake.nio;

import com.azure.core.http.rest.PagedIterable;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.models.PathItem;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link AzureDataLakeDirectoryStream}.
 */
public final class AzureDataLakeDirectoryStreamTest
{
    private AzureDataLakePath dirPath;
    private DataLakeFileSystemClient mockFsClient;

    @BeforeEach
    @SuppressWarnings("unchecked")
    public void setUp() {
        final URI rootUri;
        final ADLSAccountFileSystem accountFs;
        final ADLSContainerFileSystem containerFs;
        final DataLakeServiceClient mockServiceClient;
        final AzureDataLakeFileSystemProvider provider;
        provider = new AzureDataLakeFileSystemProvider();
        mockServiceClient = mock(DataLakeServiceClient.class);
        rootUri = URI.create("abfss://account.dfs.core.windows.net");
        accountFs = new ADLSAccountFileSystem(provider, mockServiceClient, rootUri);
        mockFsClient = mock(DataLakeFileSystemClient.class);
        when(mockFsClient.getFileSystemName()).thenReturn("container");
        containerFs = new ADLSContainerFileSystem(accountFs, mockFsClient);
        dirPath = new AzureDataLakePath(containerFs, "/mydir");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testIteratorListsEntries() throws IOException {
        final PathItem item;
        final PagedIterable<PathItem> mockPagedIterable;
        final AzureDataLakeDirectoryStream stream;
        final Iterator<Path> it;
        final Path entry;
        item = mock(PathItem.class);
        when(item.getName()).thenReturn("mydir/file.txt");
        mockPagedIterable = mock(PagedIterable.class);
        when(mockPagedIterable.iterator()).thenReturn(Collections.singletonList(item).iterator());
        when(mockFsClient.listPaths(any(), any())).thenReturn(mockPagedIterable);
        stream = new AzureDataLakeDirectoryStream(dirPath, mockFsClient, null);
        it = stream.iterator();
        assertTrue(it.hasNext());
        entry = it.next();
        assertEquals("/mydir/file.txt", entry.toString());
        assertFalse(it.hasNext());
        stream.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testIteratorWithFilter() throws IOException {
        final PathItem item1;
        final PathItem item2;
        final PagedIterable<PathItem> mockPagedIterable;
        final AzureDataLakeDirectoryStream stream;
        final Iterator<Path> it;
        item1 = mock(PathItem.class);
        when(item1.getName()).thenReturn("mydir/keep.txt");
        item2 = mock(PathItem.class);
        when(item2.getName()).thenReturn("mydir/skip.log");
        mockPagedIterable = mock(PagedIterable.class);
        when(mockPagedIterable.iterator()).thenReturn(java.util.Arrays.asList(item1, item2).iterator());
        when(mockFsClient.listPaths(any(), any())).thenReturn(mockPagedIterable);
        stream = new AzureDataLakeDirectoryStream(dirPath, mockFsClient,
                p -> p.toString().endsWith(".txt"));
        it = stream.iterator();
        assertTrue(it.hasNext());
        assertEquals("/mydir/keep.txt", it.next().toString());
        assertFalse(it.hasNext());
        stream.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testIteratorThrowsAfterClose() throws IOException {
        final PagedIterable<PathItem> mockPagedIterable;
        final AzureDataLakeDirectoryStream stream;
        mockPagedIterable = mock(PagedIterable.class);
        when(mockFsClient.listPaths(any(), any())).thenReturn(mockPagedIterable);
        stream = new AzureDataLakeDirectoryStream(dirPath, mockFsClient, null);
        stream.close();
        assertThrows(IllegalStateException.class, stream::iterator);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testIteratorThrowsWhenCalledTwice() {
        final PagedIterable<PathItem> mockPagedIterable;
        final AzureDataLakeDirectoryStream stream;
        mockPagedIterable = mock(PagedIterable.class);
        when(mockPagedIterable.iterator()).thenReturn(Collections.emptyIterator());
        when(mockFsClient.listPaths(any(), any())).thenReturn(mockPagedIterable);
        stream = new AzureDataLakeDirectoryStream(dirPath, mockFsClient, null);
        stream.iterator();
        assertThrows(IllegalStateException.class, stream::iterator);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testEmptyDirectory() throws IOException {
        final PagedIterable<PathItem> mockPagedIterable;
        final AzureDataLakeDirectoryStream stream;
        final Iterator<Path> it;
        mockPagedIterable = mock(PagedIterable.class);
        when(mockPagedIterable.iterator()).thenReturn(Collections.emptyIterator());
        when(mockFsClient.listPaths(any(), any())).thenReturn(mockPagedIterable);
        stream = new AzureDataLakeDirectoryStream(dirPath, mockFsClient, null);
        it = stream.iterator();
        assertFalse(it.hasNext());
        stream.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testRootDirectoryPath() throws IOException {
        final PathItem item;
        final PagedIterable<PathItem> mockPagedIterable;
        final AzureDataLakeDirectoryStream stream;
        final AzureDataLakePath root;
        item = mock(PathItem.class);
        when(item.getName()).thenReturn("topfile.txt");
        mockPagedIterable = mock(PagedIterable.class);
        when(mockPagedIterable.iterator()).thenReturn(Collections.singletonList(item).iterator());
        when(mockFsClient.listPaths(any(), any())).thenReturn(mockPagedIterable);
        root = new AzureDataLakePath(dirPath.getFileSystem(), "/");
        stream = new AzureDataLakeDirectoryStream(root, mockFsClient, null);
        assertTrue(stream.iterator().hasNext());
        stream.close();
    }

    @Test
    public void testFilterImplementsDirectoryStreamFilter() {
        // Verify that DirectoryStream.Filter is correctly typed
        final DirectoryStream.Filter<? super Path> filter = p -> true;
        assertNotNull(filter);
    }
}
