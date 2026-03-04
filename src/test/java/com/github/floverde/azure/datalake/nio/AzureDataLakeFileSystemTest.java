package com.github.floverde.azure.datalake.nio;

import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.PathMatcher;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link AzureDataLakeFileSystem} via its concrete subclass
 * {@link ADLSContainerFileSystem}.
 */
public final class AzureDataLakeFileSystemTest
{
    private AzureDataLakeFileSystem fileSystem;

    @BeforeEach
    public void setUp() {
        final URI rootUri;
        final DataLakeServiceClient mockServiceClient;
        final DataLakeFileSystemClient mockFsClient;
        final ADLSAccountFileSystem accountFs;
        final AzureDataLakeFileSystemProvider provider;
        provider = new AzureDataLakeFileSystemProvider();
        mockServiceClient = mock(DataLakeServiceClient.class);
        rootUri = URI.create("abfss://account.dfs.core.windows.net");
        accountFs = new ADLSAccountFileSystem(provider, mockServiceClient, rootUri);
        mockFsClient = mock(DataLakeFileSystemClient.class);
        when(mockFsClient.getFileSystemName()).thenReturn("container");
        fileSystem = new ADLSContainerFileSystem(accountFs, mockFsClient);
    }

    @Test
    public void testGetSeparator() {
        assertEquals("/", fileSystem.getSeparator());
    }

    @Test
    public void testGetFileStores() {
        assertFalse(fileSystem.getFileStores().iterator().hasNext());
    }

    @Test
    public void testSupportedFileAttributeViews() {
        assertTrue(fileSystem.supportedFileAttributeViews().contains("basic"));
        assertEquals(1, fileSystem.supportedFileAttributeViews().size());
    }

    @Test
    public void testIsReadOnly() {
        assertFalse(fileSystem.isReadOnly());
    }

    @Test
    public void testGetPathMatcherGlob() {
        final PathMatcher matcher = fileSystem.getPathMatcher("glob:*.txt");
        assertTrue(matcher.matches(Path.of("report.txt")));
        assertFalse(matcher.matches(Path.of("report.java")));
    }

    @Test
    public void testGetPathMatcherGlobDoublestar() {
        final PathMatcher matcher = fileSystem.getPathMatcher("glob:**.txt");
        assertTrue(matcher.matches(Path.of("a/b/report.txt")));
        assertFalse(matcher.matches(Path.of("a/b/report.java")));
    }

    @Test
    public void testGetPathMatcherRegex() {
        final PathMatcher matcher = fileSystem.getPathMatcher("regex:.*\\.txt");
        assertTrue(matcher.matches(Path.of("report.txt")));
        assertFalse(matcher.matches(Path.of("report.java")));
    }

    @Test
    public void testGetPathMatcherInvalidSyntax() {
        assertThrows(IllegalArgumentException.class,
                () -> fileSystem.getPathMatcher("invalidsyntax"));
    }

    @Test
    public void testGetPathMatcherUnsupportedSyntax() {
        assertThrows(UnsupportedOperationException.class,
                () -> fileSystem.getPathMatcher("custom:pattern"));
    }

    @Test
    public void testGetUserPrincipalLookupService() {
        assertThrows(UnsupportedOperationException.class,
                () -> fileSystem.getUserPrincipalLookupService());
    }

    @Test
    public void testNewWatchService() {
        assertThrows(UnsupportedOperationException.class,
                () -> fileSystem.newWatchService());
    }

    @Test
    public void testProvider() {
        assertNotNull(fileSystem.provider());
        assertInstanceOf(AzureDataLakeFileSystemProvider.class, fileSystem.provider());
    }
}
