package com.github.floverde.azure.datalake.nio;

import com.azure.storage.file.datalake.models.PathProperties;
import org.junit.jupiter.api.Test;

import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class AzureDataLakeFileAttributesTest {

    @Test
    void testFileAttributes() {
        PathProperties props = mock(PathProperties.class);
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
        OffsetDateTime created = now.minusDays(1);
        when(props.getLastModified()).thenReturn(now);
        when(props.getCreationTime()).thenReturn(created);
        when(props.getFileSize()).thenReturn(1024L);

        AzureDataLakeFileAttributes attrs = new AzureDataLakeFileAttributes(props, false);

        assertFalse(attrs.isDirectory());
        assertTrue(attrs.isRegularFile());
        assertFalse(attrs.isSymbolicLink());
        assertFalse(attrs.isOther());
        assertEquals(1024L, attrs.size());
        assertNull(attrs.fileKey());
        assertEquals(FileTime.from(now.toInstant()), attrs.lastModifiedTime());
        assertEquals(FileTime.from(created.toInstant()), attrs.creationTime());
        assertEquals(attrs.lastModifiedTime(), attrs.lastAccessTime());
    }

    @Test
    void testDirectoryAttributes() {
        PathProperties props = mock(PathProperties.class);
        when(props.getLastModified()).thenReturn(OffsetDateTime.now(ZoneOffset.UTC));
        when(props.getCreationTime()).thenReturn(OffsetDateTime.now(ZoneOffset.UTC));
        when(props.getFileSize()).thenReturn(0L);

        AzureDataLakeFileAttributes attrs = new AzureDataLakeFileAttributes(props, true);

        assertTrue(attrs.isDirectory());
        assertFalse(attrs.isRegularFile());
        assertEquals(0L, attrs.size());
    }

    @Test
    void testRootDirectoryAttributes() {
        AzureDataLakeFileAttributes attrs = new AzureDataLakeFileAttributes();
        assertTrue(attrs.isDirectory());
        assertFalse(attrs.isRegularFile());
        assertEquals(0L, attrs.size());
        assertEquals(FileTime.from(Instant.EPOCH), attrs.creationTime());
        assertEquals(FileTime.from(Instant.EPOCH), attrs.lastModifiedTime());
    }

    @Test
    void testNullDates() {
        PathProperties props = mock(PathProperties.class);
        when(props.getLastModified()).thenReturn(null);
        when(props.getCreationTime()).thenReturn(null);
        when(props.getFileSize()).thenReturn(0L);

        AzureDataLakeFileAttributes attrs = new AzureDataLakeFileAttributes(props, false);
        assertEquals(FileTime.from(Instant.EPOCH), attrs.lastModifiedTime());
        assertEquals(FileTime.from(Instant.EPOCH), attrs.creationTime());
        assertEquals(0L, attrs.size());
    }
}
