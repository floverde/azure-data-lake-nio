package com.github.floverde.azure.datalake.nio;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.nio.file.Path;
import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class AzureDataLakePathTest {

    private AzureDataLakeFileSystem mockFs;
    private AzureDataLakeFileSystemProvider mockProvider;

    @BeforeEach
    void setUp() {
        mockFs = mock(AzureDataLakeFileSystem.class);
        mockProvider = mock(AzureDataLakeFileSystemProvider.class);
        when(mockFs.provider()).thenReturn(mockProvider);
        when(mockFs.getSeparator()).thenReturn("/");
        when(mockFs.getRootUri()).thenReturn(
                URI.create("abfss://mycontainer@myaccount.dfs.core.windows.net"));
    }

    @Test
    void testAbsolutePath() {
        AzureDataLakePath p = new AzureDataLakePath(mockFs, "/foo/bar");
        assertTrue(p.isAbsolute());
        assertEquals("/foo/bar", p.toString());
    }

    @Test
    void testRelativePath() {
        AzureDataLakePath p = new AzureDataLakePath(mockFs, "foo/bar");
        assertFalse(p.isAbsolute());
        assertEquals("foo/bar", p.toString());
    }

    @Test
    void testRoot() {
        AzureDataLakePath p = new AzureDataLakePath(mockFs, "/foo/bar");
        Path root = p.getRoot();
        assertNotNull(root);
        assertEquals("/", root.toString());

        AzureDataLakePath rel = new AzureDataLakePath(mockFs, "foo/bar");
        assertNull(rel.getRoot());
    }

    @Test
    void testGetFileName() {
        assertEquals("bar", new AzureDataLakePath(mockFs, "/foo/bar").getFileName().toString());
        assertEquals("foo", new AzureDataLakePath(mockFs, "foo").getFileName().toString());
        assertNull(new AzureDataLakePath(mockFs, "/").getFileName());
    }

    @Test
    void testGetParent() {
        assertEquals("/foo", new AzureDataLakePath(mockFs, "/foo/bar").getParent().toString());
        assertEquals("/", new AzureDataLakePath(mockFs, "/foo").getParent().toString());
        assertNull(new AzureDataLakePath(mockFs, "/").getParent());
        assertNull(new AzureDataLakePath(mockFs, "foo").getParent());
    }

    @Test
    void testGetNameCount() {
        assertEquals(2, new AzureDataLakePath(mockFs, "/foo/bar").getNameCount());
        assertEquals(0, new AzureDataLakePath(mockFs, "/").getNameCount());
        assertEquals(1, new AzureDataLakePath(mockFs, "foo").getNameCount());
    }

    @Test
    void testGetName() {
        AzureDataLakePath p = new AzureDataLakePath(mockFs, "/foo/bar/baz");
        assertEquals("foo", p.getName(0).toString());
        assertEquals("bar", p.getName(1).toString());
        assertEquals("baz", p.getName(2).toString());
        assertThrows(IllegalArgumentException.class, () -> p.getName(3));
    }

    @Test
    void testSubpath() {
        AzureDataLakePath p = new AzureDataLakePath(mockFs, "/foo/bar/baz");
        assertEquals("bar/baz", p.subpath(1, 3).toString());
        assertEquals("foo/bar", p.subpath(0, 2).toString());
    }

    @Test
    void testStartsWith() {
        AzureDataLakePath p = new AzureDataLakePath(mockFs, "/foo/bar/baz");
        assertTrue(p.startsWith(new AzureDataLakePath(mockFs, "/foo")));
        assertTrue(p.startsWith(new AzureDataLakePath(mockFs, "/foo/bar")));
        assertTrue(p.startsWith(new AzureDataLakePath(mockFs, "/")));
        assertFalse(p.startsWith(new AzureDataLakePath(mockFs, "/bar")));
        assertFalse(p.startsWith(new AzureDataLakePath(mockFs, "foo")));
    }

    @Test
    void testEndsWith() {
        AzureDataLakePath p = new AzureDataLakePath(mockFs, "/foo/bar/baz");
        assertTrue(p.endsWith(new AzureDataLakePath(mockFs, "baz")));
        assertTrue(p.endsWith(new AzureDataLakePath(mockFs, "bar/baz")));
        assertFalse(p.endsWith(new AzureDataLakePath(mockFs, "foo")));
    }

    @Test
    void testNormalize() {
        assertEquals("/foo/baz",
                new AzureDataLakePath(mockFs, "/foo/bar/../baz").normalize().toString());
        assertEquals("/foo/bar",
                new AzureDataLakePath(mockFs, "/foo/./bar").normalize().toString());
        assertEquals("foo/baz",
                new AzureDataLakePath(mockFs, "foo/bar/../baz").normalize().toString());
    }

    @Test
    void testResolve() {
        AzureDataLakePath base = new AzureDataLakePath(mockFs, "/foo");
        assertEquals("/foo/bar", base.resolve("bar").toString());
        assertEquals("/bar", base.resolve("/bar").toString());
        assertEquals("/foo", base.resolve("").toString());
    }

    @Test
    void testRelativize() {
        AzureDataLakePath p1 = new AzureDataLakePath(mockFs, "/foo/bar");
        AzureDataLakePath p2 = new AzureDataLakePath(mockFs, "/foo/bar/baz");
        assertEquals("baz", p1.relativize(p2).toString());

        AzureDataLakePath p3 = new AzureDataLakePath(mockFs, "/foo");
        assertEquals("../bar",
                p3.relativize(new AzureDataLakePath(mockFs, "/baz/../bar")).normalize().toString());
    }

    @Test
    void testToAbsolutePath() {
        AzureDataLakePath rel = new AzureDataLakePath(mockFs, "foo/bar");
        assertEquals("/foo/bar", rel.toAbsolutePath().toString());

        AzureDataLakePath abs = new AzureDataLakePath(mockFs, "/foo/bar");
        assertSame(abs, abs.toAbsolutePath());
    }

    @Test
    void testToUri() {
        AzureDataLakePath p = new AzureDataLakePath(mockFs, "/foo/bar");
        URI uri = p.toUri();
        assertEquals("abfss", uri.getScheme());
        assertEquals("mycontainer@myaccount.dfs.core.windows.net", uri.getAuthority());
        assertEquals("/foo/bar", uri.getPath());
    }

    @Test
    void testIterator() {
        AzureDataLakePath p = new AzureDataLakePath(mockFs, "/foo/bar/baz");
        Iterator<Path> it = p.iterator();
        assertEquals("foo", it.next().toString());
        assertEquals("bar", it.next().toString());
        assertEquals("baz", it.next().toString());
        assertFalse(it.hasNext());
    }

    @Test
    void testCompareTo() {
        AzureDataLakePath a = new AzureDataLakePath(mockFs, "/foo/bar");
        AzureDataLakePath b = new AzureDataLakePath(mockFs, "/foo/baz");
        assertTrue(a.compareTo(b) < 0);
        assertTrue(b.compareTo(a) > 0);
        assertEquals(0, a.compareTo(new AzureDataLakePath(mockFs, "/foo/bar")));
    }

    @Test
    void testEquals() {
        AzureDataLakePath p1 = new AzureDataLakePath(mockFs, "/foo/bar");
        AzureDataLakePath p2 = new AzureDataLakePath(mockFs, "/foo/bar");
        AzureDataLakePath p3 = new AzureDataLakePath(mockFs, "/foo/baz");
        assertEquals(p1, p2);
        assertNotEquals(p1, p3);
    }

    @Test
    void testHashCode() {
        AzureDataLakePath p1 = new AzureDataLakePath(mockFs, "/foo/bar");
        AzureDataLakePath p2 = new AzureDataLakePath(mockFs, "/foo/bar");
        assertEquals(p1.hashCode(), p2.hashCode());
    }

    @Test
    void testToAzurePathString() {
        assertEquals("foo/bar", new AzureDataLakePath(mockFs, "/foo/bar").toAzurePathString());
        assertEquals("foo/bar", new AzureDataLakePath(mockFs, "foo/bar").toAzurePathString());
        assertEquals("", new AzureDataLakePath(mockFs, "/").toAzurePathString());
    }
}
