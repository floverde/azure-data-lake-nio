package com.github.floverde.azure.datalake.nio;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import java.nio.file.Path;
import java.util.Iterator;
import java.net.URI;

public final class AzureDataLakePathTest
{
    private ADLSContainerFileSystem mockFs;
    private AzureDataLakeFileSystemProvider mockProvider;

    @BeforeEach
    public void setUp() {
        final URI rootURI;
        this.mockFs = mock(ADLSContainerFileSystem.class);
        this.mockProvider = mock(AzureDataLakeFileSystemProvider.class);
        rootURI = URI.create("abfss://mycontainer@myaccount.dfs.core.windows.net");
        when(this.mockFs.provider()).thenReturn(this.mockProvider);
        when(this.mockFs.getSeparator()).thenReturn("/");
        when(this.mockFs.getRootURI()).thenReturn(rootURI);
    }

    @Test
    public void testAbsolutePath() {
        final AzureDataLakePath path;
        path = this.getPath("/foo/bar");
        assertTrue(path.isAbsolute());
        assertEquals("/foo/bar", path.toString());
    }

    @Test
    public void testRelativePath() {
        final AzureDataLakePath path;
        path = this.getPath("foo/bar");
        assertFalse(path.isAbsolute());
        assertEquals("foo/bar", path.toString());
    }

    @Test
    public void testRoot() {
        final Path root;
        final AzureDataLakePath rel;
        final AzureDataLakePath path;
        path = this.getPath("/foo/bar");
        root = path.getRoot();
        assertNotNull(root);
        assertEquals("/", root.toString());

        rel = this.getPath("foo/bar");
        assertNull(rel.getRoot());
    }

    @Test
    public void testGetFileName() {
        assertEquals("bar", this.getPath("/foo/bar").getFileName().toString());
        assertEquals("foo", this.getPath("foo").getFileName().toString());
        assertNull(this.getPath("/").getFileName());
    }

    @Test
    public void testGetParent() {
        assertEquals("/foo", this.getPath("/foo/bar").getParent().toString());
        assertEquals("/", this.getPath("/foo").getParent().toString());
        assertNull(this.getPath("/").getParent());
        assertNull(this.getPath("foo").getParent());
    }

    @Test
    public void testGetNameCount() {
        assertEquals(2, this.getPath("/foo/bar").getNameCount());
        assertEquals(0, this.getPath("/").getNameCount());
        assertEquals(1, this.getPath("foo").getNameCount());
    }

    @Test
    public void testGetName() {
        final AzureDataLakePath path = this.getPath("/foo/bar/baz");
        assertEquals("foo", path.getName(0).toString());
        assertEquals("bar", path.getName(1).toString());
        assertEquals("baz", path.getName(2).toString());
        assertThrows(IllegalArgumentException.class, () -> path.getName(3));
    }

    @Test
    public void testSubpath() {
        final AzureDataLakePath path = this.getPath("/foo/bar/baz");
        assertEquals("bar/baz", path.subpath(1, 3).toString());
        assertEquals("foo/bar", path.subpath(0, 2).toString());
    }

    @Test
    public void testStartsWith() {
        final AzureDataLakePath path = this.getPath("/foo/bar/baz");
        assertTrue(path.startsWith(this.getPath("/foo")));
        assertTrue(path.startsWith(this.getPath("/foo/bar")));
        assertTrue(path.startsWith(this.getPath("/")));
        assertFalse(path.startsWith(this.getPath("/bar")));
        assertFalse(path.startsWith(this.getPath("foo")));
    }

    @Test
    public void testEndsWith() {
        final AzureDataLakePath path = this.getPath("/foo/bar/baz");
        assertTrue(path.endsWith(this.getPath("baz")));
        assertTrue(path.endsWith(this.getPath("bar/baz")));
        assertFalse(path.endsWith(this.getPath("foo")));
    }

    @Test
    public void testNormalize() {
        assertEquals("/foo/baz", this.getPath("/foo/bar/../baz").normalize().toString());
        assertEquals("/foo/bar", this.getPath("/foo/./bar").normalize().toString());
        assertEquals("foo/baz", this.getPath("foo/bar/../baz").normalize().toString());
    }

    @Test
    public void testResolve() {
        final AzureDataLakePath base = this.getPath("/foo");
        assertEquals("/foo/bar", base.resolve("bar").toString());
        assertEquals("/bar", base.resolve("/bar").toString());
        assertEquals("/foo", base.resolve("").toString());
    }

    @Test
    public void testRelativize() {
        final AzureDataLakePath p1, p2, p3;
        p1 = this.getPath("/foo/bar");
        p2 = this.getPath("/foo/bar/baz");
        assertEquals("baz", p1.relativize(p2).toString());

        p3 = this.getPath("/foo");
        assertEquals("../bar", p3.relativize(this.getPath("/baz/../bar")).normalize().toString());
    }

    @Test
    public void testToAbsolutePath() {
        final AzureDataLakePath abs, rel = this.getPath("foo/bar");
        assertEquals("/foo/bar", rel.toAbsolutePath().toString());

        abs = this.getPath("/foo/bar");
        assertSame(abs, abs.toAbsolutePath());
    }

    @Test
    public void testToUri() {
        final URI uri;
        final AzureDataLakePath path;
        path = this.getPath("/foo/bar");
        uri = path.toUri();
        assertEquals("abfss", uri.getScheme());
        assertEquals("mycontainer@myaccount.dfs.core.windows.net", uri.getAuthority());
        assertEquals("/foo/bar", uri.getPath());
    }

    @Test
    public void testIterator() {
        final Iterator<Path> it;
        final AzureDataLakePath path;
        path = this.getPath("/foo/bar/baz");
        it = path.iterator();
        assertEquals("foo", it.next().toString());
        assertEquals("bar", it.next().toString());
        assertEquals("baz", it.next().toString());
        assertFalse(it.hasNext());
    }

    @Test
    public void testCompareTo() {
        final AzureDataLakePath a, b;
        a = this.getPath("/foo/bar");
        b = this.getPath("/foo/baz");
        assertTrue(a.compareTo(b) < 0);
        assertTrue(b.compareTo(a) > 0);
        assertEquals(0, a.compareTo(this.getPath("/foo/bar")));
    }

    @Test
    public void testEquals() {
        final AzureDataLakePath p1, p2, p3;
        p1 = this.getPath("/foo/bar");
        p2 = this.getPath("/foo/bar");
        p3 = this.getPath("/foo/baz");
        assertEquals(p1, p2);
        assertNotEquals(p1, p3);
    }

    @Test
    public void testHashCode() {
        final AzureDataLakePath p1, p2;
        p1 = this.getPath("/foo/bar");
        p2 = this.getPath("/foo/bar");
        assertEquals(p1.hashCode(), p2.hashCode());
    }

    @Test
    public void testToAzurePathString() {
        assertEquals("foo/bar", this.getPath("/foo/bar").toAzurePathString());
        assertEquals("foo/bar", this.getPath("foo/bar").toAzurePathString());
        assertEquals("", this.getPath("/").toAzurePathString());
    }

    private AzureDataLakePath getPath(final String path) {
        return new AzureDataLakePath(this.mockFs, path);
    }
}
