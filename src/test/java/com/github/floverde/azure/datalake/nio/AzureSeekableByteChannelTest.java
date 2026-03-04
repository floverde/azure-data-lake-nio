package com.github.floverde.azure.datalake.nio;

import com.azure.storage.blob.specialized.BlobInputStream;
import com.azure.storage.blob.models.BlobProperties;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.NonReadableChannelException;
import java.nio.channels.NonWritableChannelException;
import java.nio.file.ClosedFileSystemException;
import java.nio.file.FileSystem;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link AzureSeekableByteChannel}.
 */
public final class AzureSeekableByteChannelTest
{
    /** Creates a mock Path backed by an open file system. */
    private static Path openPath() {
        final Path mockPath = mock(Path.class);
        final FileSystem mockFs = mock(FileSystem.class);
        when(mockPath.getFileSystem()).thenReturn(mockFs);
        when(mockFs.isOpen()).thenReturn(true);
        return mockPath;
    }

    /** Creates a mock Path backed by a closed file system. */
    private static Path closedFsPath() {
        final Path mockPath = mock(Path.class);
        final FileSystem mockFs = mock(FileSystem.class);
        when(mockPath.getFileSystem()).thenReturn(mockFs);
        when(mockFs.isOpen()).thenReturn(false);
        return mockPath;
    }

    // -----------------------------------------------------------------------
    // truncate
    // -----------------------------------------------------------------------

    @Test
    public void testTruncateAlwaysThrows() throws IOException {
        final Path path = openPath();
        final OutputStream mockWriter = mock(OutputStream.class);
        final AzureSeekableByteChannel channel = new AzureSeekableByteChannel(mockWriter, path);
        assertThrows(UnsupportedOperationException.class, () -> channel.truncate(0L));
    }

    // -----------------------------------------------------------------------
    // isOpen / close (write channel)
    // -----------------------------------------------------------------------

    @Test
    public void testIsOpenInitiallyTrueForWriteChannel() throws IOException {
        final Path path = openPath();
        final OutputStream mockWriter = mock(OutputStream.class);
        final AzureSeekableByteChannel channel = new AzureSeekableByteChannel(mockWriter, path);
        assertTrue(channel.isOpen());
    }

    @Test
    public void testCloseWriteChannelMarksAsClosed() throws IOException {
        final Path path = openPath();
        final OutputStream mockWriter = mock(OutputStream.class);
        final AzureSeekableByteChannel channel = new AzureSeekableByteChannel(mockWriter, path);
        channel.close();
        assertFalse(channel.isOpen());
        verify(mockWriter).close();
    }

    @Test
    public void testOperationsThrowAfterCloseWriteChannel() throws IOException {
        final Path path = openPath();
        final OutputStream mockWriter = mock(OutputStream.class);
        final AzureSeekableByteChannel channel = new AzureSeekableByteChannel(mockWriter, path);
        channel.close();
        assertThrows(ClosedChannelException.class, channel::position);
        assertThrows(ClosedChannelException.class, channel::size);
        assertThrows(ClosedChannelException.class,
                () -> channel.write(ByteBuffer.allocate(1)));
    }

    // -----------------------------------------------------------------------
    // isOpen / close (read channel)
    // -----------------------------------------------------------------------

    @Test
    public void testIsOpenInitiallyTrueForReadChannel() throws IOException {
        final Path path = openPath();
        final BlobInputStream mockReader = mock(BlobInputStream.class);
        final AzureSeekableByteChannel channel = new AzureSeekableByteChannel(mockReader, path);
        assertTrue(channel.isOpen());
    }

    @Test
    public void testCloseReadChannelMarksAsClosed() throws IOException {
        final Path path = openPath();
        final BlobInputStream mockReader = mock(BlobInputStream.class);
        final AzureSeekableByteChannel channel = new AzureSeekableByteChannel(mockReader, path);
        channel.close();
        assertFalse(channel.isOpen());
        verify(mockReader).close();
    }

    // -----------------------------------------------------------------------
    // Mode enforcement
    // -----------------------------------------------------------------------

    @Test
    public void testReadOnWriteChannelThrowsNonReadableChannelException() {
        final Path path = openPath();
        final OutputStream mockWriter = mock(OutputStream.class);
        final AzureSeekableByteChannel channel = new AzureSeekableByteChannel(mockWriter, path);
        assertThrows(NonReadableChannelException.class,
                () -> channel.read(ByteBuffer.allocate(10)));
    }

    @Test
    public void testWriteOnReadChannelThrowsNonWritableChannelException() {
        final Path path = openPath();
        final BlobInputStream mockReader = mock(BlobInputStream.class);
        final AzureSeekableByteChannel channel = new AzureSeekableByteChannel(mockReader, path);
        assertThrows(NonWritableChannelException.class,
                () -> channel.write(ByteBuffer.allocate(10)));
    }

    // -----------------------------------------------------------------------
    // position and size (write channel)
    // -----------------------------------------------------------------------

    @Test
    public void testInitialPositionIsZeroForWriteChannel() throws IOException {
        final Path path = openPath();
        final OutputStream mockWriter = mock(OutputStream.class);
        final AzureSeekableByteChannel channel = new AzureSeekableByteChannel(mockWriter, path);
        assertEquals(0L, channel.position());
    }

    @Test
    public void testInitialSizeIsZeroForWriteChannel() throws IOException {
        final Path path = openPath();
        final OutputStream mockWriter = mock(OutputStream.class);
        final AzureSeekableByteChannel channel = new AzureSeekableByteChannel(mockWriter, path);
        assertEquals(0L, channel.size());
    }

    @Test
    public void testWriteUpdatesPositionAndSize() throws IOException {
        final Path path = openPath();
        final OutputStream mockWriter = mock(OutputStream.class);
        final AzureSeekableByteChannel channel = new AzureSeekableByteChannel(mockWriter, path);
        final ByteBuffer buf = ByteBuffer.wrap(new byte[]{1, 2, 3});
        final int written = channel.write(buf);
        assertEquals(3, written);
        assertEquals(3L, channel.position());
        assertEquals(3L, channel.size());
    }

    @Test
    public void testWriteInvokesUnderlyingOutputStream() throws IOException {
        final Path path = openPath();
        final OutputStream mockWriter = mock(OutputStream.class);
        final AzureSeekableByteChannel channel = new AzureSeekableByteChannel(mockWriter, path);
        channel.write(ByteBuffer.wrap(new byte[]{10, 20}));
        verify(mockWriter).write(any(byte[].class), anyInt(), eq(2));
    }

    // -----------------------------------------------------------------------
    // Closed file system
    // -----------------------------------------------------------------------

    @Test
    public void testIsOpenThrowsWhenFileSystemIsClosed() {
        final Path path = closedFsPath();
        final OutputStream mockWriter = mock(OutputStream.class);
        final AzureSeekableByteChannel channel = new AzureSeekableByteChannel(mockWriter, path);
        assertThrows(ClosedFileSystemException.class, channel::isOpen);
    }

    // -----------------------------------------------------------------------
    // size for read channel
    // -----------------------------------------------------------------------

    @Test
    public void testSizeForReadChannelDelegatesToBlobProperties() throws IOException {
        final Path path = openPath();
        final BlobInputStream mockReader = mock(BlobInputStream.class);
        final BlobProperties mockProps = mock(BlobProperties.class);
        when(mockReader.getProperties()).thenReturn(mockProps);
        when(mockProps.getBlobSize()).thenReturn(512L);
        final AzureSeekableByteChannel channel = new AzureSeekableByteChannel(mockReader, path);
        assertEquals(512L, channel.size());
    }

    // -----------------------------------------------------------------------
    // read (read channel)
    // -----------------------------------------------------------------------

    @Test
    public void testReadReturnsMinusOneAtEndOfStream() throws IOException {
        final Path path = openPath();
        final BlobInputStream mockReader = mock(BlobInputStream.class);
        final BlobProperties mockProps = mock(BlobProperties.class);
        when(mockReader.getProperties()).thenReturn(mockProps);
        // Blob size = 0 so position (0) >= size (0) → returns -1
        when(mockProps.getBlobSize()).thenReturn(0L);
        final AzureSeekableByteChannel channel = new AzureSeekableByteChannel(mockReader, path);
        assertEquals(-1, channel.read(ByteBuffer.allocate(10)));
    }
}
