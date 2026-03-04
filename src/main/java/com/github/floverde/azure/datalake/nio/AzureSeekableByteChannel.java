package com.github.floverde.azure.datalake.nio;

import com.azure.storage.blob.specialized.BlobInputStream;
import java.nio.file.ClosedFileSystemException;
import java.io.OutputStream;
import java.nio.channels.*;
import java.io.InputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Objects;

/**
 * An implementation of {@link SeekableByteChannel} that provides
 * seekable read and write access to Azure Data Lake Storage blobs.
 * <p>A channel may only be opened in read mode OR write mode. It may not be opened in
 * read/write mode. Seeking is supported for reads, but not for writes. Modifications to
 * existing files is not permitted - only creating new files or overwriting existing files.</p>
 * <p>This type is not threadsafe to prevent having to hold locks across network calls.</p>
 *
 * @see SeekableByteChannel
 */
public class AzureSeekableByteChannel implements SeekableByteChannel
{
    /**
     * Current position of the channel.
     */
    private long position;

    /**
     * Flag indicating whether the channel is closed.
     */
    private boolean closed;

    /**
     * The path associated with this channel.
     */
    private final Path path;

    /**
     * The underlying writer for this channel, if it was opened for writing.
     * This will be {@code null} if the channel was opened for reading.
     */
    private final OutputStream writer;

    /**
     * The underlying reader for this channel, if it was opened for reading.
     * This will be {@code null} if the channel was opened for writing.
     */
    private final BlobInputStream reader;

    /**
     * Create a new channel for reading an ADLS resource.
     *
     * @param reader The underlying reader for this channel.
     * @param path The path associated with this channel.
     * @throws ClassCastException If the provided stream is not an instance of {@link BlobInputStream}.
     * @throws NullPointerException If either argument is {@code null}.
     */
    public AzureSeekableByteChannel(final InputStream reader, final Path path) {
        this((BlobInputStream) reader, path);
    }

    /**
     * Create a new channel for writing an ADLS resource.
     *
     * @param writer The underlying writer for this channel.
     * @param path The path associated with this channel.
     * @throws NullPointerException If either argument is {@code null}.
     */
    public AzureSeekableByteChannel(final OutputStream writer, final Path path) {
        this.writer = Objects.requireNonNull(writer, "OutputStream cannot be null");
        this.path = Objects.requireNonNull(path, "Path cannot be null");
        this.closed = false;
        this.position = 0L;
        this.reader = null;
    }

    /**
     * Create a new channel for reading an ADLS resource.
     *
     * @param reader The underlying reader for this channel.
     * @param path The path associated with this channel.
     * @throws NullPointerException If either argument is {@code null}.
     */
    public AzureSeekableByteChannel(final BlobInputStream reader, final Path path) {
        this.reader = Objects.requireNonNull(reader, "InputStream cannot be null");
        this.path = Objects.requireNonNull(path, "Path cannot be null");
        this.reader.mark(Integer.MAX_VALUE);
        this.closed = false;
        this.position = 0L;
        this.writer = null;
    }

    /**
     * Reads a sequence of bytes from this channel into the given buffer.
     *
     * <p>Bytes are read starting at this channel's current position, and
     * then the position is updated with the number of bytes actually read.
     * Otherwise this method behaves exactly as specified in the {@link
     * ReadableByteChannel} interface.</p>
     *
     * @param dst The buffer into which bytes are to be transferred.
     * @throws ClosedFileSystemException If the underlying file system is closed.
     * @throws NonReadableChannelException If this channel was not opened for reading.
     * @throws ClosedChannelException If this channel is closed.
     * @throws IOException If some other I/O error occurs.
     * @return The number of bytes read, possibly zero, or {@code -1}
     *         if the channel has reached end-of-stream.
     */
    @Override
    public int read(final ByteBuffer dst) throws IOException {
        int count, pos;
        final int limit;
        final byte[] buf;
        // Ensure that the underlying file system is still open
        AzureDataLakePath.ensureFileSystemOpen(this.path);
        // Ensure that the channel is still open for reading
        this.validateOpen().validateReadMode();
        // Check if the current position is greater than or equal to the blob size
        if (this.position >= this.size()) {
            // Return -1 to indicating the end-of-stream
            return -1;
        } else {
            if (dst.hasArray()) {
                pos = dst.position();
                limit = pos + dst.remaining();
                buf = dst.array();
            } else {
                pos = 0;
                limit = dst.remaining();
                buf = new byte[limit];
            }
            while(pos < limit) {
                count = this.reader.read(buf, pos, limit - pos);
                if (count == -1) {
                    break;
                }
                pos += count;
            }
            if (dst.hasArray()) {
                count = pos - dst.position();
                dst.position(pos);
            } else {
                count = pos;
                dst.put(buf, 0, pos);
            }

            this.position += count;
            return count;
        }
    }

    /**
     * Writes a sequence of bytes to this channel from the given buffer.
     *
     * <p>Bytes are written starting at this channel's current position, unless
     * the channel is connected to an entity such as a file that is opened with
     * the {@link java.nio.file.StandardOpenOption#APPEND APPEND} option, in
     * which case the position is first advanced to the end. The entity to which
     * the channel is connected is grown, if necessary, to accommodate the
     * written bytes, and then the position is updated with the number of bytes
     * actually written. Otherwise this method behaves exactly as specified by
     * the {@link WritableByteChannel} interface.</p>
     *
     * @param src The buffer from which bytes are to be retrieved.
     * @throws ClosedFileSystemException If the underlying file system is closed.
     * @throws NonWritableChannelException If this channel was not opened for writing.
     * @throws ClosedChannelException If this channel is closed.
     * @throws IOException If some other I/O error occurs.
     * @return The number of bytes written, possibly zero.
     */
    @Override
    public int write(final ByteBuffer src) throws IOException {
        final byte[] buf;
        final int pos, length;
        // Ensure that the underlying file system is still open
        AzureDataLakePath.ensureFileSystemOpen(this.path);
        // Ensure that the channel is still open for writing
        this.validateOpen().validateWriteMode();
        length = src.remaining();
        this.position += length;
        if (src.hasArray()) {
            pos = src.position();
            buf = src.array();
            src.position(pos + length);
        } else {
            pos = 0;
            buf = new byte[length];
            src.get(buf);
        }
        this.writer.write(buf, pos, length);
        return length;
    }

    /**
     * Sets this channel's position.
     * <p>Setting the position to a value that is greater than the current size is legal
     * but does not change the size of the entity. A later attempt to read bytes at such
     * a position will immediately return an end-of-file indication. A later attempt to
     * write bytes at such a position will cause the entity to grow to accommodate the
     * new bytes; the values of any bytes between the previous end-of-file and the
     * newly-written bytes are unspecified.</p><p>Setting the channel's position is not
     * recommended when connected to an entity, typically a file, that is opened with
     * the {@link java.nio.file.StandardOpenOption#APPEND APPEND} option. When opened
     * for append, the position is first advanced to the end before writing.</p>
     *
     * @param newPosition The new position of the channel, measured in bytes.
     * @throws ClosedFileSystemException If the underlying file system is closed.
     * @throws ClosedChannelException If this channel is closed.
     * @throws IllegalArgumentException If the new position is negative.
     * @throws IOException If some other I/O error occurs.
     * @return This channel.
     */
    @Override
    public SeekableByteChannel position(final long newPosition) throws IOException {
        // Ensure that the underlying file system is still open
        AzureDataLakePath.ensureFileSystemOpen(this.path);
        // Ensure that the channel is still open for reading
        this.validateOpen().validateReadMode();
        // Check that the new position is not negative
        if (newPosition < 0L) {
            throw new IllegalArgumentException("Seek position cannot be negative");
        } else if (newPosition > this.size()) {
            // If the new position is greater than the current size, set the
            // position to the new value but do not attempt to skip
            this.position = newPosition;
        } else {
            // Reset the reader to the beginning and skip to the new position
            this.reader.reset();
            // Mark the reader at the new position so that future calls to position() can skip from there
            this.reader.mark(Integer.MAX_VALUE);
            // Skip to the new position and update the position field if successful
            if (this.reader.skip(newPosition) < newPosition) {
                // If we were not able to skip to the desired position, throw an exception
                throw new IOException("Could not set desired position");
            } else {
                // Update the position field to the new value
                this.position = newPosition;
            }
        }
        return this;
    }

    /**
     * Truncates the entity, to which this channel is connected, to the given size.
     * <p>Because Azure Data Lake Storage does not support truncation, this method
     * will always throw an {@link UnsupportedOperationException}.</p>
     *
     * @param size The new size, a non-negative byte count.
     * @throws UnsupportedOperationException Always thrown.
     * @throws IOException If some other I/O error occurs.
     * @return This channel.
     */
    @Override
    public SeekableByteChannel truncate(final long size) throws IOException {
        // Truncation is not supported by Azure Data Lake Storage, so throw an exception
        throw new UnsupportedOperationException("Truncation is not supported by AzureSeekableByteChannel");
    }

    /**
     * Returns this channel's position.
     * <p>The position is the number of bytes from the beginning of the entity
     * to the current position. The initial position is always {@code 0}.</p>
     *
     * @throws ClosedFileSystemException If the underlying file system is closed.
     * @throws ClosedChannelException If this channel is closed.
     * @throws IOException If some other I/O error occurs.
     * @return This channel's position.
     */
    @Override
    public long position() throws IOException {
        // Ensure that the underlying file system is still open
        AzureDataLakePath.ensureFileSystemOpen(this.path);
        // Ensure that the channel is still open and return the current position
        return this.validateOpen().position;
    }

    /**
     * Returns the size of the blob. If the channel is open for reading,
     * this will return the size of the blob as reported by the reader.
     * <p>If the channel is open for writing, this will return the current
     * position as the size, since the actual size of the blob may not be
     * known until the channel is closed.</p>
     *
     * @throws ClosedFileSystemException If the underlying file system is closed.
     * @throws ClosedChannelException If this channel is closed.
     * @throws IOException If some other I/O error occurs.
     * @return The current size, measured in bytes.
     */
    @Override
    public long size() throws IOException {
        // Ensure that the underlying file system is still open
        AzureDataLakePath.ensureFileSystemOpen(this.path);
        // Ensure that the channel is still open
        this.validateOpen();
        // Check if the channel is open for reading
        if (this.reader != null) {
            // Return the size of the blob as reported by the reader
            return this.reader.getProperties().getBlobSize();
        }
        // Return the current position as the size
        return this.position;
    }

    /**
     * Tells whether this channel is open.
     *
     * @throws ClosedFileSystemException If the underlying file system is closed.
     * @return {@code true} if, and only if, this channel is open.
     */
    @Override
    public boolean isOpen() {
        // Ensure that the underlying file system is still open
        AzureDataLakePath.ensureFileSystemOpen(this.path);
        // Return true if the channel is not closed
        return !this.closed;
    }

    /**
     * Closes this channel.
     * <p>After a channel is closed, any further attempt to invoke I/O
     * operations upon it will cause a {@link ClosedChannelException} to be thrown.</p>
     * <p>If this channel is already closed then invoking this method has no effect.</p>
     * <p>This method may be invoked at any time. If some other thread has already invoked it,
     * however, then another invocation will block until the first invocation is complete, after
     * which it will return without effect.</p>
     *
     * @throws ClosedFileSystemException If the underlying file system is closed.
     * @throws IOException If an I/O error occurs.
     */
    @Override
    public void close() throws IOException {
        // Ensure that the underlying file system is still open
        AzureDataLakePath.ensureFileSystemOpen(this.path);
        // Check if the channel was created for reading
        if (this.reader != null) {
            // Close the underlying reader
            this.reader.close();
        } else {
            // Close the underlying writer
            this.writer.close();
        }
        // Mark the channel as closed
        this.closed = true;
    }

    /**
     * Helper method to validate that the channel is open.
     *
     * @throws ClosedChannelException If this channel is closed.
     * @return This channel if it is open.
     */
    private AzureSeekableByteChannel validateOpen() throws ClosedChannelException {
        if (this.closed) {
            throw new ClosedChannelException();
        }
        return this;
    }

    /**
     * Helper method to validate that the channel is open for reading.
     *
     * @throws NonReadableChannelException If this channel was not opened for reading.
     */
    private void validateReadMode() {
        if (this.reader == null) {
            throw new NonReadableChannelException();
        }
    }

    /**
     * Helper method to validate that the channel is open for writing.
     *
     * @throws NonWritableChannelException If this channel was not opened for writing.
     */
    private void validateWriteMode() {
        if (this.writer == null) {
            throw new NonWritableChannelException();
        }
    }
}
