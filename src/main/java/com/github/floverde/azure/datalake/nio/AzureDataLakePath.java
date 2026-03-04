package com.github.floverde.azure.datalake.nio;

import com.azure.storage.file.datalake.DataLakeDirectoryClient;
import com.azure.storage.file.datalake.DataLakeFileClient;
import io.netty.util.internal.EmptyArrays;
import io.netty.util.internal.StringUtil;
import java.nio.file.*;
import java.io.File;
import java.net.URI;
import java.util.*;

/**
 * A {@link Path} implementation for Azure Data Lake Storage Gen2.
 * <p>Paths are normalized on creation: backslashes are converted to forward slashes
 * and any trailing slashes are removed (except for the root path {@code "/"}).</p>
 * <p>Absolute paths start with {@code "/"}, while relative paths do not.
 * The root path is represented as {@code "/"} and the empty string represents
 * the empty (relative) path.</p>
 * <p>Conversion to {@link java.io.File} and registration with a
 * {@link WatchService} are not supported.</p>
 *
 * @see ADLSContainerFileSystem
 */
public class AzureDataLakePath implements Path
{
    private final ADLSContainerFileSystem fileSystem;
    private final String pathString; // normalized, no trailing slash except "/"

    AzureDataLakePath(final ADLSContainerFileSystem fileSystem, final String path) {
        this.fileSystem = fileSystem;
        this.pathString = normalize(path);
    }

    private static String normalize(String path) {
        if (path == null || path.isEmpty()) {
            return "";
        }
        // Normalize slashes
        String p = path.replace('\\', '/');
        // Remove trailing slash unless it's just "/"
        while (p.length() > 1 && p.endsWith("/")) {
            p = p.substring(0, p.length() - 1);
        }
        return p;
    }

    String getPathString() {
        return pathString;
    }

    @Override
    public ADLSContainerFileSystem getFileSystem() {
        return fileSystem;
    }

    @Override
    public boolean isAbsolute() {
        return pathString.startsWith("/");
    }

    @Override
    public Path getRoot() {
        if (isAbsolute()) {
            return new AzureDataLakePath(fileSystem, "/");
        }
        return null;
    }

    @Override
    public Path getFileName() {
        if (pathString.isEmpty()) {
            return null;
        }
        if (pathString.equals("/")) {
            return null;
        }
        String[] parts = getSegments();
        if (parts.length == 0) {
            return null;
        }
        return new AzureDataLakePath(fileSystem, parts[parts.length - 1]);
    }

    @Override
    public Path getParent() {
        if (pathString.isEmpty() || pathString.equals("/")) {
            return null;
        }
        int lastSlash = pathString.lastIndexOf('/');
        if (lastSlash < 0) {
            return null;
        }
        if (lastSlash == 0) {
            return new AzureDataLakePath(fileSystem, "/");
        }
        return new AzureDataLakePath(fileSystem, pathString.substring(0, lastSlash));
    }

    @Override
    public int getNameCount() {
        return getSegments().length;
    }

    @Override
    public Path getName(int index) {
        String[] parts = getSegments();
        if (index < 0 || index >= parts.length) {
            throw new IllegalArgumentException("Index out of range: " + index);
        }
        return new AzureDataLakePath(fileSystem, parts[index]);
    }

    @Override
    public Path subpath(int beginIndex, int endIndex) {
        String[] parts = getSegments();
        if (beginIndex < 0 || beginIndex >= parts.length || endIndex <= beginIndex || endIndex > parts.length) {
            throw new IllegalArgumentException("Invalid range: " + beginIndex + ", " + endIndex);
        }
        StringBuilder sb = new StringBuilder();
        for (int i = beginIndex; i < endIndex; i++) {
            if (sb.length() > 0) sb.append('/');
            sb.append(parts[i]);
        }
        return new AzureDataLakePath(fileSystem, sb.toString());
    }

    @Override
    public boolean startsWith(Path other) {
        if (!(other instanceof AzureDataLakePath)) {
            return false;
        }
        AzureDataLakePath o = (AzureDataLakePath) other;
        if (o.fileSystem != this.fileSystem) {
            return false;
        }
        if (this.isAbsolute() != o.isAbsolute()) {
            return false;
        }
        String[] thisParts = this.getSegments();
        String[] otherParts = o.getSegments();
        if (otherParts.length > thisParts.length) {
            return false;
        }
        // If other is root "/" and this is absolute
        if (o.pathString.equals("/") && this.isAbsolute()) {
            return true;
        }
        for (int i = 0; i < otherParts.length; i++) {
            if (!thisParts[i].equals(otherParts[i])) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean startsWith(String other) {
        return startsWith(new AzureDataLakePath(fileSystem, other));
    }

    @Override
    public boolean endsWith(Path other) {
        if (!(other instanceof AzureDataLakePath)) {
            return false;
        }
        AzureDataLakePath o = (AzureDataLakePath) other;
        if (o.isAbsolute()) {
            return this.equals(o);
        }
        String[] thisParts = this.getSegments();
        String[] otherParts = o.getSegments();
        if (otherParts.length > thisParts.length) {
            return false;
        }
        int offset = thisParts.length - otherParts.length;
        for (int i = 0; i < otherParts.length; i++) {
            if (!thisParts[offset + i].equals(otherParts[i])) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean endsWith(String other) {
        return endsWith(new AzureDataLakePath(fileSystem, other));
    }

    @Override
    public Path normalize() {
        String[] parts = getSegments();
        Deque<String> stack = new ArrayDeque<>();
        for (String part : parts) {
            if (part.equals(".")) {
                // skip
            } else if (part.equals("..")) {
                if (!stack.isEmpty() && !stack.peekLast().equals("..")) {
                    stack.pollLast();
                } else if (!isAbsolute()) {
                    stack.addLast("..");
                }
            } else {
                stack.addLast(part);
            }
        }
        StringBuilder sb = new StringBuilder();
        if (isAbsolute()) {
            sb.append('/');
        }
        Iterator<String> it = stack.iterator();
        while (it.hasNext()) {
            sb.append(it.next());
            if (it.hasNext()) sb.append('/');
        }
        return new AzureDataLakePath(fileSystem, sb.toString());
    }

    @Override
    public Path resolve(Path other) {
        if (other.isAbsolute()) {
            return other;
        }
        AzureDataLakePath o = toAzurePath(other);
        if (o.pathString.isEmpty()) {
            return this;
        }
        if (pathString.isEmpty()) {
            return o;
        }
        String base = pathString.endsWith("/") ? pathString : pathString + "/";
        return new AzureDataLakePath(fileSystem, base + o.pathString);
    }

    @Override
    public Path resolve(String other) {
        return resolve(new AzureDataLakePath(fileSystem, other));
    }

    @Override
    public Path resolveSibling(Path other) {
        Path parent = getParent();
        if (parent == null) {
            return other;
        }
        return parent.resolve(other);
    }

    @Override
    public Path resolveSibling(String other) {
        return resolveSibling(new AzureDataLakePath(fileSystem, other));
    }

    @Override
    public Path relativize(Path other) {
        AzureDataLakePath o = toAzurePath(other);
        if (this.isAbsolute() != o.isAbsolute()) {
            throw new IllegalArgumentException("Cannot relativize absolute path against relative path");
        }
        if (this.pathString.equals(o.pathString)) {
            return new AzureDataLakePath(fileSystem, "");
        }
        String[] thisParts = this.getSegments();
        String[] otherParts = o.getSegments();
        int commonLen = 0;
        int minLen = Math.min(thisParts.length, otherParts.length);
        while (commonLen < minLen && thisParts[commonLen].equals(otherParts[commonLen])) {
            commonLen++;
        }
        StringBuilder sb = new StringBuilder();
        for (int i = commonLen; i < thisParts.length; i++) {
            if (sb.length() > 0) sb.append('/');
            sb.append("..");
        }
        for (int i = commonLen; i < otherParts.length; i++) {
            if (sb.length() > 0) sb.append('/');
            sb.append(otherParts[i]);
        }
        return new AzureDataLakePath(fileSystem, sb.toString());
    }

    @Override
    public Path toAbsolutePath() {
        if (isAbsolute()) {
            return this;
        }
        return new AzureDataLakePath(fileSystem, "/" + pathString);
    }

    @Override
    public Path toRealPath(LinkOption... options) {
        return toAbsolutePath().normalize();
    }

    @Override
    public File toFile() {
        throw new UnsupportedOperationException("Azure Data Lake paths cannot be converted to File");
    }

    @Override
    public URI toUri() {
        URI rootUri = fileSystem.getRootURI();
        String path = isAbsolute() ? pathString : "/" + pathString;
        // rootUri is like abfss://container@account.dfs.core.windows.net
        String uriStr = rootUri.toString() + path;
        return URI.create(uriStr);
    }

    @Override
    public WatchKey register(WatchService watcher, WatchEvent.Kind<?>[] events, WatchEvent.Modifier... modifiers) {
        throw new UnsupportedOperationException("WatchService is not supported");
    }

    @Override
    public WatchKey register(WatchService watcher, WatchEvent.Kind<?>... events) {
        throw new UnsupportedOperationException("WatchService is not supported");
    }

    @Override
    public Iterator<Path> iterator() {
        String[] parts = getSegments();
        List<Path> list = new ArrayList<>();
        for (String part : parts) {
            list.add(new AzureDataLakePath(fileSystem, part));
        }
        return list.iterator();
    }

    @Override
    public int compareTo(Path other) {
        AzureDataLakePath o = toAzurePath(other);
        return this.pathString.compareTo(o.pathString);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof AzureDataLakePath)) return false;
        AzureDataLakePath other = (AzureDataLakePath) obj;
        return Objects.equals(fileSystem, other.fileSystem)
                && Objects.equals(pathString, other.pathString);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fileSystem, pathString);
    }

    @Override
    public String toString() {
        return pathString;
    }

    /**
     * Returns the path segments (splitting on '/'), excluding empty segments.
     * For root "/" returns empty array.
     */
    String[] getSegments() {
        if (pathString.isEmpty() || pathString.equals("/")) {
            return EmptyArrays.EMPTY_STRINGS;
        }
        String s = pathString;
        if (s.startsWith("/")) {
            s = s.substring(1);
        }
        return s.split("/", -1);
    }

    /**
     * Returns the path string suitable for use with Azure SDK (no leading slash).
     */
    String toAzurePathString() {
        if (this.isRoot()) {
            return StringUtil.EMPTY_STRING;
        }
        if (this.pathString.startsWith("/")) {
            return this.pathString.substring(1);
        }
        return this.pathString;
    }

    private AzureDataLakePath toAzurePath(Path p) {
        if (p instanceof AzureDataLakePath) {
            return (AzureDataLakePath) p;
        }
        return new AzureDataLakePath(fileSystem, p.toString());
    }

    DataLakeDirectoryClient getDirectoryClient() {
        return this.fileSystem.getDirectoryClient(this);
    }

    DataLakeFileClient getFileClient() {
        return this.fileSystem.getFileClient(this);
    }

    static void ensureFileSystemOpen(final Path path) {
        if (!path.getFileSystem().isOpen()) {
            throw new ClosedFileSystemException();
        }
    }

    public final boolean isRoot() {
        return this.pathString.isEmpty() || this.pathString.equals("/");
    }
}
