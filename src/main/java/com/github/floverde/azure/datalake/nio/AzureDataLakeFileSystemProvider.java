package com.github.floverde.azure.datalake.nio;

import com.azure.core.credential.AzureSasCredential;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import com.azure.storage.file.datalake.models.DataLakeStorageException;
import com.azure.storage.file.datalake.models.ListPathsOptions;
import com.azure.storage.file.datalake.models.PathProperties;

import java.io.*;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.*;
import java.nio.file.attribute.*;
import java.nio.file.spi.FileSystemProvider;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

public class AzureDataLakeFileSystemProvider extends FileSystemProvider {

    static final String SCHEME = "abfss";

    // Package-private to allow test subclass injection
    final Map<URI, AzureDataLakeFileSystem> fileSystems = new ConcurrentHashMap<>();

    // Stored env for on-demand filesystem creation; keyed by azure.account.<account-name>.*
    final AtomicReference<Map<String, ?>> storedEnv = new AtomicReference<>();

    @Override
    public String getScheme() {
        return SCHEME;
    }

    @Override
    public FileSystem newFileSystem(URI uri, Map<String, ?> env) throws IOException {
        // Store env (with per-account keys) before any check so getFileSystem can use it later
        if (env != null && !env.isEmpty()) {
            storedEnv.set(env);
        }
        URI rootUri = toRootUri(uri);
        if (fileSystems.containsKey(rootUri)) {
            throw new FileSystemAlreadyExistsException(rootUri.toString());
        }
        AzureDataLakeFileSystem fs = buildFileSystem(uri, rootUri, env);
        fileSystems.put(rootUri, fs);
        return fs;
    }

    @Override
    public FileSystem getFileSystem(URI uri) {
        URI rootUri = toRootUri(uri);
        AzureDataLakeFileSystem fs = fileSystems.get(rootUri);
        if (fs != null) {
            return fs;
        }
        Map<String, ?> env = storedEnv.get();
        if (env == null) {
            throw new FileSystemNotFoundException(rootUri.toString());
        }
        try {
            AzureDataLakeFileSystem newFs = buildFileSystem(uri, rootUri, env);
            AzureDataLakeFileSystem existing = fileSystems.putIfAbsent(rootUri, newFs);
            return existing != null ? existing : newFs;
        } catch (IOException e) {
            throw (FileSystemNotFoundException)
                    new FileSystemNotFoundException(rootUri.toString()).initCause(e);
        }
    }

    /**
     * Builds an {@link AzureDataLakeFileSystem} for the given URI.
     * <p>
     * The {@code env} map may contain per-account credentials using the format
     * {@code azure.account.<account-name>.key} or {@code azure.account.<account-name>.sas.token}.
     * </p>
     */
    protected AzureDataLakeFileSystem buildFileSystem(URI uri, URI rootUri, Map<String, ?> env)
            throws IOException {
        // Parse the authority: container@account.dfs.core.windows.net
        String authority = uri.getAuthority();
        if (authority == null) {
            throw new IllegalArgumentException("URI must have authority: " + uri);
        }
        int atIndex = authority.indexOf('@');
        if (atIndex < 0) {
            throw new IllegalArgumentException(
                    "URI authority must be container@account.dfs.core.windows.net: " + authority);
        }
        String containerName = authority.substring(0, atIndex);
        String host = authority.substring(atIndex + 1);
        String accountName = host.split("\\.")[0];
        String endpoint = "https://" + host;

        DataLakeServiceClientBuilder builder = new DataLakeServiceClientBuilder()
                .endpoint(endpoint);

        if (env != null) {
            String keyPrefix = "azure.account." + accountName + ".";
            if (env.containsKey(keyPrefix + "key")) {
                String accountKey = (String) env.get(keyPrefix + "key");
                builder.credential(new StorageSharedKeyCredential(accountName, accountKey));
            } else if (env.containsKey(keyPrefix + "sas.token")) {
                String sasToken = (String) env.get(keyPrefix + "sas.token");
                builder.credential(new AzureSasCredential(sasToken));
            }
            // For service principal, add azure-identity dependency and use ClientSecretCredentialBuilder.
        }

        DataLakeServiceClient serviceClient = builder.buildClient();
        DataLakeFileSystemClient fsClient = serviceClient.getFileSystemClient(containerName);
        return new AzureDataLakeFileSystem(this, fsClient, rootUri);
    }

    @Override
    public Path getPath(URI uri) {
        AzureDataLakeFileSystem fs = (AzureDataLakeFileSystem) getFileSystem(uri);
        String path = uri.getPath();
        if (path == null || path.isEmpty()) {
            path = "/";
        }
        return fs.getPath(path);
    }

    @Override
    public InputStream newInputStream(Path path, OpenOption... options) throws IOException {
        AzureDataLakePath adlsPath = toAzureDataLakePath(path);
        try {
            return adlsPath.getFileSystem().getFileSystemClient()
                    .getFileClient(adlsPath.toAzurePathString())
                    .openInputStream()
                    .getInputStream();
        } catch (DataLakeStorageException e) {
            throw toIOException(e, path);
        }
    }

    @Override
    public OutputStream newOutputStream(Path path, OpenOption... options) throws IOException {
        AzureDataLakePath adlsPath = toAzureDataLakePath(path);
        Set<OpenOption> opts = new HashSet<>(Arrays.asList(options));
        if (opts.isEmpty()) {
            opts.add(StandardOpenOption.CREATE);
            opts.add(StandardOpenOption.TRUNCATE_EXISTING);
        }

        boolean append = opts.contains(StandardOpenOption.APPEND);
        boolean createNew = opts.contains(StandardOpenOption.CREATE_NEW);

        com.azure.storage.file.datalake.DataLakeFileClient fileClient =
                adlsPath.getFileSystem().getFileSystemClient()
                        .getFileClient(adlsPath.toAzurePathString());

        if (createNew) {
            try {
                fileClient.getProperties();
                // File exists — throw
                throw new FileAlreadyExistsException(path.toString());
            } catch (DataLakeStorageException e) {
                if (e.getStatusCode() != 404) {
                    throw toIOException(e, path);
                }
                // 404 means file doesn't exist — fine for CREATE_NEW
            }
        }

        if (append) {
            return new AppendOutputStream(fileClient);
        } else {
            return new UploadOutputStream(fileClient);
        }
    }

    @Override
    public DirectoryStream<Path> newDirectoryStream(Path dir, DirectoryStream.Filter<? super Path> filter)
            throws IOException {
        AzureDataLakePath adlsPath = toAzureDataLakePath(dir);
        DataLakeFileSystemClient fsClient = adlsPath.getFileSystem().getFileSystemClient();
        return new AzureDataLakeDirectoryStream(adlsPath, fsClient, filter);
    }

    @Override
    public void createDirectory(Path dir, FileAttribute<?>... attrs) throws IOException {
        AzureDataLakePath adlsPath = toAzureDataLakePath(dir);
        try {
            adlsPath.getFileSystem().getFileSystemClient()
                    .getDirectoryClient(adlsPath.toAzurePathString())
                    .create();
        } catch (DataLakeStorageException e) {
            throw toIOException(e, dir);
        }
    }

    @Override
    public void delete(Path path) throws IOException {
        AzureDataLakePath adlsPath = toAzureDataLakePath(path);
        try {
            adlsPath.getFileSystem().getFileSystemClient()
                    .getFileClient(adlsPath.toAzurePathString())
                    .delete();
        } catch (DataLakeStorageException e) {
            if (e.getStatusCode() == 404) {
                throw new NoSuchFileException(path.toString());
            }
            throw toIOException(e, path);
        }
    }

    @Override
    public void copy(Path source, Path target, CopyOption... options) throws IOException {
        boolean replaceExisting = false;
        for (CopyOption opt : options) {
            if (opt == StandardCopyOption.REPLACE_EXISTING) {
                replaceExisting = true;
            }
        }
        AzureDataLakePath src = toAzureDataLakePath(source);
        AzureDataLakePath dst = toAzureDataLakePath(target);

        if (!replaceExisting) {
            try {
                dst.getFileSystem().getFileSystemClient()
                        .getFileClient(dst.toAzurePathString())
                        .getProperties();
                throw new FileAlreadyExistsException(target.toString());
            } catch (DataLakeStorageException e) {
                if (e.getStatusCode() != 404) {
                    throw toIOException(e, target);
                }
            }
        }

        // Use a temp file to avoid buffering the entire source file in memory.
        java.nio.file.Path tmp = createSecureTempFile("adls-copy-");
        try {
            try (FileOutputStream fos = new FileOutputStream(tmp.toFile())) {
                src.getFileSystem().getFileSystemClient()
                        .getFileClient(src.toAzurePathString())
                        .read(fos);
            } catch (DataLakeStorageException e) {
                throw toIOException(e, source);
            }
            try {
                dst.getFileSystem().getFileSystemClient()
                        .getFileClient(dst.toAzurePathString())
                        .uploadFromFile(tmp.toAbsolutePath().toString(), true);
            } catch (DataLakeStorageException e) {
                throw toIOException(e, target);
            }
        } finally {
            Files.deleteIfExists(tmp);
        }
    }

    @Override
    public void move(Path source, Path target, CopyOption... options) throws IOException {
        boolean replaceExisting = false;
        for (CopyOption opt : options) {
            if (opt == StandardCopyOption.REPLACE_EXISTING) {
                replaceExisting = true;
            }
        }
        AzureDataLakePath src = toAzureDataLakePath(source);
        AzureDataLakePath dst = toAzureDataLakePath(target);

        if (!replaceExisting) {
            try {
                dst.getFileSystem().getFileSystemClient()
                        .getFileClient(dst.toAzurePathString())
                        .getProperties();
                throw new FileAlreadyExistsException(target.toString());
            } catch (DataLakeStorageException e) {
                if (e.getStatusCode() != 404) {
                    throw toIOException(e, target);
                }
            }
        }

        try {
            src.getFileSystem().getFileSystemClient()
                    .getFileClient(src.toAzurePathString())
                    .rename(null, dst.toAzurePathString());
        } catch (DataLakeStorageException e) {
            throw toIOException(e, source);
        }
    }

    @Override
    public boolean isSameFile(Path path, Path path2) throws IOException {
        if (path.equals(path2)) {
            return true;
        }
        AzureDataLakePath p1 = toAzureDataLakePath(path);
        AzureDataLakePath p2 = toAzureDataLakePath(path2);
        return p1.toAbsolutePath().normalize().toString()
                .equals(p2.toAbsolutePath().normalize().toString());
    }

    @Override
    public boolean isHidden(Path path) {
        return false;
    }

    @Override
    public FileStore getFileStore(Path path) {
        throw new UnsupportedOperationException("FileStore is not supported");
    }

    @Override
    public void checkAccess(Path path, AccessMode... modes) throws IOException {
        AzureDataLakePath adlsPath = toAzureDataLakePath(path);
        String pathStr = adlsPath.toAzurePathString();
        if (pathStr.isEmpty()) {
            // root always exists
            return;
        }
        try {
            adlsPath.getFileSystem().getFileSystemClient()
                    .getFileClient(pathStr)
                    .getProperties();
        } catch (DataLakeStorageException e) {
            if (e.getStatusCode() == 404) {
                throw new NoSuchFileException(path.toString());
            }
            throw toIOException(e, path);
        }
    }

    @Override
    public <V extends FileAttributeView> V getFileAttributeView(Path path, Class<V> type, LinkOption... options) {
        return null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <A extends BasicFileAttributes> A readAttributes(Path path, Class<A> type, LinkOption... options)
            throws IOException {
        if (!type.isAssignableFrom(AzureDataLakeFileAttributes.class)) {
            throw new UnsupportedOperationException("Unsupported attribute type: " + type);
        }
        AzureDataLakePath adlsPath = toAzureDataLakePath(path);
        String pathStr = adlsPath.toAzurePathString();
        if (pathStr.isEmpty()) {
            return (A) new AzureDataLakeFileAttributes();
        }
        try {
            PathProperties props = adlsPath.getFileSystem().getFileSystemClient()
                    .getFileClient(pathStr)
                    .getProperties();
            Map<String, String> metadata = props.getMetadata();
            boolean isDir = metadata != null
                    && "true".equalsIgnoreCase(metadata.get("hdi_isfolder"));
            return (A) new AzureDataLakeFileAttributes(props, isDir);
        } catch (DataLakeStorageException e) {
            if (e.getStatusCode() == 404) {
                throw new NoSuchFileException(path.toString());
            }
            throw toIOException(e, path);
        }
    }

    @Override
    public Map<String, Object> readAttributes(Path path, String attributes, LinkOption... options)
            throws IOException {
        AzureDataLakeFileAttributes attrs = readAttributes(path, AzureDataLakeFileAttributes.class, options);
        Map<String, Object> result = new HashMap<>();
        String attrList = attributes;
        if (attrList.startsWith("basic:")) {
            attrList = attrList.substring("basic:".length());
        }
        if (attrList.equals("*")) {
            result.put("creationTime", attrs.creationTime());
            result.put("lastModifiedTime", attrs.lastModifiedTime());
            result.put("lastAccessTime", attrs.lastAccessTime());
            result.put("isDirectory", attrs.isDirectory());
            result.put("isRegularFile", attrs.isRegularFile());
            result.put("isSymbolicLink", attrs.isSymbolicLink());
            result.put("isOther", attrs.isOther());
            result.put("size", attrs.size());
            result.put("fileKey", attrs.fileKey());
        } else {
            for (String attr : attrList.split(",")) {
                attr = attr.trim();
                switch (attr) {
                    case "creationTime":     result.put(attr, attrs.creationTime());     break;
                    case "lastModifiedTime": result.put(attr, attrs.lastModifiedTime()); break;
                    case "lastAccessTime":   result.put(attr, attrs.lastAccessTime());   break;
                    case "isDirectory":      result.put(attr, attrs.isDirectory());      break;
                    case "isRegularFile":    result.put(attr, attrs.isRegularFile());    break;
                    case "isSymbolicLink":   result.put(attr, attrs.isSymbolicLink());   break;
                    case "isOther":          result.put(attr, attrs.isOther());          break;
                    case "size":             result.put(attr, attrs.size());             break;
                    case "fileKey":          result.put(attr, attrs.fileKey());          break;
                    default: break;
                }
            }
        }
        return result;
    }

    @Override
    public SeekableByteChannel newByteChannel(Path path, Set<? extends OpenOption> options,
                                              FileAttribute<?>... attrs) throws IOException {
        throw new UnsupportedOperationException("newByteChannel is not supported; use newInputStream/newOutputStream");
    }

    @Override
    public void setAttribute(Path path, String attribute, Object value, LinkOption... options) {
        throw new UnsupportedOperationException("setAttribute is not supported");
    }

    void removeFileSystem(URI rootUri) {
        fileSystems.remove(rootUri);
    }

    private static URI toRootUri(URI uri) {
        try {
            return new URI(uri.getScheme(), uri.getAuthority(), null, null, null);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid URI: " + uri, e);
        }
    }

    private static AzureDataLakePath toAzureDataLakePath(Path path) {
        if (!(path instanceof AzureDataLakePath)) {
            throw new ProviderMismatchException("Expected AzureDataLakePath, got: " + path.getClass());
        }
        return (AzureDataLakePath) path;
    }

    private static IOException toIOException(DataLakeStorageException e, Path path) {
        switch (e.getStatusCode()) {
            case 404:
                return new NoSuchFileException(path.toString());
            case 409:
                return new FileAlreadyExistsException(path.toString());
            default:
                return new IOException("Azure storage error: " + e.getMessage(), e);
        }
    }

    private static java.nio.file.Path createSecureTempFile(String prefix) throws IOException {
        try {
            return Files.createTempFile(prefix, null,
                    java.nio.file.attribute.PosixFilePermissions.asFileAttribute(
                            java.nio.file.attribute.PosixFilePermissions.fromString("rw-------")));
        } catch (UnsupportedOperationException e) {
            // Non-POSIX filesystem (e.g., Windows) — fall back to default
            return Files.createTempFile(prefix, null);
        }
    }

    /** OutputStream backed by a temp file; uploads on close (overwrite). */
    private static class UploadOutputStream extends OutputStream {
        private final com.azure.storage.file.datalake.DataLakeFileClient fileClient;
        private final java.nio.file.Path tmpFile;
        private final FileOutputStream tmpOut;
        private boolean closed = false;

        UploadOutputStream(com.azure.storage.file.datalake.DataLakeFileClient fileClient)
                throws IOException {
            this.fileClient = fileClient;
            this.tmpFile = createSecureTempFile("adls-upload-");
            this.tmpOut = new FileOutputStream(tmpFile.toFile());
        }

        @Override
        public void write(int b) throws IOException {
            tmpOut.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            tmpOut.write(b, off, len);
        }

        @Override
        public void close() throws IOException {
            if (!closed) {
                closed = true;
                try {
                    tmpOut.close();
                    try {
                        fileClient.uploadFromFile(tmpFile.toAbsolutePath().toString(), true);
                    } catch (DataLakeStorageException e) {
                        throw new IOException("Failed to upload data", e);
                    }
                } finally {
                    Files.deleteIfExists(tmpFile);
                }
            }
        }
    }

    /** OutputStream backed by a temp file; appends content on close. */
    private static class AppendOutputStream extends OutputStream {
        private final com.azure.storage.file.datalake.DataLakeFileClient fileClient;
        private final java.nio.file.Path tmpFile;
        private final FileOutputStream tmpOut;
        private boolean closed = false;

        AppendOutputStream(com.azure.storage.file.datalake.DataLakeFileClient fileClient)
                throws IOException {
            this.fileClient = fileClient;
            this.tmpFile = createSecureTempFile("adls-append-");
            this.tmpOut = new FileOutputStream(tmpFile.toFile());
        }

        @Override
        public void write(int b) throws IOException {
            tmpOut.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            tmpOut.write(b, off, len);
        }

        @Override
        public void close() throws IOException {
            if (!closed) {
                closed = true;
                try {
                    tmpOut.close();
                    long dataLen = Files.size(tmpFile);
                    long currentSize = 0;
                    try {
                        currentSize = fileClient.getProperties().getFileSize();
                    } catch (DataLakeStorageException e) {
                        if (e.getStatusCode() != 404) {
                            throw new IOException("Failed to get file properties", e);
                        }
                        // File doesn't exist yet — start from offset 0
                    }
                    try (FileInputStream fis = new FileInputStream(tmpFile.toFile())) {
                        fileClient.append(fis, currentSize, dataLen);
                    }
                    try {
                        fileClient.flush(currentSize + dataLen);
                    } catch (DataLakeStorageException e) {
                        throw new IOException("Failed to flush append data", e);
                    }
                } finally {
                    Files.deleteIfExists(tmpFile);
                }
            }
        }
    }
}
