package com.github.floverde.azure.datalake.nio;

import com.azure.core.util.CoreUtils;
import com.azure.core.credential.TokenCredential;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import com.azure.storage.file.datalake.models.DataLakeStorageException;
import com.azure.storage.file.datalake.models.PathProperties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.ConcurrentHashMap;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.spi.FileSystemProvider;
import io.netty.util.internal.StringUtil;
import java.nio.file.attribute.*;
import java.nio.file.*;
import java.net.URI;
import java.util.*;
import java.io.*;

/**
 * File system provider for Azure Data Lake Storage Gen2, implementing the {@link FileSystemProvider} interface to
 * allow Java applications to interact with ADLS Gen2 using the standard NIO APIs.<p>This provider supports efficient
 * connection management by caching account-level file system instances based on the root URI (scheme + authority),
 * and allows for flexible authentication using account keys, SAS tokens, service principal credentials, managed
 * identity, or any {@link TokenCredential} instance provided through the environment map.</p>
 *
 * <p>Supported authentication environment map keys:</p>
 * <ul>
 *   <li>{@code azure.account.key} — Storage account shared key (Base64-encoded string).</li>
 *   <li>{@code azure.sas.token} — Shared Access Signature token string.</li>
 *   <li>{@code azure.credential} — A pre-built {@link TokenCredential} instance (highest precedence after account key
 *       and SAS token).</li>
 *   <li>{@code azure.client.id} + {@code azure.client.secret} + {@code azure.tenant.id} — Service principal
 *       (client secret) credentials.</li>
 *   <li>{@code azure.managed.identity.client.id} — User-assigned managed identity client ID. If omitted but the key
 *       {@code azure.managed.identity.auto} is set to {@code "true"}, system-assigned managed identity is used.</li>
 * </ul>
 *
 * @see FileSystemProvider
 */
public class AzureDataLakeFileSystemProvider extends FileSystemProvider
{
    /**
     * URI scheme for Azure Data Lake Storage Gen2.
     */
    private static final String SCHEME = "abfss";

    /**
     * Map of root URI (scheme + authority) to account-level file system.
     * <p>The root URI is used to ensure that different containers under the
     * same account share the same file system instance, which allows for
     * efficient connection reuse and consistent metadata caching.</p>
     */
    final Map<URI, ADLSAccountFileSystem> fileSystems;

    /**
     * Define the set of supported options for opening an input stream.
     */
    private static final Set<OpenOption> INPUT_STREAM_OPTIONS;

    /**
     * Define the set of supported options for opening an output stream.
     */
    private static final Set<OpenOption> OUTPUT_STREAM_OPTIONS;

    /**
     * Default options for opening an output stream, which creates a new file or truncates an existing one.
     */
    private static final Set<OpenOption> OUTPUT_STREAM_DEFAULT_OPTIONS;

    /**
     * Creates a new instance of {@link AzureDataLakeFileSystemProvider}.
     */
    public AzureDataLakeFileSystemProvider() {
        // Initialize the map of the account-level file systems
        this.fileSystems = new ConcurrentHashMap<>();
    }

    /**
     * Creates a new file system for the given URI and environment.
     * <p>The URI must have the scheme "abfss" and an authority in the form of "&lt;account&gt;.dfs.core.windows.net".</p>
     * <p>The environment map can contain provider-specific properties for authentication. See the class-level
     * documentation for the full list of supported authentication keys.</p>
     * <p>The method ensures that only one file system instance is created per account (root URI)
     * and allows for multiple container-level file systems to be created under the same account.</p>
     *
     * @param uri URI reference.
     * @param env A map of provider specific properties to configure the file system.
     * @throws FileSystemAlreadyExistsException If a file system for the given root URI already exists.
     * @throws ProviderMismatchException if the URI is invalid or does not have the expected scheme.
     * @throws IllegalArgumentException If the URI is invalid or missing required components.
     * @throws IOException If an I/O error occurs creating the file system.
     * @return A new file system instance.
     */
    @Override
    public FileSystem newFileSystem(final URI uri, final Map<String, ?> env) throws IOException {
        final URI rootURI;
        final String containerName;
        final ADLSAccountFileSystem accountFs;
        // Get the root URI (scheme + authority) to use as
        // the key for caching the account-level file system
        rootURI = AzureDataLakeFileSystemProvider.toRootURI(uri);
        // Atomically compute or retrieve the account-level file system for
        // the root URI, ensuring that only one instance is created per account
        accountFs = this.fileSystems.compute(rootURI, (key, value) -> {
            if (value != null) {
                throw new FileSystemAlreadyExistsException(key.toString());
            }
            return this.createFileSystem(key, env);
        });
        // If the URI contains user info, treat it as the container
        // name and return the corresponding container-level file system
        if (!StringUtil.isNullOrEmpty((containerName = uri.getUserInfo()))) {
            return accountFs.getContainerFileSystem(containerName);
        }
        // If no container is specified, return the account-level file system
        return accountFs;
    }

    /**
     * Creates a new account-level file system for the given root URI and environment.
     *
     * <p>Authentication is resolved from the environment map in the following priority order:</p>
     * <ol>
     *   <li>{@code azure.account.key} — Storage account shared key.</li>
     *   <li>{@code azure.sas.token} — Shared Access Signature token.</li>
     *   <li>{@code azure.credential} — A pre-built {@link TokenCredential} instance.</li>
     *   <li>{@code azure.client.id} + {@code azure.client.secret} + {@code azure.tenant.id} — Service principal
     *       with client secret.</li>
     *   <li>{@code azure.managed.identity.client.id} — User-assigned managed identity.</li>
     *   <li>{@code azure.managed.identity.auto=true} — System-assigned managed identity.</li>
     * </ol>
     *
     * @param rootURI the root URI (scheme + authority) for the account-level file system.
     * @param env a map of provider specific properties to configure the file system, such as authentication credentials.
     * @return a new instance of {@link ADLSAccountFileSystem} configured with the given root URI and environment.
     */
    private ADLSAccountFileSystem createFileSystem(final URI rootURI, final Map<String, ?> env) {
        final DataLakeServiceClientBuilder builder;
        final ADLSConfigurationReader configuration;
        // Extract the authority from the root URI
        final String authority = rootURI.getAuthority();
        // Ensure that the authority is present in the URI
        if (authority != null) {
            // Initialize the DataLakeServiceClientBuilder with the endpoint constructed from the authority
            builder = new DataLakeServiceClientBuilder().endpoint("https://" + authority);
        } else {
            // Raise an exception indicating that the URI must have an authority component
            throw new IllegalArgumentException("URI must have authority: " + rootURI);
        }
        // Create an object to read the configuration from the environment map
        configuration = new ADLSConfigurationReader(env, authority);
        // Check whether the environment contains a shared key credential
        if (configuration.hasSharedKeyCredential()) {
            // If a shared key credential is found, configure the builder with it
            builder.credential(configuration.getSharedKeyCredential());
        }
        // Check whether the environment contains a SAS token credential
        else if (configuration.hasSasCredential()) {
            // If a SAS token credential is found, configure the builder with it
            builder.credential(configuration.getSasCredential());
        }
        // Check whether the environment contains a pre-built TokenCredential instance
        else if (configuration.hasPreBuiltCredential()) {
            // If a pre-built TokenCredential is found, configure the builder with it
            builder.credential(configuration.getPreBuiltCredential());
        }
        // Check whether the environment contains service principal credentials
        else if (configuration.hasClientSecretCredential()) {
            // If service principal credentials are found, configure the builder with it
            builder.credential(configuration.getClientSecretCredential());
        }
        // Check whether the environment contains managed identity credentials
        else if (configuration.hasManagedIdentityCredential()) {
            // If managed identity credentials are found, configure the builder with it
            builder.credential(configuration.getManagedIdentityCredential());
        }
        // Create a new account-level file system using the configured builder and root URI
        return new ADLSAccountFileSystem(this, builder.buildClient(), rootURI);
    }

    /**
     * Helper method to retrieve the appropriate file system for
     * a given URI, optionally requiring a container to be specified.
     * <p>If {@code requireContainer} is {@code true}, the method will throw an
     * exception if the URI does not specify a container name as user info.</p>
     *
     * @param uri the URI for which to retrieve the file system.
     * @param requireContainer {@code true} to require the URI to specify a container name as user info.
     * @return the file system associated with the given URI.
     */
    private FileSystem getFileSystem(final URI uri, final boolean requireContainer) {
        final URI rootUri;
        final String containerName;
        final ADLSAccountFileSystem accountFs;
        // Get the root URI to retrieve an account-level file system
        rootUri = AzureDataLakeFileSystemProvider.toRootURI(uri);
        // Retrieve the account-level file system from the cache
        if ((accountFs = this.fileSystems.get(rootUri)) == null) {
            // Throw an exception if no file system exists for the root
            // URI, which means the URI is invalid or not yet initialized
            throw new FileSystemNotFoundException(rootUri.toString());
        }
        // If the URI contains user info, treat it as the container name
        if (!StringUtil.isNullOrEmpty((containerName = uri.getUserInfo()))) {
            // Return the container-level file system for the specified container name
            return accountFs.getContainerFileSystem(containerName);
        } else if (requireContainer) {
            // If a container is required but not specified, throw an exception with a clear message
            throw new IllegalArgumentException("URI must specify the container name as user info: " +
                    "abfss://<container>@" + uri.toString().substring(8));
        }
        // If no container is specified, return the account-level file system
        return accountFs;
    }

    /**
     * Retrieves the file system associated with the given URI.
     * <p>The URI must have the scheme "abfss" and an authority in the form of
     * "&lt;account&gt;.dfs.core.windows.net".</p><p>If the URI contains user info,
     * it is treated as the container name, and the corresponding container-level
     * file system is returned.</p>
     *
     * @param uri the URI for which to retrieve the file system.
     * @return the file system associated with the given URI.
     * @throws FileSystemNotFoundException if no file system exists for the root URI (scheme + authority).
     * @throws IllegalArgumentException if a container is required but not specified in the URI.
     */
    @Override
    public FileSystem getFileSystem(final URI uri) {
        // Return the file system associated with the given URI,
        // allowing either account-level or container-level ones
        return this.getFileSystem(uri, false);
    }

    /**
     * Retrieves a {@link Path} object for the given URI by first obtaining the appropriate file system and then
     * extracting the path component from the URI. <p>The method ensures that the URI is valid and that the file
     * system exists for the given URI, throwing appropriate exceptions if not. If the path component of the URI
     * is empty or {@code null}, the method returns the root path of the file system.</p>
     *
     * @param uri The URI to convert.
     * @throws FileSystemNotFoundException if no file system exists for the root URI (scheme + authority).
     * @throws IllegalArgumentException if the URI is invalid or missing required components.
     * @throws ProviderMismatchException if the URI scheme does not match this provider.
     * @return A {@link Path} object corresponding to the given URI.
     * @see Path#of(URI)
     */
    @Override
    public Path getPath(final URI uri) {
        final String path;
        final FileSystem fs;
        // Get the container-level file system for the given URI
        fs = this.getFileSystem(uri, true);
        // Extract the path component from the URI and return a Path object for it
        if (StringUtil.isNullOrEmpty((path = uri.getPath()))) {
            // If the path component is empty or null, return the root path of the file system
            return fs.getPath("/");
        }
        // Return the Path object for the extracted path component
        return fs.getPath(path);
    }

    /**
     * Opens a file for reading and returns an input stream to read from the file.
     *
     * @param path the path to the file to open.
     * @param options options specifying how the file is opened.
     * @throws IOException if an I/O error occurs or the file cannot be read.
     * @return an input stream to read from the file.
     * @see Files#newInputStream(Path, OpenOption...)
     */
    @Override
    public InputStream newInputStream(final Path path, final OpenOption... options) throws IOException {
        // If options are provided, check that they only contain the read option; if not, raise an exception
        if (CoreUtils.isNullOrEmpty(options) || (options.length == 1 && !options[0].equals(StandardOpenOption.READ))) {
            // Raise an exception indicating that only the read option is supported
            throw new UnsupportedOperationException("Only the read option is supported.");
        }
        // Open the input stream for the specified path using the ADLS client and return it
        return AzureDataLakeFileSystemProvider.openInputStream(path);
    }

    /**
     * Opens a file for reading and returns an input stream to read from the file, using the ADLS client.
     *
     * @param path the path to the file to open.
     * @return an input stream to read from the file.
     * @throws IOException if an I/O error occurs or the file cannot be read.
     * @see AzureDataLakeFileSystemProvider#newInputStream(Path, OpenOption...)
     */
    private static InputStream openInputStream(final Path path) throws IOException {
        final AzureDataLakePath adlsPath;
        // Cast the given path to an ADLS-specific path object
        adlsPath = AzureDataLakeFileSystemProvider.toAzureDataLakePath(path);
        // Ensure that the underlying file system is still open
        AzureDataLakePath.ensureFileSystemOpen(path);
        try {
            // Return an InputStream for the specified path by opening a stream
            return adlsPath.getFileClient().openInputStream().getInputStream();
        } catch (final DataLakeStorageException ex) {
            // Rethrow any storage exceptions as IOExceptions with a clear message
            throw AzureDataLakeFileSystemProvider.toIOException(ex, path);
        }
    }

    /**
     * Opens a file for writing and returns an output stream to write to the file.
     * <p>The method validates the provided open options to ensure that only supported combinations are allowed
     * for opening an OutputStream.</p><p>If the {@code CREATE_NEW} option is specified, the method checks that
     * the file does not already exist and throws a {@link FileAlreadyExistsException} if it does.</p><p>If the
     * {@code APPEND} option is specified, the method returns an OutputStream that appends content to the end
     * of the file if it already exists; otherwise, it returns an OutputStream that creates a new file or
     * overwrites an existing one.</p>
     *
     * @param path the path to the file to open or create.
     * @param options options specifying how the file is opened.
     * @throws ProviderMismatchException if the given path is not an instance of {@link AzureDataLakePath}.
     * @throws IOException if an I/O error occurs or the file cannot be opened for writing.
     * @throws ClosedFileSystemException If the underlying file system is closed.
     * @return an output stream to write to the file.
     * @see Files#newOutputStream(Path, OpenOption...)
     */
    @Override
    public OutputStream newOutputStream(final Path path, final OpenOption... options) throws IOException {
        return this.newOutputStream(path, options != null ? Set.of(options) : null);
    }

    /**
     * Internal helper method to open a file for writing and return an output stream, with a set of options.
     *
     * @param path the path to the file to open or create.
     * @param opts options specifying how the file is opened; may be {@code null} or empty to use default options.
     * @throws ProviderMismatchException if the given path is not an instance of {@link AzureDataLakePath}.
     * @throws IOException if an I/O error occurs or the file cannot be opened for writing.
     * @throws ClosedFileSystemException If the underlying file system is closed.
     * @see AzureDataLakeFileSystemProvider#newOutputStream(Path, OpenOption...)
     * @return an output stream to write to the file.
     */
    private OutputStream newOutputStream(final Path path, Set<? extends OpenOption> opts) throws IOException {
        final AzureDataLakePath adlsPath;
        final DataLakeFileClient fileClient;
        // Cast the given path to an ADLS-specific path object
        adlsPath = AzureDataLakeFileSystemProvider.toAzureDataLakePath(path);
        // Validate the provided open options, ensuring that only supported
        // combinations are allowed for opening an OutputStream
        if (CoreUtils.isNullOrEmpty(opts)) {
            opts = AzureDataLakeFileSystemProvider.OUTPUT_STREAM_DEFAULT_OPTIONS;
        } else {
            for (final OpenOption opt : opts) {
                if (AzureDataLakeFileSystemProvider.OUTPUT_STREAM_OPTIONS.contains(opt)) {
                    throw new UnsupportedOperationException("Unsupported open option: " + opt);
                }
            }
            if (!opts.contains(StandardOpenOption.WRITE) || !opts.contains(StandardOpenOption.TRUNCATE_EXISTING) &&
                    !opts.contains(StandardOpenOption.CREATE_NEW) && !opts.contains(StandardOpenOption.APPEND)) {
                throw new IllegalArgumentException("Write and either Append, CreateNew or TruncateExisting" +
                        " must be specified to open an OutputStream");
            }
        }
        // Ensure that the underlying file system is still open
        AzureDataLakePath.ensureFileSystemOpen(path);
        // Get the ADLS client for this specific file
        fileClient = adlsPath.getFileClient();
        // If required, check that the file does not already exist
        if (opts.contains(StandardOpenOption.CREATE_NEW)) {
            try {
                // Check if file exists by getting its properties
                fileClient.getProperties();
                // If the previous command was successful then the file already exists
                throw new FileAlreadyExistsException(path.toString());
            } catch (final DataLakeStorageException e) {
                // 404 means file doesn't exist — fine for CREATE_NEW
                if (e.getStatusCode() != 404) {
                    // In the other cases, rethrow as IOException with a clear message
                    throw AzureDataLakeFileSystemProvider.toIOException(e, path);
                }
            }
        }
        // Check whether to append the content if the file already exists
        if (opts.contains(StandardOpenOption.APPEND)) {
            // Returns an OutputStream that appends the content to the end of the file if it already exists
            return new AzureDataLakeFileSystemProvider.AppendOutputStream(fileClient);
        }
        // Returns an OutputStream that creates a new file or overwrites an existing one
        return new AzureDataLakeFileSystemProvider.UploadOutputStream(fileClient);
    }

    /**
     * Opens a directory, returning a {@code DirectoryStream} to iterate over the
     * entries in the directory.<p>This method works in exactly the manner specified
     * by the {@link Files#newDirectoryStream(Path, DirectoryStream.Filter)} method.</p>
     *
     * @param dir the path to the directory.
     * @param filter the directory stream filter.
     * @throws ProviderMismatchException if the given path is not an instance of {@link AzureDataLakePath}.
     * @throws NotDirectoryException if the file could not otherwise be opened because
     *         it is not a directory <i>(optional specific exception)</i>.
     * @throws ClosedFileSystemException If the underlying file system is closed.
     * @throws IOException if an I/O error occurs.
     * @return a new and open {@code DirectoryStream} object for the specified directory.
     * @see Files#newDirectoryStream(Path, DirectoryStream.Filter)
     */
    @Override
    public DirectoryStream<Path> newDirectoryStream(final Path dir, final DirectoryStream.Filter<? super Path> filter) throws IOException {
        final AzureDataLakePath adlsPath;
        final DataLakeFileSystemClient fsClient;
        // Cast the given path to an ADLS-specific path object
        adlsPath = AzureDataLakeFileSystemProvider.toAzureDataLakePath(dir);
        // Ensure that the underlying file system is still open
        AzureDataLakePath.ensureFileSystemOpen(adlsPath);
        // Get the ADLS file system client for this specific path
        fsClient = adlsPath.getFileSystem().getFileSystemClient();
        // Return a new DirectoryStream that lists the entries in the specified directory
        return new AzureDataLakeDirectoryStream(adlsPath, fsClient, filter);
    }

    /**
     * Creates a new directory at the specified path. <p>This method works in exactly the
     * manner specified by the {@link Files#createDirectory(Path, FileAttribute[])} method.</p>
     * <p>The method ensures that the given path is valid and that the underlying file system
     * is still open, throwing appropriate exceptions if not. If the directory already exists,
     * a {@link FileAlreadyExistsException} is thrown.</p>
     *
     * @param dir the directory to create
     * @param attrs an optional list of file attributes to set atomically when creating the directory.
     * @throws ProviderMismatchException if the given path is not an instance of {@link AzureDataLakePath}.
     * @throws IOException if an I/O error occurs or the directory cannot be created.
     * @throws ClosedFileSystemException If the underlying file system is closed.
     * @see Files#createDirectory(Path, FileAttribute[])
     */
    @Override
    public void createDirectory(final Path dir, final FileAttribute<?>... attrs) throws IOException {
        final AzureDataLakePath adlsPath;
        // Cast the given path to an ADLS-specific path object
        adlsPath = AzureDataLakeFileSystemProvider.toAzureDataLakePath(dir);
        // Ensure that the underlying file system is still open
        AzureDataLakePath.ensureFileSystemOpen(adlsPath);
        try {
            adlsPath.getDirectoryClient().create();
        } catch (final DataLakeStorageException ex) {
            throw AzureDataLakeFileSystemProvider.toIOException(ex, dir);
        }
    }

    /**
     * Deletes a file.<p>This method works in exactly the manner
     * specified by the {@link Files#delete(Path)} method.</p>
     *
     * @param path the path to the file to delete.
     * @throws ProviderMismatchException if the given path is not an instance of {@link AzureDataLakePath}.
     * @throws ClosedFileSystemException If the underlying file system is closed.
     * @throws IOException if an I/O error occurs or the file cannot be deleted.
     * @see Files#delete(Path)
     */
    @Override
    public void delete(final Path path) throws IOException {
        final AzureDataLakePath adlsPath;
        adlsPath = AzureDataLakeFileSystemProvider.toAzureDataLakePath(path);
        // Ensure that the underlying file system is still open
        AzureDataLakePath.ensureFileSystemOpen(adlsPath);
        try {
            adlsPath.getFileClient().delete();
        } catch (final DataLakeStorageException ex) {
            throw AzureDataLakeFileSystemProvider.toIOException(ex, path);
        }
    }

    /**
     * Helper method to determine if the {@link StandardCopyOption#REPLACE_EXISTING
     * REPLACE_EXISTING} option is present in the given array of copy options.
     *
     * @param options the array of copy options to check; may be {@code null} or empty.
     * @return {@code true} if the {@code REPLACE_EXISTING} option is present; {@code false} otherwise.
     */
    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    private static boolean isReplaceExisting(final CopyOption[] options) {
        for (final CopyOption opt : options) {
            if (opt == StandardCopyOption.REPLACE_EXISTING) {
                return true;
            }
        }
        return false;
    }

    /**
     * Copy a file to a target file.<p>This method works in exactly the manner specified
     * by the {@link Files#copy(Path,Path,CopyOption[])} method except that both the
     * source and target paths must be associated with this provider.</p>
     *
     * @param source the path to the file to copy.
     * @param target the path to the target file.
     * @param options options specifying how the copy should be done.
     *
     * @throws ProviderMismatchException if the given path is not an instance of {@link AzureDataLakePath}.
     * @throws UnsupportedOperationException if the array contains a copy option that is not supported.
     * @throws FileAlreadyExistsException if the target file exists but cannot be replaced because the
     *         {@code REPLACE_EXISTING} option is not specified <i>(optional specific exception)</i>
     * @throws DirectoryNotEmptyException the {@code REPLACE_EXISTING} option is specified but the file
     *         cannot be replaced because it is a non-empty directory <i>(optional specific exception)</i>.
     * @throws ClosedFileSystemException If the underlying file system is closed.
     * @throws IOException if an I/O error occurs.
     * @see Files#copy(Path, Path, CopyOption...)
     */
    @Override
    public void copy(final Path source, final Path target, final CopyOption... options) throws IOException {
        final Path tmp;
        final AzureDataLakePath src, dst;
        DataLakeFileClient dstClient = null;
        // Ensure that the source file system is still open
        AzureDataLakePath.ensureFileSystemOpen(source);
        // Ensure that the target file system is still open
        AzureDataLakePath.ensureFileSystemOpen(target);
        // Cast the source paths to ADLS-specific path object
        src = AzureDataLakeFileSystemProvider.toAzureDataLakePath(source);
        // Cast the target paths to ADLS-specific path object
        dst = AzureDataLakeFileSystemProvider.toAzureDataLakePath(target);
        // If replacement is not requested, check that the target file does not already exist
        if (!AzureDataLakeFileSystemProvider.isReplaceExisting(options)) {
            try {
                // Get the ADLS client for the target file
                dstClient = dst.getFileClient();
                // Check if the target file exists by getting its properties
                dstClient.getProperties();
                // If the previous command was successful then the file already exists
                throw new FileAlreadyExistsException(target.toString());
            } catch (final DataLakeStorageException ex) {
                // 404 means file doesn't exist, which is fine for copy
                if (ex.getStatusCode() != 404) {
                    // In the other cases, rethrow as IOException with a clear message
                    throw AzureDataLakeFileSystemProvider.toIOException(ex, target);
                }
            }
        }
        // Use a temp file to avoid buffering the entire source file in memory.
        tmp = AzureDataLakeFileSystemProvider.createSecureTempFile("adls-copy-");
        try {
            try (FileOutputStream fos = new FileOutputStream(tmp.toFile())) {
                src.getFileClient().read(fos);
            } catch (final DataLakeStorageException ex) {
                throw AzureDataLakeFileSystemProvider.toIOException(ex, source);
            }
            try {
                if (dstClient == null) {
                    dstClient = dst.getFileClient();
                }
                dstClient.uploadFromFile(tmp.toAbsolutePath().toString(), true);
            } catch (final DataLakeStorageException ex) {
                throw AzureDataLakeFileSystemProvider.toIOException(ex, target);
            }
        } finally {
            // Delete the temporary file after the usage
            Files.deleteIfExists(tmp);
        }
    }

    /**
     * Move or rename a file to a target file. This method works in exactly the
     * manner specified by the {@link Files#move} method except that both the
     * source and target paths must be associated with this provider.
     *
     * @param source the path to the file to move.
     * @param target the path to the target file.
     * @param options options specifying how the move should be done.
     * @throws ProviderMismatchException if the given path is not an instance of {@link AzureDataLakePath}.
     * @throws UnsupportedOperationException if the array contains a copy option that is not supported.
     * @throws FileAlreadyExistsException if the target file exists but cannot be replaced because the
     *         {@code REPLACE_EXISTING} option is not specified <i>(optional specific exception)</i>.
     * @throws ClosedFileSystemException If the underlying file system is closed.
     * @throws IOException if an I/O error occurs.
     * @see Files#move(Path, Path, CopyOption...)
     */
    @Override
    public void move(final Path source, final Path target, final CopyOption... options) throws IOException {
        final AzureDataLakePath src, dst;
        // Ensure that the source file system is still open
        AzureDataLakePath.ensureFileSystemOpen(source);
        // Ensure that the target file system is still open
        AzureDataLakePath.ensureFileSystemOpen(target);
        // Cast the source paths to ADLS-specific path object
        src = AzureDataLakeFileSystemProvider.toAzureDataLakePath(source);
        // Cast the target paths to ADLS-specific path object
        dst = AzureDataLakeFileSystemProvider.toAzureDataLakePath(target);
        // If replacement is not requested, check that the target file does not already exist
        if (!AzureDataLakeFileSystemProvider.isReplaceExisting(options)) {
            try {
                // Check if the target file exists by getting its properties
                dst.getFileClient().getProperties();
                // If the previous command was successful then the file already exists
                throw new FileAlreadyExistsException(target.toString());
            } catch (final DataLakeStorageException ex) {
                // 404 means file doesn't exist, which is fine for copy
                if (ex.getStatusCode() != 404) {
                    // In the other cases, rethrow as IOException with a clear message
                    throw AzureDataLakeFileSystemProvider.toIOException(ex, target);
                }
            }
        }
        try {
            src.getFileClient().rename(null, dst.toAzurePathString());
        } catch (final DataLakeStorageException ex) {
            throw AzureDataLakeFileSystemProvider.toIOException(ex, source);
        }
    }

    /**
     * Determines if two paths locate the same file by comparing their normalized string representations.
     *
     * @param path one path to the file.
     * @param path2 the other path.
     * @throws IOException if an I/O error occurs or the paths cannot be compared.
     * @return {@code true} if the two paths locate the same file; {@code false} otherwise.
     * @see Files#isSameFile(Path, Path)
     */
    @Override
    public boolean isSameFile(final Path path, final Path path2) throws IOException {
        return path.equals(path2) || AzureDataLakeFileSystemProvider.toAzureDataLakePath(
                path).normalize().toString().equals(AzureDataLakeFileSystemProvider.
                toAzureDataLakePath(path2).normalize().toString());
    }

    /**
     * Determines if a file is considered hidden.<p>This method works in exactly the manner specified by the
     * {@link Files#isHidden(Path)} method except that the given path must be associated with this provider.</p>
     * <p>Since ADLS Gen2 does not have a concept of hidden files, this method always returns {@code false}.</p>
     *
     * @param path the path to the file to test
     * @return {@code false} since ADLS Gen2 does not have a concept of hidden files.
     * @see Files#isHidden(Path)
     */
    @Override
    public boolean isHidden(final Path path) {
        return false;
    }

    /**
     * Returns the {@link FileStore} representing the file store where a file is located.
     * <p>This method works in exactly the manner specified by the {@link Files#getFileStore} method.</p>
     *
     * @param path the path to the file.
     * @return the file store where the file is stored.
     * @throws IOException if an I/O error occurs.
     * @see Files#getFileStore(Path)
     */
    @Override
    public FileStore getFileStore(final Path path) throws IOException {
        throw new UnsupportedOperationException("FileStore is not supported");
    }

    /**
     * Checks the existence and accessibility of a file by attempting to retrieve its properties.
     *
     * @param path the path to the file to check.
     * @param modes The access modes to check; may have zero elements.
     * @throws IOException if an I/O error occurs or the file does not exist or is not accessible.
     * @throws ClosedFileSystemException If the underlying file system is closed.
     */
    @Override
    public void checkAccess(final Path path, final AccessMode... modes) throws IOException {
        final AzureDataLakePath adlsPath;
        // Ensure that at least one access mode is specified
        if (CoreUtils.isNullOrEmpty(modes)) {
            // Raise an exception if no access modes are specified
            throw new AccessDeniedException("The access cannot be determined.");
        }
        // Ensure that the underlying file system is still open
        AzureDataLakePath.ensureFileSystemOpen(path);
        // Ensure that the given path is an ADLS-specific object
        if (path instanceof AzureDataLakePath) {
            // Cast the given path to an ADLS-specific path object
            adlsPath = (AzureDataLakePath) path;
            // Skip access checks if the path refers to the root
            // directory, since it always exists and is accessible
            if (!adlsPath.isRoot()) {
                try {
                    // Check access by attempting to get the file properties;
                    // if it succeeds, the file exists and is accessible
                    adlsPath.getFileClient().getProperties();
                } catch (final DataLakeStorageException ex) {
                    // Rethrow any storage exceptions as IOExceptions with a clear message
                    throw AzureDataLakeFileSystemProvider.toIOException(ex, path);
                }
            }
        }
    }

    /**
     * Returns a file attribute view of a given type.<p>This method works in exactly
     * the manner specified by the {@link Files#getFileAttributeView} method.</p>
     *
     * @param path the path to the file.
     * @param type the {@code Class} object corresponding to the file attribute view
     * @param options options indicating how symbolic links are handled.
     * @param <V> The {@code FileAttributeView} type.
     * @return a file attribute view of the specified type, or {@code null}
     *         if the attribute view type is not available.
     * @see Files#getFileAttributeView(Path,Class,LinkOption...)
     */
    @Override
    public <V extends FileAttributeView> V getFileAttributeView(final Path path, final Class<V> type,
                                                                final LinkOption... options) {
        return null;
    }

    private static boolean isDirectory(final PathProperties properties) {
        final Map<String, String> metadata = properties.getMetadata();
        return metadata != null && Boolean.parseBoolean(metadata.get("hdi_isfolder"));
    }

    /**
     * Reads a file's attributes as a bulk operation.<p>This method works in exactly the manner specified by the
     * {@link Files#readAttributes(Path,Class,LinkOption...)} method except that the given path must be associated
     * with this provider and the only supported attribute type is {@link AzureDataLakeFileAttributes}. If the file
     * does not exist, a {@link NoSuchFileException} is thrown.</p>
     *
     * @param path the path to the file.
     * @param type the {@code Class} of the file attributes required to read.
     * @param options options indicating how symbolic links are handled.
     * @param <A> The {@code BasicFileAttributes} type.
     * @throws UnsupportedOperationException if the attribute type is not supported.
     * @throws NoSuchFileException if the file does not exist.
     * @throws IOException if an I/O error occurs.
     * @see Files#readAttributes(Path,Class,LinkOption...)
     * @return a file attributes of the specified type.
     */
    @Override
    @SuppressWarnings("unchecked")
    public <A extends BasicFileAttributes> A readAttributes(final Path path, final Class<A> type,
                                                            final LinkOption... options) throws IOException {
        final boolean isDirectory;
        final PathProperties props;
        final AzureDataLakePath adlsPath;
        if (!type.isAssignableFrom(AzureDataLakeFileAttributes.class)) {
            throw new UnsupportedOperationException("Unsupported attribute type: " + type);
        }
        // Cast the given path to an ADLS-specific path object
        adlsPath = AzureDataLakeFileSystemProvider.toAzureDataLakePath(path);
        // Check if the path refers to the root directory
        if (!adlsPath.isRoot()) {
            try {
                props = adlsPath.getFileClient().getProperties();
                isDirectory = AzureDataLakeFileSystemProvider.isDirectory(props);
                return (A) new AzureDataLakeFileAttributes(props, isDirectory);
            } catch (final DataLakeStorageException ex) {
                throw AzureDataLakeFileSystemProvider.toIOException(ex, path);
            }
        } else {
            // Return default attributes for the root directory, since it always exists
            return (A) new AzureDataLakeFileAttributes();
        }
    }

    /**
     * Reads a set of file attributes as a bulk operation. This method works in exactly the
     * manner specified by the {@link Files#readAttributes(Path,String,LinkOption[])} method.
     *
     * @param path the path to the file.
     * @param attributes the attributes to read
     * @param options options indicating how symbolic links are handled.
     * @return a map of the attributes returned; may be empty. The map's keys
     *         are the attribute names, its values are the attribute values.
     * @throws UnsupportedOperationException if the attribute view is not available.
     * @throws IllegalArgumentException if no attributes are specified or an unrecognized attributes is specified
     * @throws IOException If an I/O error occurs.
     * @see Files#readAttributes(Path,String,LinkOption[])
     */
    @Override
    public Map<String, Object> readAttributes(final Path path, final String attributes,
                                              final LinkOption... options) throws IOException {
        String attrList;
        final Map<String, Object> result;
        final AzureDataLakeFileAttributes attrs;
        attrs = this.readAttributes(path, AzureDataLakeFileAttributes.class, options);
        result = new HashMap<>();
        attrList = attributes;
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

    /**
     * Opens or creates a file, returning a seekable byte channel to access the
     * file. <p>This method works in exactly the manner specified by the {@link
     * Files#newByteChannel(Path,Set,FileAttribute[])} method.</p>
     *
     * @param path the path to the file to open or create.
     * @param options options specifying how the file is opened.
     * @param attrs an optional list of file attributes to set atomically when creating the file.
     * @throws IllegalArgumentException if the set contains an invalid combination of options.
     * @throws ProviderMismatchException if the given path is not an instance of {@link AzureDataLakePath}.
     * @throws UnsupportedOperationException if an unsupported open option is specified or the array contains
     *         attributes that cannot be set atomically when creating the file.
     * @throws FileAlreadyExistsException if a file of that name already exists and the {@link
     *         StandardOpenOption#CREATE_NEW CREATE_NEW} option is specified <i>(optional specific exception)</i>.
     * @throws ClosedFileSystemException If the underlying file system is closed.
     * @throws IOException if an I/O error occurs.
     * @return a new seekable byte channel.
     */
    @Override
    public SeekableByteChannel newByteChannel(final Path path, final Set<? extends OpenOption> options,
                                              final FileAttribute<?>... attrs) throws IOException {
        // Check if the options include the option to open a write channel
        if (options != null && options.contains(StandardOpenOption.WRITE)) {
            // Open an output stream for the specified path and return as SeekableByteChannel
            return new AzureSeekableByteChannel(this.newOutputStream(path, options), path);
        }
        // If options are provided, check that they only contain the read option; if not, raise an exception
        if (CoreUtils.isNullOrEmpty(options) || AzureDataLakeFileSystemProvider.INPUT_STREAM_OPTIONS.equals(options)) {
            // Open the input stream for the specified path using the ADLS client and return as SeekableByteChannel
            return new AzureSeekableByteChannel(AzureDataLakeFileSystemProvider.openInputStream(path), path);
        } else {
            // Raise an exception indicating that only the read option is supported
            throw new UnsupportedOperationException("Only the read option is supported.");
        }
    }

    /**
     * Sets the value of a file attribute. This method works in exactly the
     * manner specified by the {@link Files#setAttribute} method.
     *
     * @param path the path to the file.
     * @param attribute the attribute to set.
     * @param value the attribute value.
     * @param options options indicating how symbolic links are handled.
     * @throws UnsupportedOperationException if the attribute view is not available.
     * @throws IllegalArgumentException if the attribute name is not specified, or is not recognized, or
     *         the attribute value is of the correct type but has an inappropriate value
     * @throws ClassCastException If the attribute value is not of the expected type or
     *         is a collection containing elements that are not of the expected type.
     * @throws IOException If an I/O error occurs.
     */
    @Override
    public void setAttribute(final Path path, final String attribute, final Object value,
                             final LinkOption... options) throws IOException {
        throw new UnsupportedOperationException("setAttribute is not supported");
    }

    /**
     * Helper method to convert a URI to its root form (scheme + authority) for use as a key in the file system map.
     *
     * @param uri the URI to be converted.
     * @return a new URI containing only the scheme and authority of the given URI.
     * @throws ProviderMismatchException if the URI is invalid or does not have the expected scheme.
     */
    private static URI toRootURI(final URI uri) {
        // Ensure the URI has the correct scheme and authority before creating the root URI
        if (AzureDataLakeFileSystemProvider.SCHEME.equals(uri.getScheme())) {
            try {
                // Create a new URI with only the scheme and authority
                // (host + port) to serve as the root URI for the file system map
                return new URI(AzureDataLakeFileSystemProvider.SCHEME, null, uri.
                        getHost(), uri.getPort(), null, null, null);
            } catch (final Exception ex) {
                // This should never happen since we're only using valid components of the original URI,
                // but if it does, wrap it in an IllegalArgumentException with a clear message
                throw new IllegalArgumentException("Invalid URI: " + uri, ex);
            }
        } else {
            // Raise an exception if the URI scheme does not match the expected scheme for this provider
            throw new ProviderMismatchException("URI scheme does not match provider: " + uri);
        }
    }

    /**
     * Helper method to cast a generic path to {@link AzureDataLakePath}, ensuring type safety.
     *
     * @param path the path to be cast.
     * @return the given path cast to {@link AzureDataLakePath}.
     * @throws ProviderMismatchException if the given path is not an instance of {@link AzureDataLakePath}.
     */
    private static AzureDataLakePath toAzureDataLakePath(final Path path) {
        if (!(path instanceof AzureDataLakePath)) {
            throw new ProviderMismatchException("Expected AzureDataLakePath, got: " + path.getClass());
        }
        return (AzureDataLakePath) path;
    }

    /**
     * Returns the URI scheme that identifies this provider.
     *
     * @return URI scheme of this provider.
     */
    @Override
    public String getScheme() {
        return AzureDataLakeFileSystemProvider.SCHEME;
    }

    /**
     * Converts a {@link DataLakeStorageException} to an appropriate {@link IOException} based on the status code.
     *
     * @param ex the {@link DataLakeStorageException} to be converted.
     * @param path the {@link Path} associated with the exception.
     * @return an {@link IOException} that corresponds to the given exception.
     */
    private static IOException toIOException(final DataLakeStorageException ex, final Path path) {
        switch (ex.getStatusCode()) {
            case 404:
                return new NoSuchFileException(path.toString());
            case 409:
                return new FileAlreadyExistsException(path.toString());
            default:
                return new IOException("Azure storage error: " + ex.getMessage(), ex);
        }
    }

    /**
     * Creates a secure temporary file with permissions set to 600
     * (owner read/write) if supported by the underlying filesystem.
     *
     * @param prefix the prefix string to be used in generating the file's name.
     * @return the path to the created temporary file.
     * @throws IOException if an I/O error occurs or the temporary-file directory does not exist.
     */
    private static Path createSecureTempFile(final String prefix) throws IOException {
        final Set<PosixFilePermission> permissions;
        try {
            // Set permissions to 600 (owner read/write) if POSIX file attribute view is supported
            permissions = EnumSet.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE);
            // Create temporary file with specified permissions if the filesystem supports POSIX attributes
            return Files.createTempFile(prefix, null, PosixFilePermissions.asFileAttribute(permissions));
        } catch (final UnsupportedOperationException ignore) {
            // Non-POSIX filesystem (e.g., Windows) — fall back to default
            return Files.createTempFile(prefix, null);
        }
    }

    /** OutputStream backed by a temp file; uploads on close (overwrite). */
    private static final class UploadOutputStream extends OutputStream
    {
        private final Path tmpFile;
        private final AtomicBoolean closed;
        private final FileOutputStream tmpOut;
        private final DataLakeFileClient fileClient;

        private UploadOutputStream(final DataLakeFileClient fileClient) throws IOException {
            this.tmpFile = AzureDataLakeFileSystemProvider.createSecureTempFile("adls-upload-");
            this.tmpOut = new FileOutputStream(this.tmpFile.toFile());
            this.closed = new AtomicBoolean(false);
            this.fileClient = fileClient;
        }

        @Override
        public void write(final int b) throws IOException {
            this.tmpOut.write(b);
        }

        @Override
        public void write(final byte[] b, final int off, final int len) throws IOException {
            this.tmpOut.write(b, off, len);
        }

        @Override
        public void close() throws IOException {
            if (this.closed.compareAndSet(false, true)) {
                try {
                    this.tmpOut.close();
                    try {
                        this.fileClient.uploadFromFile(this.tmpFile.toAbsolutePath().toString(), true);
                    } catch (final DataLakeStorageException e) {
                        throw new IOException("Failed to upload data", e);
                    }
                } finally {
                    Files.deleteIfExists(this.tmpFile);
                }
            }
        }
    }

    /** OutputStream backed by a temp file; appends content on close. */
    private static final class AppendOutputStream extends OutputStream
    {
        private final Path tmpFile;
        private final AtomicBoolean closed;
        private final FileOutputStream tmpOut;
        private final DataLakeFileClient fileClient;

        private AppendOutputStream(final DataLakeFileClient fileClient) throws IOException {
            this.tmpFile = AzureDataLakeFileSystemProvider.createSecureTempFile("adls-append-");
            this.tmpOut = new FileOutputStream(this.tmpFile.toFile());
            this.closed = new AtomicBoolean(false);
            this.fileClient = fileClient;
        }

        @Override
        public void write(int b) throws IOException {
            this.tmpOut.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            this.tmpOut.write(b, off, len);
        }

        @Override
        public void close() throws IOException {
            if (this.closed.compareAndSet(false, true)) {
                try {
                    this.tmpOut.close();
                    long dataLen = Files.size(this.tmpFile);
                    long currentSize = 0;
                    try {
                        currentSize = this.fileClient.getProperties().getFileSize();
                    } catch (DataLakeStorageException e) {
                        if (e.getStatusCode() != 404) {
                            throw new IOException("Failed to get file properties", e);
                        }
                        // File doesn't exist yet — start from offset 0
                    }
                    try (final FileInputStream fis = new FileInputStream(this.tmpFile.toFile())) {
                        this.fileClient.append(fis, currentSize, dataLen);
                    }
                    try {
                        this.fileClient.flush(currentSize + dataLen, false);
                    } catch (final DataLakeStorageException e) {
                        throw new IOException("Failed to flush append data", e);
                    }
                } finally {
                    Files.deleteIfExists(this.tmpFile);
                }
            }
        }
    }

    static {
        INPUT_STREAM_OPTIONS = Collections.singleton(StandardOpenOption.READ);
        OUTPUT_STREAM_OPTIONS = Collections.unmodifiableSet(EnumSet.of(StandardOpenOption.
                CREATE_NEW, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.
                APPEND, StandardOpenOption.TRUNCATE_EXISTING));
        OUTPUT_STREAM_DEFAULT_OPTIONS = Collections.unmodifiableSet(EnumSet.of(StandardOpenOption.
                CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING));
    }
}
