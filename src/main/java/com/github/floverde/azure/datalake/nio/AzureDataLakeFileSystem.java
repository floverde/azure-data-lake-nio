package com.github.floverde.azure.datalake.nio;

import com.github.floverde.azure.datalake.nio.matchers.GlobPathMatcher;
import com.github.floverde.azure.datalake.nio.matchers.RegexPathMatcher;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.util.Collections;
import java.util.Objects;
import java.nio.file.*;
import java.util.Set;
import java.net.URI;

/**
 * Abstract base class for Azure Data Lake Storage Gen2 file systems.
 * <p>This class implements the common functionality shared between account-level
 * ({@link ADLSAccountFileSystem}) and container-level ({@link ADLSContainerFileSystem})
 * file system representations.</p>
 * <p>The file system uses {@code "/"} as the path separator and supports only the
 * {@code "basic"} file attribute view. Write operations are supported; the file system
 * is never read-only.</p>
 * <p>Path matchers with both {@code glob} and {@code regex} syntax are supported.
 * Neither {@link WatchService} nor {@link UserPrincipalLookupService} are supported.</p>
 *
 * @see ADLSAccountFileSystem
 * @see ADLSContainerFileSystem
 */
public abstract class AzureDataLakeFileSystem extends FileSystem
{
    private static final String SEPARATOR = "/";

    protected final AzureDataLakeFileSystemProvider provider;

    /**
     * Creates a new file system associated with the given provider.
     *
     * @param provider the provider that created this file system; must not be {@code null}.
     * @throws NullPointerException if {@code provider} is {@code null}.
     */
    protected AzureDataLakeFileSystem(final AzureDataLakeFileSystemProvider provider) {
        this.provider = Objects.requireNonNull(provider, "provider must not be null");
    }

    /**
     * Returns the provider that created this file system.
     *
     * @return the {@link AzureDataLakeFileSystemProvider} that created this file system.
     */
    @Override
    public final AzureDataLakeFileSystemProvider provider() {
        return this.provider;
    }

    /**
     * Returns the name separator for this file system, which is {@code "/"}.
     *
     * @return {@code "/"}.
     */
    @Override
    public final String getSeparator() {
        return AzureDataLakeFileSystem.SEPARATOR;
    }

    /**
     * Returns an empty iterable since Azure Data Lake Storage Gen2 does not
     * expose file stores through this API.
     *
     * @return an empty {@link Iterable}.
     */
    @Override
    public Iterable<FileStore> getFileStores() {
        return Collections.emptyList();
    }

    /**
     * Returns a singleton set containing {@code "basic"}, which is the only
     * supported file attribute view for Azure Data Lake Storage Gen2.
     *
     * @return an immutable set containing {@code "basic"}.
     */
    @Override
    public Set<String> supportedFileAttributeViews() {
        return Collections.singleton("basic");
    }

    /**
     * Returns {@code false} since Azure Data Lake Storage Gen2 supports write operations.
     *
     * @return {@code false}.
     */
    @Override
    public boolean isReadOnly() {
        return false;
    }

    /**
     * Returns a {@link PathMatcher} that matches paths against the given pattern.
     * <p>The pattern must be in the form {@code syntax:pattern}, where {@code syntax}
     * is either {@code "glob"} or {@code "regex"} (case-insensitive).</p>
     * <p>Glob patterns support {@code *} (matches any sequence of characters within
     * a path element), {@code **} (matches any sequence across path element boundaries),
     * and {@code ?} (matches a single character).</p>
     *
     * @param syntaxAndPattern the pattern in the form {@code syntax:pattern}.
     * @throws IllegalArgumentException if the pattern does not contain a colon separator.
     * @throws UnsupportedOperationException if the syntax is not {@code "glob"} or {@code "regex"}.
     * @throws java.util.regex.PatternSyntaxException if the pattern is an invalid regular expression.
     * @return a {@link PathMatcher} for the given pattern.
     */
    @Override
    public PathMatcher getPathMatcher(final String syntaxAndPattern) {
        final String syntax, pattern;
        final int colonIndex = syntaxAndPattern.indexOf(':');
        if (colonIndex > 0) {
            syntax = syntaxAndPattern.substring(0, colonIndex);
            pattern = syntaxAndPattern.substring(colonIndex + 1);
            if ("glob".equalsIgnoreCase(syntax)) {
                return new GlobPathMatcher(pattern);
            }
            if ("regex".equalsIgnoreCase(syntax)) {
                return new RegexPathMatcher(pattern);
            }
            throw new UnsupportedOperationException("Syntax not supported: " + syntax);
        } else {
            throw new IllegalArgumentException("Invalid syntaxAndPattern: " + syntaxAndPattern);
        }
    }

    /**
     * Throws {@link UnsupportedOperationException} because
     * {@link java.nio.file.attribute.UserPrincipalLookupService} is not supported.
     *
     * @throws UnsupportedOperationException always.
     */
    @Override
    public UserPrincipalLookupService getUserPrincipalLookupService() {
        throw new UnsupportedOperationException("UserPrincipalLookupService is not supported");
    }

    /**
     * Throws {@link UnsupportedOperationException} because
     * {@link WatchService} is not supported.
     *
     * @throws UnsupportedOperationException always.
     */
    @Override
    public WatchService newWatchService() {
        throw new UnsupportedOperationException("WatchService is not supported");
    }

    /**
     * Returns the root URI for this file system.
     * <p>For account-level file systems, this is {@code abfss://account.dfs.core.windows.net}.
     * For container-level file systems, this is
     * {@code abfss://container@account.dfs.core.windows.net}.</p>
     *
     * @return the root {@link URI} for this file system.
     */
    public abstract URI getRootURI();
}
