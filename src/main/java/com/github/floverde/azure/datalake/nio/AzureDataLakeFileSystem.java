package com.github.floverde.azure.datalake.nio;

import java.net.URI;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.PatternSyntaxException;
import java.util.regex.Pattern;
import java.util.Collections;
import java.util.Objects;
import java.nio.file.*;
import java.util.Set;

public abstract class AzureDataLakeFileSystem extends FileSystem
{
    private static final String SEPARATOR = "/";

    protected final AzureDataLakeFileSystemProvider provider;

    protected AzureDataLakeFileSystem(final AzureDataLakeFileSystemProvider provider) {
        this.provider = Objects.requireNonNull(provider, "provider must not be null");
    }

    @Override
    public final AzureDataLakeFileSystemProvider provider() {
        return this.provider;
    }

    @Override
    public final String getSeparator() {
        return AzureDataLakeFileSystem.SEPARATOR;
    }

    @Override
    public Iterable<FileStore> getFileStores() {
        return Collections.emptyList();
    }

    @Override
    public Set<String> supportedFileAttributeViews() {
        return Collections.singleton("basic");
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public PathMatcher getPathMatcher(String syntaxAndPattern) {
        int colonIndex = syntaxAndPattern.indexOf(':');
        if (colonIndex <= 0) {
            throw new IllegalArgumentException("Invalid syntaxAndPattern: " + syntaxAndPattern);
        }
        String syntax = syntaxAndPattern.substring(0, colonIndex);
        String pattern = syntaxAndPattern.substring(colonIndex + 1);

        Pattern regexPattern;
        if ("glob".equalsIgnoreCase(syntax)) {
            regexPattern = Pattern.compile(globToRegex(pattern));
        } else if ("regex".equalsIgnoreCase(syntax)) {
            try {
                regexPattern = Pattern.compile(pattern);
            } catch (PatternSyntaxException e) {
                throw new PatternSyntaxException(e.getDescription(), e.getPattern(), e.getIndex());
            }
        } else {
            throw new UnsupportedOperationException("Syntax not supported: " + syntax);
        }

        Pattern finalPattern = regexPattern;
        return path -> finalPattern.matcher(path.toString()).matches();
    }

    private static String globToRegex(String glob) {
        StringBuilder sb = new StringBuilder("^");
        for (int i = 0; i < glob.length(); i++) {
            char c = glob.charAt(i);
            switch (c) {
                case '*':
                    if (i + 1 < glob.length() && glob.charAt(i + 1) == '*') {
                        sb.append(".*");
                        i++;
                    } else {
                        sb.append("[^/]*");
                    }
                    break;
                case '?':
                    sb.append("[^/]");
                    break;
                case '{':
                    sb.append("(?:");
                    break;
                case '}':
                    sb.append(")");
                    break;
                case ',':
                    sb.append("|");
                    break;
                case '.':
                case '(':
                case ')':
                case '+':
                case '|':
                case '^':
                case '$':
                case '@':
                case '%':
                case '[':
                case ']':
                case '\\':
                    sb.append('\\').append(c);
                    break;
                default:
                    sb.append(c);
            }
        }
        sb.append("$");
        return sb.toString();
    }

    @Override
    public UserPrincipalLookupService getUserPrincipalLookupService() {
        throw new UnsupportedOperationException("UserPrincipalLookupService is not supported");
    }

    @Override
    public WatchService newWatchService() {
        throw new UnsupportedOperationException("WatchService is not supported");
    }

    public abstract URI getRootURI();
}
