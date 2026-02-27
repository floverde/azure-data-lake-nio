package com.github.floverde.azure.datalake.nio;

import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;

import java.io.IOException;
import java.net.URI;
import java.nio.file.*;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.nio.file.spi.FileSystemProvider;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public class AzureDataLakeFileSystem extends FileSystem {

    private final AzureDataLakeFileSystemProvider provider;
    private final DataLakeServiceClient serviceClient;
    private final Map<String, DataLakeFileSystemClient> containerClients = new ConcurrentHashMap<>();
    private final URI rootUri;
    private volatile boolean open = true;

    AzureDataLakeFileSystem(AzureDataLakeFileSystemProvider provider,
                             DataLakeServiceClient serviceClient,
                             URI rootUri) {
        this.provider = provider;
        this.serviceClient = serviceClient;
        this.rootUri = rootUri;
    }

    DataLakeFileSystemClient getFileSystemClient(String containerName) {
        return containerClients.computeIfAbsent(containerName, serviceClient::getFileSystemClient);
    }

    URI getRootUri() {
        return rootUri;
    }

    @Override
    public FileSystemProvider provider() {
        return provider;
    }

    @Override
    public void close() throws IOException {
        if (open) {
            open = false;
            provider.removeFileSystem(rootUri);
        }
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public String getSeparator() {
        return "/";
    }

    @Override
    public Iterable<Path> getRootDirectories() {
        List<Path> roots = new ArrayList<>();
        String host = rootUri.getHost();
        for (String containerName : containerClients.keySet()) {
            String authority = containerName + "@" + host;
            roots.add(new AzureDataLakePath(this, authority, "/"));
        }
        return roots;
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
    public Path getPath(String first, String... more) {
        StringBuilder sb = new StringBuilder(first);
        for (String s : more) {
            if (sb.length() > 0 && !sb.toString().endsWith("/") && !s.startsWith("/")) {
                sb.append('/');
            }
            sb.append(s);
        }
        return new AzureDataLakePath(this, sb.toString());
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
}
