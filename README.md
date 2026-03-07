# Azure Data Lake Storage Gen2 NIO.2 file system

A Java NIO.2 `FileSystemProvider` for **Azure Data Lake Storage Gen2** (ADLS Gen2),
enabling standard `java.nio.file` API access to ADLS Gen2 storage.

## Features

- Full Java NIO.2 `FileSystem` / `Path` / `FileSystemProvider` integration.
- Account-level and container-level file system hierarchy.
- Supports reading, writing (create/overwrite), appending, deleting, copying, moving, and listing directory entries.
- `SeekableByteChannel` support for random-read access.
- Lazy directory listing via `DirectoryStream`.
- Authentication via account key, SAS token, service principal, managed identity, or any `TokenCredential`.
- URI scheme: `abfss://`.

## Requirements

- Java 11 or later
- Maven 3.x

## URI Format

```
abfss://<container>@<account>.dfs.core.windows.net/<path>
```

- `<container>` — the ADLS Gen2 container (file system) name.
- `<account>` — the storage account name.
- `<path>` — the path within the container (optional).

**Account-level URI** (no container specified):
```
abfss://<account>.dfs.core.windows.net
```

## Usage

### Opening a File System

Authentication credentials are provided through the environment map passed to `FileSystems.newFileSystem()`.
The following authentication mechanisms are supported, evaluated in priority order:

#### 1. Account Key (Shared Key)

```java
URI uri = URI.create("abfss://mycontainer@myaccount.dfs.core.windows.net");
Map<String, String> env = Map.of("azure.account.key", "<base64-encoded-key>");
FileSystem fs = FileSystems.newFileSystem(uri, env);
```

#### 2. SAS Token

```java
Map<String, String> env = Map.of("azure.sas.token", "<sas-token>");
FileSystem fs = FileSystems.newFileSystem(uri, env);
```

#### 3. Pre-built `TokenCredential`

Pass any `com.azure.core.credential.TokenCredential` instance (e.g. from `azure-identity`):

```java
TokenCredential credential = new DefaultAzureCredentialBuilder().build();
Map<String, Object> env = Map.of("azure.credential", credential);
FileSystem fs = FileSystems.newFileSystem(uri, env);
```

#### 4. Service Principal (Client Secret)

```java
Map<String, String> env = Map.of(
    "azure.client.id",     "<application-client-id>",
    "azure.client.secret", "<client-secret>",
    "azure.tenant.id",     "<tenant-id>"
);
FileSystem fs = FileSystems.newFileSystem(uri, env);
```

#### 5. User-Assigned Managed Identity

```java
Map<String, String> env = Map.of("azure.managed.identity.client.id", "<managed-identity-client-id>");
FileSystem fs = FileSystems.newFileSystem(uri, env);
```

#### 6. System-Assigned Managed Identity

```java
Map<String, String> env = Map.of("azure.managed.identity.auto", "true");
FileSystem fs = FileSystems.newFileSystem(uri, env);
```

### Working with Paths

```java
// Get a path within the container file system
Path file = fs.getPath("/data/input.csv");

// Or use the provider directly
Path file = Path.of(URI.create("abfss://mycontainer@myaccount.dfs.core.windows.net/data/input.csv"));
```

### Reading a File

```java
try (InputStream in = Files.newInputStream(file)) {
    // read data
}

// Or with a seekable byte channel
try (SeekableByteChannel ch = Files.newByteChannel(file, StandardOpenOption.READ)) {
    ByteBuffer buf = ByteBuffer.allocate(4096);
    ch.read(buf);
}
```

### Writing a File

```java
// Overwrite (create or truncate)
try (OutputStream out = Files.newOutputStream(file, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
    out.write("hello".getBytes());
}

// Append
try (OutputStream out = Files.newOutputStream(file, StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
    out.write(" world".getBytes());
}
```

### Listing a Directory

```java
try (DirectoryStream<Path> stream = Files.newDirectoryStream(fs.getPath("/data"))) {
    for (Path entry : stream) {
        System.out.println(entry);
    }
}
```

### Creating and Deleting Directories

```java
Files.createDirectory(fs.getPath("/data/output"));
Files.delete(fs.getPath("/data/old-file.csv"));
```

### Reading File Attributes

```java
BasicFileAttributes attrs = Files.readAttributes(file, BasicFileAttributes.class);
System.out.println(attrs.size());
System.out.println(attrs.isDirectory());
System.out.println(attrs.lastModifiedTime());
```

### Closing the File System

```java
fs.close();
```

## Project Structure

| Class                             | Description                                                                           |
|-----------------------------------|---------------------------------------------------------------------------------------|
| `AzureDataLakeFileSystemProvider` | The NIO.2 `FileSystemProvider` (URI scheme `abfss`). Entry point for file operations. |
| `ADLSAccountFileSystem`           | Account-level `FileSystem`. Manages container file systems.                           |
| `ADLSContainerFileSystem`         | Container-level `FileSystem`. Used for all path and I/O operations.                   |
| `ADLSConfigurationReader`         | Utility for reading configuration and credentials from the environment map.           |
| `AzureDataLakePath`               | `Path` implementation backed by an ADLS Gen2 container.                               |
| `AzureDataLakeFileAttributes`     | `BasicFileAttributes` implementation.                                                 |
| `AzureDataLakeDirectoryStream`    | Lazy `DirectoryStream` implementation using the ADLS SDK.                             |
| `AzureSeekableByteChannel`        | `SeekableByteChannel` for reading blobs; write-only channels supported for upload.    |

## Building

```bash
mvn clean package
```

## Testing

```bash
mvn test
```

## Limitations

- `WatchService` is not supported.
- `UserPrincipalLookupService` is not supported.
- `FileStore` is not supported.
- Truncation of existing files is not supported.
- Read/write mode channels are not supported (open a channel for reading **or** writing, not both).
- `toFile()` on a path is not supported.
