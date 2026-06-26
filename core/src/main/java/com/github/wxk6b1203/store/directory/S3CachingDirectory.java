package com.github.wxk6b1203.store.directory;

import com.github.wxk6b1203.metadata.common.CommittingIndexFile;
import com.github.wxk6b1203.metadata.common.IndexCommitSnapshot;
import com.github.wxk6b1203.metadata.common.IndexFileMetadata;
import com.github.wxk6b1203.metadata.common.IndexFileStatus;
import com.github.wxk6b1203.store.common.FileChecksums;
import com.github.wxk6b1203.store.common.PathUtil;
import com.github.wxk6b1203.store.manifest.ManifestManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.BaseDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.store.NoLockFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * A Directory implementation that uses S3 as the underlying storage.
 */
@Slf4j
public class S3CachingDirectory extends BaseDirectory {
    private static final long SLOW_LOCAL_SYNC_WARN_NANOS = 1_000_000_000L;

    private final Path basePath;
    private final String indexName;
    private final ManifestManager manifestManager;
    private final AtomicLong nextTempFileCounter = new AtomicLong();
    private final Path walDataPath;
    private final Path sharedDataPath;
    private final Path sharedTempPath;
    private final NIOFSDirectory walDirectory;
    private final NIOFSDirectory sharedDirectory;
    private final List<IndexFileStatus> readableRemoteStatuses;
    private final boolean includeWalFiles;
    private final boolean fixedRemoteSnapshotSelection;
    private final IndexCommitSnapshot remoteSnapshot;
    private final Map<String, IndexFileMetadata> remoteSnapshotFiles;
    private final Set<String> syncedFiles = new ConcurrentSkipListSet<>();
    private final Set<String> pendingLocalDeletes = new ConcurrentSkipListSet<>();
    private final Set<String> deletedRemoteFiles = new ConcurrentSkipListSet<>();
    private final Map<String, Object> cacheLocks = new ConcurrentHashMap<>();
    private String snapshotPinId;

    public S3CachingDirectory(
            S3DirectoryOptions options,
            LockFactory lockFactory,
            ManifestManager manifestManager
    ) throws IOException {
        this(options, lockFactory, manifestManager, List.of(
                IndexFileStatus.DIRTY,
                IndexFileStatus.UPLOADING,
                IndexFileStatus.CLEAN,
                IndexFileStatus.PINNED
        ), true);
    }

    public S3CachingDirectory(
            S3DirectoryOptions options,
            LockFactory lockFactory,
            ManifestManager manifestManager,
            List<IndexFileStatus> readableRemoteStatuses
    ) throws IOException {
        this(options, lockFactory, manifestManager, readableRemoteStatuses, true);
    }

    public S3CachingDirectory(
            S3DirectoryOptions options,
            LockFactory lockFactory,
            ManifestManager manifestManager,
            List<IndexFileStatus> readableRemoteStatuses,
            boolean includeWalFiles
    ) throws IOException {
        this(options, lockFactory, manifestManager, readableRemoteStatuses, includeWalFiles, null);
    }

    public S3CachingDirectory(
            S3DirectoryOptions options,
            LockFactory lockFactory,
            ManifestManager manifestManager,
            List<IndexFileStatus> readableRemoteStatuses,
            boolean includeWalFiles,
            Long remoteSnapshotGeneration
    ) throws IOException {
        super(lockFactory);
        this.basePath = options.basePath();
        this.indexName = options.indexName();
        this.manifestManager = manifestManager;
        this.readableRemoteStatuses = List.copyOf(readableRemoteStatuses);
        this.includeWalFiles = includeWalFiles;
        this.fixedRemoteSnapshotSelection = remoteSnapshotGeneration != null;
        this.remoteSnapshot = remoteSnapshotGeneration == null
                ? manifestManager.latestSnapshot(indexName)
                : remoteSnapshotGeneration < 0
                ? null
                : manifestManager.snapshot(indexName, remoteSnapshotGeneration);
        this.remoteSnapshotFiles = snapshotFiles(remoteSnapshot);
        this.walDataPath = PathUtil.walDataPath(basePath, indexName);
        this.sharedDataPath = PathUtil.sharedDataPath(basePath, indexName);
        this.sharedTempPath = PathUtil.sharedTempPath(basePath, indexName);
        Files.createDirectories(walDataPath);
        Files.createDirectories(sharedDataPath);
        Files.createDirectories(sharedTempPath);
        this.walDirectory = new NIOFSDirectory(walDataPath, NoLockFactory.INSTANCE);
        this.sharedDirectory = new NIOFSDirectory(sharedDataPath, NoLockFactory.INSTANCE);
    }

    @Override
    public String[] listAll() throws IOException {
        ensureOpen();
        Set<String> names = new HashSet<>();
        if (includeWalFiles) {
            names.addAll(List.of(walDirectory.listAll()));
        }
        if (remoteSnapshot != null) {
            remoteSnapshotFiles.keySet().stream()
                    .filter(name -> !deletedRemoteFiles.contains(name))
                    .forEach(names::add);
            return names.toArray(String[]::new);
        }
        if (fixedRemoteSnapshotSelection) {
            return names.toArray(String[]::new);
        }
        try {
            manifestManager.listAll(indexName, readableRemoteStatuses)
                    .stream()
                    .map(IndexFileMetadata::getName)
                    .filter(name -> !deletedRemoteFiles.contains(name))
                    .forEach(names::add);
            return names.toArray(String[]::new);
        } catch (Exception e) {
            throw new IOException("Failed to list objects from S3", e);
        }
    }

    @Override
    public void deleteFile(String name) throws IOException {
        ensureOpen();
        syncedFiles.remove(name);
        if (knownRemoteFile(name)) {
            deletedRemoteFiles.add(name);
        }
        deleteLocalIfExists(name);
    }

    @Override
    public long fileLength(String name) throws IOException {
        ensureOpen();
        if (walFileAvailable(name)) {
            return Files.size(walDataPath.resolve(name));
        }
        if (deletedRemoteFiles.contains(name)) {
            throw new NoSuchFileException(name);
        }
        IndexFileMetadata snapshotFile = remoteSnapshotFiles.get(name);
        if (snapshotFile != null && sharedCacheAvailable(name)) {
            return Files.size(sharedDataPath.resolve(name));
        }
        if (snapshotFile != null) {
            return snapshotFile.getSize();
        }
        Path sharedFile = sharedDataPath.resolve(name);
        if (sharedCacheAvailable(name)) {
            return Files.size(sharedFile);
        }
        IndexFileMetadata metadata = readableRemoteFileMetadata(name);
        return metadata.getSize();
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        ensureOpen();
        deletedRemoteFiles.remove(name);
        return walDirectory.createOutput(name, context);
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
        ensureOpen();
        String name = getTempFileName(prefix, suffix, nextTempFileCounter.getAndIncrement());
        return walDirectory.createOutput(name, context);
    }

    @Override
    public void sync(Collection<String> names) throws IOException {
        ensureOpen();
        List<String> localNames = new ArrayList<>();
        for (String name : names) {
            Path filePath = walDataPath.resolve(name);
            if (Files.exists(filePath)) {
                syncedFiles.add(name);
                localNames.add(name);
            }
        }
        long started = System.nanoTime();
        walDirectory.sync(localNames);
        warnSlowSync("sync", started, localNames);
    }

    /**
     * @see IndexWriter#finishCommit()
     * @throws IOException
     */
    @Override
    public void syncMetaData() throws IOException {
        ensureOpen();
        long started = System.nanoTime();
        walDirectory.syncMetaData();
        warnSlowSync("syncMetaData", started, List.of());
        syncedFiles.clear();
    }

    public CompletableFuture<Boolean> publishLocalCommit() throws IOException {
        ensureOpen();
        List<String> visibleFiles = List.of(listAll());
        if (visibleFiles.stream().noneMatch(this::isCommittedSegmentFile)) {
            return CompletableFuture.completedFuture(true);
        }
        return publishLocalFiles(currentCommitFileNames(visibleFiles));
    }

    public CompletableFuture<Boolean> publishIndexCommit(IndexCommit commit) throws IOException {
        ensureOpen();
        return publishLocalFiles(commit.getFileNames());
    }

    private CompletableFuture<Boolean> publishLocalFiles(Collection<String> snapshotFileNames) throws IOException {
        List<CommittingIndexFile> files = new ArrayList<>();
        for (String name : snapshotFileNames) {
            if (shouldPublish(name) && shouldCommitToManifest(name) && Files.exists(walDataPath.resolve(name))) {
                files.add(new CommittingIndexFile(indexName, walDataPath.resolve(name)));
            }
        }
        if (!files.isEmpty()) {
            return manifestManager.commit(files, snapshotFileNames);
        }
        return CompletableFuture.completedFuture(true);
    }

    private Set<String> currentCommitFileNames(Collection<String> publishCandidates) throws IOException {
        if (publishCandidates.stream().noneMatch(this::isCommittedSegmentFile)) {
            return Set.of();
        }
        return SegmentInfos.readLatestCommit(this)
                .files(true)
                .stream()
                .filter(this::shouldPublish)
                .collect(Collectors.toCollection(HashSet::new));
    }

    private void warnSlowSync(String operation, long started, Collection<String> names) {
        long elapsedNanos = System.nanoTime() - started;
        if (elapsedNanos < SLOW_LOCAL_SYNC_WARN_NANOS) {
            return;
        }
        log.warn(
                "slow WAL {} for {} took {}ms, files={}, sample={}",
                operation,
                indexName,
                elapsedNanos / 1_000_000,
                names.size(),
                names.stream().limit(20).toList()
        );
    }

    @Override
    public void rename(String source, String dest) throws IOException {
        ensureOpen();
        walDirectory.rename(source, dest);
        deletedRemoteFiles.remove(dest);
        if (syncedFiles.remove(source) || isCommittedSegmentFile(dest)) {
            syncedFiles.add(dest);
        }
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        ensureOpen();
        if (walFileAvailable(name)) {
            return walDirectory.openInput(name, context);
        }
        if (sharedCacheAvailable(name)) {
            return sharedDirectory.openInput(name, context);
        }
        cacheRemoteFile(name);
        if (walFileAvailable(name)) {
            return walDirectory.openInput(name, context);
        }
        if (sharedCacheAvailable(name)) {
            return sharedDirectory.openInput(name, context);
        }
        throw new java.nio.file.NoSuchFileException(name);
    }

    @Override
    public void close() throws IOException {
        isOpen = false;
        IOException failure = null;
        try {
            manifestManager.close();
        } catch (Exception e) {
            failure = e instanceof IOException ioException ? ioException : new IOException(e);
        }
        try {
            releaseSnapshotPin();
        } catch (Exception e) {
            failure = addFailure(failure, e instanceof IOException ioException ? ioException : new IOException(e));
        }
        try {
            walDirectory.close();
        } catch (IOException e) {
            if (failure == null) {
                failure = e;
            } else {
                failure.addSuppressed(e);
            }
        }
        try {
            sharedDirectory.close();
        } catch (IOException e) {
            if (failure == null) {
                failure = e;
            } else {
                failure.addSuppressed(e);
            }
        }
        try {
            retryPendingLocalDeletes();
        } catch (IOException e) {
            if (failure == null) {
                failure = e;
            } else {
                failure.addSuppressed(e);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    public Long remoteSnapshotGeneration() {
        return remoteSnapshot == null ? null : remoteSnapshot.getGeneration();
    }

    public void pinRemoteSnapshot(String pinId, java.time.Instant expiresAt) {
        ensureOpen();
        if (remoteSnapshot == null || pinId == null || pinId.isBlank()) {
            return;
        }
        releaseSnapshotPin();
        this.snapshotPinId = pinId;
        manifestManager.pinSnapshot(indexName, remoteSnapshot.getGeneration(), pinId, expiresAt.toEpochMilli());
    }

    @Override
    public Set<String> getPendingDeletions() throws IOException {
        return walDirectory.getPendingDeletions();
    }

    private void cacheRemoteFile(String name) throws IOException {
        Object lock = cacheLocks.computeIfAbsent(name, ignored -> new Object());
        synchronized (lock) {
            try {
                if (localFileAvailable(name)) {
                    return;
                }
                RemoteCacheStats.miss();
                IndexFileMetadata metadata = readableRemoteFileMetadata(name);
                Path temp = sharedTempPath.resolve(name + "." + Thread.currentThread().threadId() + ".tmp");
                try {
                    manifestManager.download(metadata, temp);
                    RemoteCacheStats.download();
                    if (localFileAvailable(name)) {
                        return;
                    }
                    moveIntoCache(temp, sharedDataPath.resolve(name), metadata);
                } finally {
                    Files.deleteIfExists(temp);
                }
            } finally {
                // Best-effort cleanup: only remove the lock if no other thread is waiting on it.
                cacheLocks.remove(name, lock);
            }
        }
    }

    private void moveIntoCache(Path source, Path target, IndexFileMetadata metadata) throws IOException {
        try {
            Files.move(source, target, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            if (sharedCacheAvailable(metadata.getName())) {
                return;
            }
            Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private IndexFileMetadata readableRemoteFileMetadata(String name) throws IOException {
        if (deletedRemoteFiles.contains(name)) {
            throw new java.nio.file.NoSuchFileException(name);
        }
        IndexFileMetadata snapshotFile = remoteSnapshotFiles.get(name);
        if (snapshotFile != null) {
            return snapshotFile;
        }
        if (remoteSnapshot != null) {
            throw new java.nio.file.NoSuchFileException(name);
        }
        if (fixedRemoteSnapshotSelection) {
            throw new java.nio.file.NoSuchFileException(name);
        }
        IndexFileMetadata metadata = manifestManager.fileMetadata(indexName, name);
        if (!readableRemoteStatuses.contains(metadata.getStatus())) {
            throw new java.nio.file.NoSuchFileException(name);
        }
        return metadata;
    }

    private boolean localFileAvailable(String name) throws IOException {
        return walFileAvailable(name) || sharedCacheAvailable(name);
    }

    private boolean walFileAvailable(String name) {
        return includeWalFiles && Files.exists(walDataPath.resolve(name));
    }

    private boolean sharedCacheAvailable(String name) throws IOException {
        Path sharedFile = sharedDataPath.resolve(name);
        if (!Files.exists(sharedFile)) {
            return false;
        }
        IndexFileMetadata metadata;
        try {
            metadata = readableRemoteFileMetadata(name);
        } catch (NoSuchFileException e) {
            return false;
        }
        if (cacheMatches(sharedFile, metadata)) {
            RemoteCacheStats.hit();
            try {
                Files.setLastModifiedTime(sharedFile, FileTime.fromMillis(System.currentTimeMillis()));
            } catch (IOException ignored) {
            }
            return true;
        }
        RemoteCacheStats.corruption();
        deleteIfExists(sharedFile);
        RemoteCacheStats.miss();
        return false;
    }

    private boolean cacheMatches(Path sharedFile, IndexFileMetadata metadata) throws IOException {
        if (Files.size(sharedFile) != metadata.getSize()) {
            return false;
        }
        return metadata.getChecksum() == 0 || FileChecksums.crc32(sharedFile) == metadata.getChecksum();
    }

    private Map<String, IndexFileMetadata> snapshotFiles(IndexCommitSnapshot snapshot) {
        if (snapshot == null || snapshot.getFiles() == null || snapshot.getFiles().isEmpty()) {
            return Map.of();
        }
        Map<String, IndexFileMetadata> files = new ConcurrentHashMap<>();
        for (IndexFileMetadata file : snapshot.getFiles()) {
            files.put(file.getName(), file);
        }
        return files;
    }

    private boolean knownRemoteFile(String name) throws IOException {
        if (remoteSnapshotFiles.containsKey(name)) {
            return true;
        }
        try {
            manifestManager.fileMetadata(indexName, name);
            return true;
        } catch (NoSuchFileException e) {
            return false;
        }
    }

    private void releaseSnapshotPin() {
        if (snapshotPinId != null && remoteSnapshot != null) {
            manifestManager.releaseSnapshotPin(indexName, snapshotPinId);
            snapshotPinId = null;
        }
    }

    private IOException addFailure(IOException failure, IOException next) {
        if (failure == null) {
            return next;
        }
        failure.addSuppressed(next);
        return failure;
    }

    private void deleteLocalIfExists(String name) throws IOException {
        boolean deleted = true;
        deleted &= deleteIfExists(walDataPath.resolve(name));
        deleted &= deleteIfExists(sharedDataPath.resolve(name));
        if (deleted) {
            pendingLocalDeletes.remove(name);
        } else {
            pendingLocalDeletes.add(name);
        }
    }

    private boolean deleteIfExists(Path path) throws IOException {
        if (!Files.exists(path)) {
            return true;
        }
        try {
            Files.deleteIfExists(path);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    private void retryPendingLocalDeletes() throws IOException {
        IOException failure = null;
        for (String name : List.copyOf(pendingLocalDeletes)) {
            try {
                deleteLocalIfExists(name);
            } catch (IOException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private boolean shouldPublish(String name) {
        return !name.startsWith("pending_segments_") && !name.equals("write.lock");
    }

    private boolean shouldCommitToManifest(String name) throws IOException {
        try {
            IndexFileMetadata metadata = manifestManager.fileMetadata(indexName, name);
            return metadata.getStatus() == IndexFileStatus.DIRTY
                    || metadata.getStatus() == IndexFileStatus.UPLOADING;
        } catch (java.nio.file.NoSuchFileException e) {
            return true;
        }
    }

    private boolean isCommittedSegmentFile(String name) {
        return name.startsWith("segments_") && !name.startsWith("pending_segments_");
    }
}
