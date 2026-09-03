package com.github.wxk6b1203.store.manifest;

import com.github.wxk6b1203.metadata.common.CommittingIndexFile;
import com.github.wxk6b1203.metadata.common.IndexCommitSnapshot;
import com.github.wxk6b1203.metadata.common.IndexCommitSnapshotPin;
import com.github.wxk6b1203.metadata.common.IndexFile;
import com.github.wxk6b1203.metadata.common.IndexFileMetadata;
import com.github.wxk6b1203.metadata.common.IndexFileStatus;
import com.github.wxk6b1203.metadata.provider.ManifestMetadataManager;
import com.github.wxk6b1203.metadata.provider.ManifestMetadataManager.FileVersion;
import com.github.wxk6b1203.metadata.provider.ManifestMetadataManager.StatusFlipOutcome;
import com.github.wxk6b1203.store.directory.Hierarchy;
import com.github.wxk6b1203.store.common.FileChecksums;
import com.github.wxk6b1203.store.common.PathUtil;
import com.github.wxk6b1203.store.object.RemoteObjectStore;
import com.github.wxk6b1203.store.object.S3RemoteObjectStore;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

@Slf4j
public class ManifestManager implements AutoCloseable {
    private static final long IN_FLIGHT_POLL_INTERVAL_MILLIS = 20L;
    private static final long COMPACTION_GRACE_MILLIS = Duration.ofMinutes(10).toMillis();
    private static final ConcurrentHashMap<UploadKey, CompletableFuture<Void>> IN_FLIGHT_UPLOADS = new ConcurrentHashMap<>();

    private final ManifestOptions options;
    private final RemoteObjectStore remoteObjectStore;
    private final ManifestMetadataManager metadataManager;
    private final boolean closeUploadWorkerPool;
    private final List<CompletableFuture<Boolean>> pendingUploads = new CopyOnWriteArrayList<>();
    private ExecutorService uploadWorkerPool;

    public ManifestManager(ManifestOptions options, S3Client s3Client, ManifestMetadataManager metadataManager) {
        this(options, s3Client == null ? null : new S3RemoteObjectStore(options.bucket(), s3Client), metadataManager);
    }

    public ManifestManager(ManifestOptions options, RemoteObjectStore remoteObjectStore, ManifestMetadataManager metadataManager) {
        this(options, remoteObjectStore, metadataManager, null, true);
    }

    public ManifestManager(
            ManifestOptions options,
            RemoteObjectStore remoteObjectStore,
            ManifestMetadataManager metadataManager,
            ExecutorService uploadWorkerPool
    ) {
        this(options, remoteObjectStore, metadataManager, uploadWorkerPool, false);
    }

    private ManifestManager(
            ManifestOptions options,
            RemoteObjectStore remoteObjectStore,
            ManifestMetadataManager metadataManager,
            ExecutorService uploadWorkerPool,
            boolean closeUploadWorkerPool
    ) {
        this.options = options == null ? new ManifestOptions("") : options;
        this.remoteObjectStore = remoteObjectStore;
        this.metadataManager = metadataManager;
        this.uploadWorkerPool = uploadWorkerPool;
        this.closeUploadWorkerPool = closeUploadWorkerPool;
    }

    public List<IndexFileMetadata> listAll(String indexName, List<IndexFileStatus> statuses) {
        return metadataManager.listAll(indexName, statuses);
    }

    /** Full-scan repair of the shard summary; see {@link ManifestMetadataManager#reconcileShardSummary}. */
    public void reconcileShardSummary(String indexName) {
        metadataManager.reconcileShardSummary(indexName);
    }

    public IndexFileMetadata fileMetadata(String indexName, String name) throws NoSuchFileException {
        IndexFileMetadata fileMetadata = metadataManager.fileMetadata(indexName, name);
        if (fileMetadata == null) {
            throw new NoSuchFileException(name);
        }
        return fileMetadata;
    }

    public IndexCommitSnapshot latestSnapshot(String indexName) {
        return metadataManager.latestSnapshot(indexName);
    }

    public IndexCommitSnapshot snapshot(String indexName, long generation) {
        return metadataManager.snapshot(indexName, generation);
    }

    public void pinSnapshot(String indexName, long generation, String pinId, long expiresAtMillis) {
        metadataManager.pinSnapshot(indexName, generation, pinId, expiresAtMillis);
    }

    public void releaseSnapshotPin(String indexName, String pinId) {
        metadataManager.releaseSnapshotPin(indexName, pinId);
    }

    public CompletableFuture<Boolean> commit(Collection<CommittingIndexFile> indexFiles) throws IOException {
        return commit(indexFiles, List.of());
    }

    public CompletableFuture<Boolean> commit(
            Collection<CommittingIndexFile> indexFiles,
            Collection<String> snapshotFileNames
    ) throws IOException {
        BuiltBatch built = buildBatch(indexFiles);
        List<IndexFile> batch = built.files();
        List<CommitFile> commitFiles = new ArrayList<>();
        List<PendingUpload> pendingUploads = new ArrayList<>();
        String snapshotIndexName = indexFiles.isEmpty() ? null : indexFiles.iterator().next().indexName();
        List<IndexFileMetadata> metadatas = batch.isEmpty()
                ? List.of()
                : metadataManager.commitFiles(batch);
        for (int i = 0; i < batch.size(); i++) {
            IndexFile file = batch.get(i);
            IndexFileMetadata metadata = metadatas.get(i);
            if (metadata != null) {
                pendingUploads.add(new PendingUpload(built.sources().get(i), metadata));
            }
            commitFiles.add(new CommitFile(file.indexName(), file.name()));
        }
        SnapshotCommit snapshotCommit = snapshotCommit(snapshotIndexName, snapshotFileNames, commitFiles);
        if (!pendingUploads.isEmpty()) {
            CompletableFuture<Boolean> upload = CompletableFuture.supplyAsync(
                    () -> uploadCommit(pendingUploads, snapshotCommit),
                    uploadWorkerPool()
            );
            trackUpload(upload);
            if (options.uploadWaitStrategy() == UploadWaitStrategy.WAIT_FOR_UPLOAD) {
                waitForUpload(upload, snapshotCommit);
            }
            return upload;
        } else {
            boolean published = publishSnapshotIfClean(snapshotCommit);
            if (options.uploadWaitStrategy() == UploadWaitStrategy.WAIT_FOR_UPLOAD && !published) {
                throw new IOException("commit snapshot was not published: " + snapshotCommit.indexName());
            }
            return CompletableFuture.completedFuture(published);
        }
    }

    /**
     * Upload locally committed files to S3 WITHOUT publishing a snapshot. Used to drain data on
     * nodes that lost shard ownership (failover) or hold leftover WAL directories after a restart.
     * Whether the content enters the visible history is decided by the current owner's commit line;
     * a deposed owner publishing snapshots would interleave a divergent segment line into the
     * snapshot sequence. Uploads are idempotent (content-addressed object keys) and metadata
     * transitions are epoch-guarded, so this never corrupts the new owner's state.
     */
    public CompletableFuture<Boolean> uploadOnly(Collection<CommittingIndexFile> indexFiles) throws IOException {
        List<CommittingIndexFile> present = new ArrayList<>();
        for (CommittingIndexFile indexFile : indexFiles) {
            if (Files.exists(indexFile.filePath())) {
                present.add(indexFile);
            }
        }
        if (present.isEmpty()) {
            return CompletableFuture.completedFuture(true);
        }
        BuiltBatch built = buildBatch(present);
        List<IndexFileMetadata> metadatas = metadataManager.commitFiles(built.files());
        List<PendingUpload> pendingUploads = new ArrayList<>();
        for (int i = 0; i < built.files().size(); i++) {
            IndexFileMetadata metadata = metadatas.get(i);
            if (metadata != null) {
                pendingUploads.add(new PendingUpload(built.sources().get(i), metadata));
            }
        }
        if (pendingUploads.isEmpty()) {
            return CompletableFuture.completedFuture(true);
        }
        CompletableFuture<Boolean> upload = CompletableFuture.supplyAsync(
                () -> transferAll(pendingUploads),
                uploadWorkerPool()
        );
        trackUpload(upload);
        return upload;
    }

    public void download(IndexFileMetadata metadata, Path target) throws IOException {
        ensureRemoteObjectStore();
        remoteObjectStore.get(metadata.getObjectKey(), target);
    }

    public void deleteIndexShards(String indexName, int numberOfShards) throws IOException {
        List<String> shardNames = new ArrayList<>(numberOfShards);
        for (int shard = 0; shard < numberOfShards; shard++) {
            shardNames.add(physicalIndexName(indexName, shard));
        }
        deleteIndices(shardNames);
    }

    public void deleteIndices(Collection<String> indexNames) throws IOException {
        ensureRemoteObjectStore();
        List<IndexFileMetadata> files = indexNames.stream()
                .flatMap(indexName -> metadataManager.listAll(indexName, allFileStatuses()).stream())
                .toList();
        Set<String> objectKeys = new LinkedHashSet<>();
        for (IndexFileMetadata file : files) {
            objectKeys.add(objectKey(file));
        }
        for (String indexName : indexNames) {
            metadataManager.listSnapshots(indexName).stream()
                    .flatMap(snapshot -> snapshot.getFiles().stream())
                    .map(this::objectKey)
                    .forEach(objectKeys::add);
        }
        remoteObjectStore.delete(objectKeys);
        for (String indexName : indexNames) {
            metadataManager.deleteAll(indexName);
        }
    }

    public void discardPendingUploads(String indexName) {
        List<IndexFileStatus> pendingStatuses = List.of(IndexFileStatus.DIRTY, IndexFileStatus.UPLOADING);
        List<IndexFileMetadata> pendingFiles = metadataManager.listAll(indexName, pendingStatuses);
        if (pendingFiles.isEmpty()) {
            return;
        }
        if (remoteObjectStore != null) {
            try {
                Set<String> objectKeys = new LinkedHashSet<>();
                for (IndexFileMetadata file : pendingFiles) {
                    objectKeys.add(objectKey(file));
                }
                remoteObjectStore.delete(objectKeys);
            } catch (IOException e) {
                log.warn("Failed to delete abandoned pending objects for {}", indexName, e);
            }
        }
        metadataManager.deleteByStatus(indexName, pendingStatuses);
    }

    public void garbageCollectSnapshots(String indexName, int retainLatestCount) throws IOException {
        ensureRemoteObjectStore();
        metadataManager.deleteExpiredSnapshotPins(System.currentTimeMillis());
        List<IndexCommitSnapshot> snapshots = metadataManager.listSnapshots(indexName).stream()
                .sorted(Comparator.comparingLong(IndexCommitSnapshot::getGeneration).reversed())
                .toList();
        if (snapshots.isEmpty()) {
            return;
        }
        Set<Long> protectedGenerations = new LinkedHashSet<>();
        snapshots.stream()
                .limit(Math.max(1, retainLatestCount))
                .map(IndexCommitSnapshot::getGeneration)
                .forEach(protectedGenerations::add);
        for (IndexCommitSnapshotPin pin : metadataManager.snapshotPins(indexName)) {
            protectedGenerations.add(pin.getGeneration());
        }

        Set<String> protectedObjectKeys = new LinkedHashSet<>();
        List<IndexCommitSnapshot> deleteCandidates = new ArrayList<>();
        for (IndexCommitSnapshot snapshot : snapshots) {
            if (protectedGenerations.contains(snapshot.getGeneration())) {
                snapshot.getFiles().stream()
                        .map(IndexFileMetadata::getObjectKey)
                        .filter(key -> key != null && !key.isBlank())
                        .forEach(protectedObjectKeys::add);
            } else {
                deleteCandidates.add(snapshot);
            }
        }

        Set<String> deleteObjectKeys = new LinkedHashSet<>();
        for (IndexCommitSnapshot snapshot : deleteCandidates) {
            snapshot.getFiles().stream()
                    .map(IndexFileMetadata::getObjectKey)
                    .filter(key -> key != null && !key.isBlank())
                    .filter(key -> !protectedObjectKeys.contains(key))
                    .forEach(deleteObjectKeys::add);
        }
        remoteObjectStore.delete(deleteObjectKeys);
        for (IndexCommitSnapshot snapshot : deleteCandidates) {
            metadataManager.deleteSnapshot(indexName, snapshot.getGeneration());
        }
        compactManifestEntries(indexName, snapshots, protectedObjectKeys);
    }

    /**
     * Compaction: delete CLEAN manifest entries no longer referenced by any retained snapshot.
     * Without this the per-file history grows unboundedly with merges (entries are only reclaimed
     * on whole-index delete), and every full-prefix scan — readiness probes, upload status,
     * UPLOAD_RETRY — gets slower forever, on top of the etcd storage cost. An entry is stale when
     * its name is absent from the latest snapshot (the live lineage) and its modified time is
     * older than the grace window (a publish may still be in flight); those contents are exactly
     * the ones the object-level GC above already stopped protecting.
     */
    private void compactManifestEntries(String indexName,
                                        List<IndexCommitSnapshot> snapshotsDescending,
                                        Set<String> protectedObjectKeys) {
        try {
            Set<String> latestNames = snapshotsDescending.isEmpty()
                    ? Set.of()
                    : snapshotsDescending.getFirst().getFiles().stream()
                            .map(IndexFileMetadata::getName)
                            .collect(Collectors.toSet());
            long graceCutoff = System.currentTimeMillis() - COMPACTION_GRACE_MILLIS;
            List<IndexFileMetadata> cleanEntries = metadataManager.listAll(indexName, List.of(IndexFileStatus.CLEAN));
            List<String> stale = new ArrayList<>();
            for (IndexFileMetadata entry : cleanEntries) {
                if (latestNames.contains(entry.getName())) {
                    continue;
                }
                if (entry.getModifiedTime() >= graceCutoff) {
                    continue;
                }
                String objectKey = entry.getObjectKey();
                if (objectKey != null && !objectKey.isBlank() && protectedObjectKeys.contains(objectKey)) {
                    continue;
                }
                stale.add(entry.getName());
            }
            if (!stale.isEmpty()) {
                metadataManager.deleteFiles(indexName, stale);
                log.debug("compacted {} stale manifest entries for {}", stale.size(), indexName);
            }
        } catch (RuntimeException e) {
            log.warn("Failed to compact manifest entries for {}", indexName, e);
        }
    }

    private BuiltBatch buildBatch(Collection<CommittingIndexFile> indexFiles) throws IOException {
        List<IndexFile> batch = new ArrayList<>();
        List<Path> batchSources = new ArrayList<>();
        String indexName = null;
        for (CommittingIndexFile indexFile : indexFiles) {
            if (indexName == null) {
                indexName = indexFile.indexName();
            } else if (!Objects.equals(indexName, indexFile.indexName())) {
                throw new IllegalArgumentException("commit must target a single physical index");
            }
            Path uploadSource = indexFile.filePath();
            long size = Files.size(uploadSource);
            long checksum = FileChecksums.crc32(uploadSource);
            long modifiedTime = Files.getLastModifiedTime(uploadSource).toMillis();
            String fileName = indexFile.filePath().getFileName().toString();
            String objectKey = remoteObjectKey(indexFile.indexName(), fileName, checksum, size);
            batch.add(new IndexFile(
                    indexFile.indexName(),
                    fileName,
                    Hierarchy.DATA.path,
                    objectKey,
                    size,
                    checksum,
                    modifiedTime
            ));
            batchSources.add(uploadSource);
        }
        return new BuiltBatch(batch, batchSources);
    }

    private record BuiltBatch(List<IndexFile> files, List<Path> sources) {
    }

    private boolean uploadCommit(List<PendingUpload> pendingUploads, SnapshotCommit snapshotCommit) {
        if (pendingUploads.isEmpty()) {
            return publishSnapshotIfClean(snapshotCommit);
        }
        if (!transferAll(pendingUploads)) {
            return false;
        }
        return publishSnapshotIfClean(snapshotCommit);
    }

    /**
     * Uploads files in batched stages. Etcd round trips used to scale with the file count
     * (per-file read + two read/CAS status transitions ≈ 7 round trips per file); the batched
     * flow is a constant ~5 plus one chunked CAS per flip stage:
     *  1. one batched read of the current entries (identity check against the commit),
     *  2. per phase (data files, then the segments file): one batched DIRTY→UPLOADING flip,
     *     the S3 puts, then one batched →CLEAN flip — the segments file only becomes visible
     *     after its data files are confirmed durable at the same epoch.
     */
    private boolean transferAll(List<PendingUpload> pendingUploads) {
        if (pendingUploads.isEmpty()) {
            return true;
        }
        try {
            ensureRemoteObjectStore();
        } catch (IOException e) {
            log.error("Remote object store unavailable; aborting upload", e);
            return false;
        }
        String indexName = pendingUploads.getFirst().metadata().getIndexName();
        Map<String, FileVersion> versions = readVersionsQuietly(indexName, pendingUploads);
        if (versions == null) {
            return false;
        }
        List<ActiveUpload> active = new ArrayList<>();
        for (PendingUpload pendingUpload : pendingUploads) {
            IndexFileMetadata expected = pendingUpload.metadata();
            FileVersion version = versions.get(expected.getName());
            if (version == null || !sameMetadataIdentity(expected, version.metadata())) {
                log.error("Manifest entry for {}/{} changed since commit; aborting publish", indexName, expected.getName());
                return false;
            }
            if (remoteReadable(version.metadata().getStatus())) {
                continue;
            }
            if (version.metadata().getStatus() != IndexFileStatus.DIRTY
                    && version.metadata().getStatus() != IndexFileStatus.UPLOADING) {
                log.error("Unexpected status {} for {}/{}; aborting publish",
                        version.metadata().getStatus(), indexName, expected.getName());
                return false;
            }
            active.add(new ActiveUpload(pendingUpload, version));
        }
        if (active.isEmpty()) {
            return true;
        }
        if (!uploadPhase(indexName, active, false)) {
            return false;
        }
        return uploadPhase(indexName, active, true);
    }

    private boolean uploadPhase(String indexName, List<ActiveUpload> active, boolean segmentsPhase) {
        List<ActiveUpload> phase = new ArrayList<>();
        for (ActiveUpload upload : active) {
            boolean isSegment = isCommittedSegmentFile(upload.pending.metadata().getName());
            if (isSegment != segmentsPhase) {
                continue;
            }
            upload.flippedHere = upload.version.metadata().getStatus() == IndexFileStatus.DIRTY;
            phase.add(upload);
        }
        if (phase.isEmpty()) {
            return true;
        }
        // One batched CAS for the whole phase (already-UPLOADING entries are rewritten with their
        // current value, a no-op that refreshes the chained revision); any stale entry makes the
        // store fall back to per-entry CAS so outcomes stay exact.
        List<FileVersion> toFlip = phase.stream()
                .map(upload -> upload.version)
                .toList();
        List<StatusFlipOutcome> outcomes = metadataManager.compareAndSetStatuses(indexName, toFlip, IndexFileStatus.UPLOADING);
        for (int i = 0; i < phase.size(); i++) {
            StatusFlipOutcome outcome = outcomes.get(i);
            if (!outcome.flipped()) {
                log.warn("Failed to mark {}/{} uploading; another writer owns the entry",
                        indexName, phase.get(i).pending.metadata().getName());
                return false;
            }
            phase.get(i).version = outcome.updated();
        }
        for (ActiveUpload upload : phase) {
            if (!transfer(upload, indexName)) {
                return false;
            }
        }
        List<FileVersion> cleanVersions = phase.stream()
                .map(upload -> upload.version)
                .toList();
        List<StatusFlipOutcome> cleanOutcomes = metadataManager.compareAndSetStatuses(indexName, cleanVersions, IndexFileStatus.CLEAN);
        for (int i = 0; i < phase.size(); i++) {
            StatusFlipOutcome outcome = cleanOutcomes.get(i);
            if (!outcome.flipped()) {
                log.warn("Failed to mark {}/{} clean; publish retries on the next maintenance tick",
                        indexName, phase.get(i).pending.metadata().getName());
                return false;
            }
            phase.get(i).version = outcome.updated();
        }
        return true;
    }

    private Map<String, FileVersion> readVersionsQuietly(String indexName, List<PendingUpload> pendingUploads) {
        try {
            List<String> names = pendingUploads.stream()
                    .map(pendingUpload -> pendingUpload.metadata().getName())
                    .toList();
            return metadataManager.fileVersionsByName(indexName, names);
        } catch (RuntimeException e) {
            log.error("Failed to read manifest state before uploading for {}", indexName, e);
            return null;
        }
    }

    /** S3 put for one file, guarded by the in-flight upload slot (cross-manager dedup). */
    private boolean transfer(ActiveUpload upload, String indexName) {
        IndexFileMetadata metadata = upload.pending.metadata();
        UploadKey uploadKey = new UploadKey(metadata.getObjectKey(), metadata.getEpoch());
        if (!acquireUploadSlot(uploadKey, metadata)) {
            IndexFileMetadata current = metadataManager.fileMetadata(indexName, metadata.getName());
            return current != null
                    && sameMetadataIdentity(metadata, current)
                    && remoteReadable(current.getStatus());
        }
        try {
            ensureRemoteObjectStore();
            if (!upload.flippedHere) {
                // The UPLOADING status predates this call (recovery of a previous run, or another
                // uploader may have finished while we waited for the slot): re-check before paying
                // for the put.
                IndexFileMetadata current = metadataManager.fileMetadata(indexName, metadata.getName());
                if (current == null || !sameMetadataIdentity(metadata, current)) {
                    log.error("Manifest entry for {}/{} changed during upload", indexName, metadata.getName());
                    return false;
                }
                if (remoteReadable(current.getStatus())) {
                    return true;
                }
            }
            remoteObjectStore.put(metadata.getObjectKey(), upload.pending.source());
            return true;
        } catch (Exception e) {
            log.error("Failed to upload index file {}/{}", metadata.getIndexName(), metadata.getName(), e);
            return false;
        } finally {
            releaseUploadSlot(uploadKey);
        }
    }

    private static final class ActiveUpload {
        private final PendingUpload pending;
        private FileVersion version;
        private boolean flippedHere;

        private ActiveUpload(PendingUpload pending, FileVersion version) {
            this.pending = pending;
            this.version = version;
        }
    }

    private boolean acquireUploadSlot(UploadKey uploadKey, IndexFileMetadata metadata) {
        long deadlineNanos = System.nanoTime() + options.uploadWaitTimeout().toNanos();
        while (true) {
            CompletableFuture<Void> created = new CompletableFuture<>();
            CompletableFuture<Void> existing = IN_FLIGHT_UPLOADS.putIfAbsent(uploadKey, created);
            if (existing == null) {
                return true;
            }
            // Another upload for the same content is in flight; wait for it to finish rather than
            // hammering the metadata store. Re-check metadata occasionally so we don't wait the
            // full timeout if the in-flight upload already succeeded but the slot wasn't cleared.
            IndexFileMetadata current = metadataManager.fileMetadata(metadata.getIndexName(), metadata.getName());
            if (sameMetadataIdentity(metadata, current) && remoteReadable(current.getStatus())) {
                return false;
            }
            long remainingNanos = deadlineNanos - System.nanoTime();
            if (remainingNanos <= 0) {
                return false;
            }
            try {
                existing.get(Math.min(remainingNanos, TimeUnit.MILLISECONDS.toNanos(IN_FLIGHT_POLL_INTERVAL_MILLIS)),
                        TimeUnit.NANOSECONDS);
            } catch (TimeoutException ignored) {
                // Poll again: re-check metadata and re-attempt slot acquisition.
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            } catch (ExecutionException ignored) {
                // The in-flight upload failed; loop to acquire the slot ourselves.
            }
        }
    }

    private void releaseUploadSlot(UploadKey uploadKey) {
        CompletableFuture<Void> pending = IN_FLIGHT_UPLOADS.remove(uploadKey);
        if (pending != null) {
            pending.complete(null);
        }
    }

    private boolean sameMetadataIdentity(IndexFileMetadata expected, IndexFileMetadata current) {
        return current != null
                && current.getEpoch() == expected.getEpoch()
                && Objects.equals(current.getObjectKey(), expected.getObjectKey());
    }

    private SnapshotCommit snapshotCommit(
            String snapshotIndexName,
            Collection<String> snapshotFileNames,
            List<CommitFile> commitFiles
    ) {
        if (snapshotFileNames == null || snapshotFileNames.isEmpty()) {
            String commitIndexName = snapshotIndexName;
            if (commitIndexName == null && !commitFiles.isEmpty()) {
                commitIndexName = commitFiles.getFirst().indexName();
            }
            return new SnapshotCommit(
                    commitIndexName,
                    commitFiles.stream()
                            .map(CommitFile::name)
                            .collect(LinkedHashSet::new, LinkedHashSet::add, LinkedHashSet::addAll)
            );
        }
        return new SnapshotCommit(snapshotIndexName, new LinkedHashSet<>(snapshotFileNames));
    }

    private boolean publishSnapshotIfClean(SnapshotCommit snapshotCommit) {
        if (snapshotCommit.indexName() == null || snapshotCommit.fileNames().isEmpty()) {
            return true;
        }
        String indexName = snapshotCommit.indexName();
        // Read only the files in this snapshot (one batched read), not every file the shard has
        // ever committed. This must be a fresh read (not the commit-time pre-read) because async
        // uploads flip statuses between commit and publish. CLEAN file metadata accumulates here
        // over time (reclaimed only on whole-index delete), so a full prefix read would decode
        // unbounded history on every publish.
        Map<String, IndexFileMetadata> allFiles = metadataManager.filesByName(indexName, snapshotCommit.fileNames());
        Map<String, IndexFileMetadata> files = new HashMap<>();
        for (String fileName : snapshotCommit.fileNames()) {
            IndexFileMetadata metadata = allFiles.get(fileName);
            if (metadata == null || !remoteReadable(metadata.getStatus())) {
                return false;
            }
            files.put(metadata.getName(), copy(metadata));
        }
        IndexFileMetadata segment = latestCommittedSegmentFile(files.values());
        if (segment == null) {
            return false;
        }
        metadataManager.publishSnapshot(segment.getIndexName(), segment.getName(), new ArrayList<>(files.values()));
        return true;
    }

    private void waitForUpload(CompletableFuture<Boolean> upload, SnapshotCommit snapshotCommit) throws IOException {
        long deadlineNanos = System.nanoTime() + options.uploadWaitTimeout().toNanos();
        try {
            boolean published = upload.get(options.uploadWaitTimeout().toMillis(), TimeUnit.MILLISECONDS);
            if (!published && !waitForSnapshot(snapshotCommit, deadlineNanos)) {
                throw new IOException("commit upload did not publish a clean snapshot: " + snapshotCommit.indexName());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while waiting for commit upload: " + snapshotCommit.indexName(), e);
        } catch (TimeoutException e) {
            throw new IOException("timed out waiting for commit upload: " + snapshotCommit.indexName(), e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() == null ? e : e.getCause();
            if (cause instanceof IOException ioException) {
                throw ioException;
            }
            throw new IOException("failed while waiting for commit upload: " + snapshotCommit.indexName(), cause);
        }
    }

    private boolean waitForSnapshot(SnapshotCommit snapshotCommit, long deadlineNanos) {
        while (System.nanoTime() < deadlineNanos) {
            if (publishSnapshotIfClean(snapshotCommit)) {
                return true;
            }
            try {
                TimeUnit.MILLISECONDS.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return publishSnapshotIfClean(snapshotCommit);
    }

    private IndexFileMetadata latestCommittedSegmentFile(Collection<IndexFileMetadata> files) {
        return files.stream()
                .filter(file -> isCommittedSegmentFile(file.getName()))
                .max(Comparator
                        .comparingLong(IndexFileMetadata::getModifiedTime)
                        .thenComparing(IndexFileMetadata::getName))
                .orElse(null);
    }

    private boolean remoteReadable(IndexFileStatus status) {
        return status == IndexFileStatus.CLEAN || status == IndexFileStatus.PINNED;
    }

    private IndexFileMetadata copy(IndexFileMetadata file) {
        return new IndexFileMetadata(
                file.getIndexName(),
                file.getName(),
                file.getDataDirectory(),
                file.getObjectKey(),
                file.getEpoch(),
                file.getSize(),
                file.getChecksum(),
                file.getModifiedTime(),
                file.getStatus()
        );
    }

    private void ensureRemoteObjectStore() throws IOException {
        if (remoteObjectStore == null) {
            throw new IOException("remote object store is not configured");
        }
    }

    private List<IndexFileStatus> allFileStatuses() {
        return List.of(
                IndexFileStatus.DIRTY,
                IndexFileStatus.UPLOADING,
                IndexFileStatus.CLEAN,
                IndexFileStatus.PINNED
        );
    }

    private String objectKey(IndexFileMetadata metadata) {
        return metadata.getObjectKey() == null || metadata.getObjectKey().isBlank()
                ? PathUtil.s3ObjectKey(metadata.getIndexName(), metadata.getName())
                : metadata.getObjectKey();
    }

    private String remoteObjectKey(String indexName, String fileName, long checksum, long size) {
        return PathUtil.s3ObjectKey(indexName, fileName + "." + Long.toUnsignedString(checksum, 16) + "." + size);
    }

    private String physicalIndexName(String indexName, int shard) {
        return indexName + "__shard_" + shard;
    }

    private boolean isCommittedSegmentFile(String name) {
        return name.startsWith("segments_") && !name.startsWith("pending_segments_");
    }

    private synchronized ExecutorService uploadWorkerPool() {
        if (uploadWorkerPool == null) {
            uploadWorkerPool = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("upload-worker-", 0).factory());
        }
        return uploadWorkerPool;
    }

    private void trackUpload(CompletableFuture<Boolean> upload) {
        pendingUploads.add(upload);
        upload.whenComplete((ignored, throwable) -> pendingUploads.remove(upload));
    }

    private void waitForPendingUploadsOnClose() {
        for (CompletableFuture<Boolean> upload : List.copyOf(pendingUploads)) {
            try {
                upload.get(options.uploadWaitTimeout().toMillis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (ExecutionException | TimeoutException e) {
                log.warn("pending manifest upload did not complete before manager close", e);
            }
        }
    }

    private record PendingUpload(Path source, IndexFileMetadata metadata) {
    }

    private record CommitFile(String indexName, String name) {
    }

    private record SnapshotCommit(String indexName, Set<String> fileNames) {
    }

    private record UploadKey(String objectKey, long epoch) {
    }

    @Override
    public void close() {
        waitForPendingUploadsOnClose();
        if (uploadWorkerPool == null || !closeUploadWorkerPool) {
            return;
        }
        this.uploadWorkerPool.shutdown();
        try {
            this.uploadWorkerPool.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
