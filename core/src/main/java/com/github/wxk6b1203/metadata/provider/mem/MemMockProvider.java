package com.github.wxk6b1203.metadata.provider.mem;

import com.github.wxk6b1203.metadata.common.IndexFile;
import com.github.wxk6b1203.metadata.common.IndexCommitSnapshot;
import com.github.wxk6b1203.metadata.common.IndexCommitSnapshotPin;
import com.github.wxk6b1203.metadata.common.IndexFileMetadata;
import com.github.wxk6b1203.metadata.common.IndexFileStatus;
import com.github.wxk6b1203.metadata.common.ShardSummary;
import com.github.wxk6b1203.metadata.provider.ManifestMetadataManager;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class MemMockProvider extends ManifestMetadataManager {
    private final ConcurrentHashMap<String, IndexFileMetadata> files = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, IndexCommitSnapshot> snapshots = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, IndexCommitSnapshotPin> pins = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ShardSummary> shardSummaries = new ConcurrentHashMap<>();

    private String keyName(IndexFile indexFile) {
        return indexFile.indexName() + "/" + indexFile.name();
    }

    private String keyName(String indexName, String fileName) {
        return indexName + "/" + fileName;
    }

    @Override
    public synchronized IndexFileMetadata commitFile(IndexFile file) {
        String key = keyName(file);
        IndexFileMetadata existing = files.get(key);
        long epoch = existing == null ? 1 : existing.getEpoch() + 1;
        IndexFileMetadata metadata = new IndexFileMetadata(
                file.indexName(),
                file.name(),
                file.dataDirectory(),
                file.objectKey(),
                epoch,
                file.size(),
                file.checksum(),
                file.modifiedTime(),
                IndexFileStatus.DIRTY
        );
        files.put(key, metadata);
        bumpPending(file.indexName(), existing == null || !isPending(existing) ? 1 : 0);
        return metadata;
    }

    private boolean isPending(IndexFileMetadata metadata) {
        return metadata.getStatus() == IndexFileStatus.DIRTY
                || metadata.getStatus() == IndexFileStatus.UPLOADING;
    }

    private void bumpPending(String indexName, int delta) {
        if (delta == 0) {
            return;
        }
        ShardSummary base = shardSummaries.getOrDefault(indexName, ShardSummary.empty());
        shardSummaries.put(indexName, new ShardSummary(
                Math.max(0, base.pendingCount() + delta),
                base.latestGeneration(),
                System.currentTimeMillis()));
    }

    @Override
    public synchronized void updateFileStatus(String indexName, String fileName, long epoch, IndexFileStatus status) {
        IndexFileMetadata metadata = fileMetadata(indexName, fileName);
        if (metadata == null || metadata.getEpoch() != epoch) {
            return;
        }
        if (!IndexFileStatus.validTransition(metadata.getStatus(), status)) {
            log.error("Invalid status transition from {} to {}", metadata.getStatus(), status);
            return;
        }
        boolean wasPending = isPending(metadata);
        metadata.setStatus(status);
        if (wasPending && !isPending(metadata)) {
            bumpPending(indexName, -1);
        } else if (!wasPending && isPending(metadata)) {
            bumpPending(indexName, 1);
        }
    }

    @Override
    public synchronized ShardSummary shardSummary(String indexName) {
        return shardSummaries.get(indexName);
    }

    @Override
    public synchronized void putShardSummary(String indexName, ShardSummary summary) {
        shardSummaries.put(indexName, summary);
    }

    @Override
    public synchronized Map<String, ShardSummary> shardSummaries() {
        return new HashMap<>(shardSummaries);
    }

    @Override
    public synchronized List<IndexFileMetadata> listAll(String indexName, List<IndexFileStatus> status) {
        String prefix = indexName + "/";
        return files.entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(prefix))
                .map(java.util.Map.Entry::getValue)
                .filter(metadata -> status.contains(metadata.getStatus()))
                .toList();
    }

    @Override
    public synchronized IndexFileMetadata fileMetadata(String indexName, String name) {
        return files.get(keyName(indexName, name));
    }

    @Override
    public synchronized long publishSnapshot(String indexName, String segmentFileName, List<IndexFileMetadata> files) {
        long generation = listSnapshots(indexName).stream()
                .mapToLong(IndexCommitSnapshot::getGeneration)
                .max()
                .orElse(0) + 1;
        IndexCommitSnapshot snapshot = new IndexCommitSnapshot(
                indexName,
                generation,
                segmentFileName,
                copyFiles(files),
                System.currentTimeMillis()
        );
        snapshots.put(snapshotKey(indexName, generation), snapshot);
        ShardSummary base = shardSummaries.getOrDefault(indexName, ShardSummary.empty());
        shardSummaries.put(indexName, new ShardSummary(
                base.pendingCount(), generation, System.currentTimeMillis()));
        return generation;
    }

    @Override
    public synchronized IndexCommitSnapshot latestSnapshot(String indexName) {
        return listSnapshots(indexName).stream()
                .max(Comparator.comparingLong(IndexCommitSnapshot::getGeneration))
                .orElse(null);
    }

    @Override
    public synchronized IndexCommitSnapshot snapshot(String indexName, long generation) {
        return snapshots.get(snapshotKey(indexName, generation));
    }

    @Override
    public synchronized List<IndexCommitSnapshot> listSnapshots(String indexName) {
        String prefix = indexName + "/";
        return snapshots.entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(prefix))
                .map(java.util.Map.Entry::getValue)
                .sorted(Comparator.comparingLong(IndexCommitSnapshot::getGeneration))
                .toList();
    }

    @Override
    public synchronized void deleteSnapshot(String indexName, long generation) {
        snapshots.remove(snapshotKey(indexName, generation));
    }

    @Override
    public synchronized void pinSnapshot(String indexName, long generation, String pinId, long expiresAtMillis) {
        pins.put(pinKey(indexName, pinId), new IndexCommitSnapshotPin(indexName, generation, pinId, expiresAtMillis));
    }

    @Override
    public synchronized void releaseSnapshotPin(String indexName, String pinId) {
        pins.remove(pinKey(indexName, pinId));
    }

    @Override
    public synchronized List<IndexCommitSnapshotPin> snapshotPins(String indexName) {
        String prefix = indexName + "/";
        return pins.entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(prefix))
                .map(java.util.Map.Entry::getValue)
                .toList();
    }

    @Override
    public synchronized void deleteExpiredSnapshotPins(long nowMillis) {
        pins.entrySet().removeIf(entry -> entry.getValue().getExpiresAtMillis() <= nowMillis);
    }

    @Override
    public synchronized void deleteFile(String indexName, String name) {
        IndexFileMetadata removed = files.remove(keyName(indexName, name));
        if (removed != null && isPending(removed)) {
            bumpPending(indexName, -1);
        }
    }

    @Override
    public synchronized void deleteByStatus(String indexName, List<IndexFileStatus> statuses) {
        String prefix = indexName + "/";
        files.entrySet().removeIf(entry -> entry.getKey().startsWith(prefix)
                && statuses.contains(entry.getValue().getStatus()));
        if (statuses.contains(IndexFileStatus.DIRTY) || statuses.contains(IndexFileStatus.UPLOADING)) {
            ShardSummary base = shardSummaries.getOrDefault(indexName, ShardSummary.empty());
            shardSummaries.put(indexName, new ShardSummary(0, base.latestGeneration(), System.currentTimeMillis()));
        }
    }

    @Override
    public synchronized void deleteAll(String indexName) {
        String prefix = indexName + "/";
        files.keySet().removeIf(key -> key.startsWith(prefix));
        snapshots.keySet().removeIf(key -> key.startsWith(prefix));
        pins.keySet().removeIf(key -> key.startsWith(prefix));
        shardSummaries.keySet().removeIf(key -> key.equals(indexName));
    }

    private String snapshotKey(String indexName, long generation) {
        return indexName + "/" + generation;
    }

    private String pinKey(String indexName, String pinId) {
        return indexName + "/" + pinId;
    }

    private List<IndexFileMetadata> copyFiles(List<IndexFileMetadata> files) {
        List<IndexFileMetadata> copies = new ArrayList<>(files.size());
        for (IndexFileMetadata file : files) {
            copies.add(copy(file));
        }
        return copies;
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
}
