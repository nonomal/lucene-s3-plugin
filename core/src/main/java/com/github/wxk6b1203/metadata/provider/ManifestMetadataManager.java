package com.github.wxk6b1203.metadata.provider;

import com.github.wxk6b1203.metadata.common.IndexFile;
import com.github.wxk6b1203.metadata.common.IndexCommitSnapshot;
import com.github.wxk6b1203.metadata.common.IndexCommitSnapshotPin;
import com.github.wxk6b1203.metadata.common.IndexFileMetadata;
import com.github.wxk6b1203.metadata.common.IndexFileStatus;
import com.github.wxk6b1203.metadata.common.ShardSummary;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class ManifestMetadataManager {
    public abstract IndexFileMetadata commitFile(IndexFile file);

    /**
     * Commit metadata for a batch of files. Default implementation loops {@link #commitFile},
     * skipping files that are already CLEAN/PINNED or unchanged (same upload identity) — mirroring
     * the per-file skip logic so non-etcd providers preserve the upload state machine.
     * etcd-backed implementations override this with a single transactional CAS so all files in a
     * commit advance their epoch atomically in one round-trip.
     *
     * @return the committed metadata for each input file, in the same order; entries may be null
     *         when the file is already CLEAN/PINNED (no upload needed) or the existing metadata
     *         when the upload identity is unchanged.
     */
    public List<IndexFileMetadata> commitFiles(List<IndexFile> files) {
        List<IndexFileMetadata> result = new ArrayList<>(files.size());
        for (IndexFile file : files) {
            IndexFileMetadata existing = fileMetadata(file.indexName(), file.name());
            if (existing != null && sameUpload(existing, file)) {
                // Same name AND same content: skip only when already durable remotely; otherwise reuse the
                // existing metadata so the upload state machine (DIRTY -> UPLOADING -> CLEAN) continues.
                if (existing.getStatus() == IndexFileStatus.CLEAN
                        || existing.getStatus() == IndexFileStatus.PINNED) {
                    result.add(null);
                } else {
                    result.add(existing);
                }
                continue;
            }
            // Same name but different content (two divergent owner histories colliding on one segment
            // name after failover) must re-commit with a bumped epoch. Skipping on CLEAN status alone
            // would keep the stale objectKey published and corrupt readers of the new history line.
            result.add(commitFile(file));
        }
        return result;
    }

    private boolean sameUpload(IndexFileMetadata metadata, IndexFile file) {
        return metadata.getSize() == file.size()
                && metadata.getChecksum() == file.checksum()
                && metadata.getModifiedTime() == file.modifiedTime()
                && java.util.Objects.equals(metadata.getObjectKey(), file.objectKey());
    }

    public abstract void updateFileStatus(String indexName, String fileName, long epoch, IndexFileStatus status);

    /** A stored file entry together with the store revision, for revision-guarded CAS updates. */
    public record FileVersion(String fileName, IndexFileMetadata metadata, long modRevision) {
    }

    /** Outcome of a conditional status flip: whether it applied, and the entry as stored now. */
    public record StatusFlipOutcome(boolean flipped, FileVersion updated) {
    }

    /**
     * Read the given files in one logical call, carrying the store revision so callers can CAS
     * status transitions without a separate read. The default loops {@link #fileMetadata(String, String)}
     * with revision {@code -1} (meaning "no revision guard available").
     */
    public Map<String, FileVersion> fileVersionsByName(String indexName, java.util.Collection<String> names) {
        Map<String, FileVersion> result = new HashMap<>();
        for (String name : names) {
            IndexFileMetadata metadata = fileMetadata(indexName, name);
            if (metadata != null) {
                result.put(name, new FileVersion(name, metadata, -1L));
            }
        }
        return result;
    }

    /**
     * Conditionally advance the status of one file: the flip applies only when the stored entry
     * still matches {@code expected} (revision when available, otherwise epoch + transition rule).
     * The returned {@link FileVersion} reflects the stored entry after the attempt, so callers can
     * chain further flips without re-reading (null when the entry disappeared).
     */
    public StatusFlipOutcome compareAndSetStatus(String indexName, FileVersion expected, IndexFileStatus to) {
        long epoch = expected.metadata().getEpoch();
        IndexFileMetadata current = fileMetadata(indexName, expected.fileName());
        if (current == null) {
            return new StatusFlipOutcome(false, null);
        }
        if (current.getEpoch() != epoch) {
            return new StatusFlipOutcome(false, new FileVersion(expected.fileName(), current, -1L));
        }
        if (current.getStatus() == to) {
            return new StatusFlipOutcome(true, new FileVersion(expected.fileName(), current, -1L));
        }
        updateFileStatus(indexName, expected.fileName(), epoch, to);
        IndexFileMetadata after = fileMetadata(indexName, expected.fileName());
        boolean flipped = after != null && after.getStatus() == to && after.getEpoch() == epoch;
        return new StatusFlipOutcome(flipped, after == null ? null : new FileVersion(expected.fileName(), after, -1L));
    }

    /**
     * Flip a batch of files in one logical call. Entries whose stored revision no longer matches
     * are reported as not flipped; the store is free to fall back to per-entry CAS so the outcome
     * for each entry is exact.
     */
    public List<StatusFlipOutcome> compareAndSetStatuses(String indexName, List<FileVersion> expected, IndexFileStatus to) {
        List<StatusFlipOutcome> results = new ArrayList<>(expected.size());
        for (FileVersion version : expected) {
            results.add(compareAndSetStatus(indexName, version, to));
        }
        return results;
    }

    /** Delete one manifest entry; used by snapshot GC compaction of stale CLEAN history. */
    public abstract void deleteFile(String indexName, String name);

    /**
     * Per-shard aggregate (pending count + latest snapshot generation) used by readiness probes
     * and retry ticks instead of full file-history scans. Returns null when no summary exists.
     */
    public abstract ShardSummary shardSummary(String indexName);

    /** Overwrite the shard summary; used by the full-scan repair below and by tests. */
    public abstract void putShardSummary(String indexName, ShardSummary summary);

    /** Read summaries for all shards in one logical call; default loops {@link #shardSummary}. */
    public java.util.Map<String, ShardSummary> shardSummaries() {
        return java.util.Map.of();
    }

    /**
     * Recompute the summary from a full scan. The transactional maintenance above cannot drift
     * from the file entries it counts, so this is a repair path for legacy shards (no summary yet)
     * and a safety net, not a hot-path dependency.
     */
    public void reconcileShardSummary(String indexName) {
        int pending = listAll(indexName, List.of(IndexFileStatus.DIRTY, IndexFileStatus.UPLOADING)).size();
        IndexCommitSnapshot snapshot = latestSnapshot(indexName);
        putShardSummary(indexName, new ShardSummary(
                pending,
                snapshot == null ? 0 : snapshot.getGeneration(),
                System.currentTimeMillis()
        ));
    }

    /**
     * Delete multiple manifest entries. The default loops {@link #deleteFile(String, String)};
     * transactional stores override this to batch the deletions.
     */
    public void deleteFiles(String indexName, java.util.Collection<String> names) {
        for (String name : names) {
            deleteFile(indexName, name);
        }
    }

    public abstract List<IndexFileMetadata> listAll(String indexName, List<IndexFileStatus> status);

    public abstract IndexFileMetadata fileMetadata(String indexName, String name);

    /**
     * Read metadata for a specific set of file names in one logical call. Default implementation
     * loops {@link #fileMetadata}; etcd-backed implementations override with a single read-only
     * transaction so callers avoid a full prefix range read when they only need a known set of
     * files. CLEAN file metadata accumulates unbounded across a shard's lifetime (reclaimed only
     * on whole-index delete), so a prefix read would decode the entire history on every call.
     */
    public Map<String, IndexFileMetadata> filesByName(String indexName, Collection<String> names) {
        Map<String, IndexFileMetadata> result = new HashMap<>(names.size());
        for (String name : names) {
            IndexFileMetadata metadata = fileMetadata(indexName, name);
            if (metadata != null) {
                result.put(name, metadata);
            }
        }
        return result;
    }

    public abstract long publishSnapshot(String indexName, String segmentFileName, List<IndexFileMetadata> files);

    public abstract IndexCommitSnapshot latestSnapshot(String indexName);

    public abstract IndexCommitSnapshot snapshot(String indexName, long generation);

    public abstract List<IndexCommitSnapshot> listSnapshots(String indexName);

    public abstract void deleteSnapshot(String indexName, long generation);

    public abstract void pinSnapshot(String indexName, long generation, String pinId, long expiresAtMillis);

    public abstract void releaseSnapshotPin(String indexName, String pinId);

    public abstract List<IndexCommitSnapshotPin> snapshotPins(String indexName);

    public abstract void deleteExpiredSnapshotPins(long nowMillis);

    public abstract void deleteByStatus(String indexName, List<IndexFileStatus> statuses);

    public abstract void deleteAll(String indexName);
}
