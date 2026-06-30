package com.github.wxk6b1203.metadata.provider;

import com.github.wxk6b1203.metadata.common.IndexFile;
import com.github.wxk6b1203.metadata.common.IndexCommitSnapshot;
import com.github.wxk6b1203.metadata.common.IndexCommitSnapshotPin;
import com.github.wxk6b1203.metadata.common.IndexFileMetadata;
import com.github.wxk6b1203.metadata.common.IndexFileStatus;

import java.util.ArrayList;
import java.util.List;

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
            if (existing != null) {
                if (existing.getStatus() == IndexFileStatus.CLEAN
                        || existing.getStatus() == IndexFileStatus.PINNED) {
                    result.add(null);
                    continue;
                }
                if (sameUpload(existing, file)) {
                    result.add(existing);
                    continue;
                }
            }
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

    public abstract List<IndexFileMetadata> listAll(String indexName, List<IndexFileStatus> status);

    public abstract IndexFileMetadata fileMetadata(String indexName, String name);

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
