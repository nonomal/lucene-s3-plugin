package com.github.wxk6b1203.metadata.common;

/**
 * Snapshot identity without the file list. Hot paths (hybrid read targets, readiness probes,
 * GC scans, generation probes) only need generation/segment/existence; the file list lives in
 * separate chunk keys so a single etcd value never grows past the request-size limit.
 */
public record IndexCommitSnapshotHeader(
        String indexName,
        long generation,
        String segmentFileName,
        int fileCount,
        int chunkCount,
        long createdAtMillis
) {
    public static IndexCommitSnapshotHeader of(IndexCommitSnapshot snapshot) {
        return new IndexCommitSnapshotHeader(
                snapshot.getIndexName(),
                snapshot.getGeneration(),
                snapshot.getSegmentFileName(),
                snapshot.getFiles() == null ? 0 : snapshot.getFiles().size(),
                0,
                snapshot.getCreatedAtMillis()
        );
    }
}
