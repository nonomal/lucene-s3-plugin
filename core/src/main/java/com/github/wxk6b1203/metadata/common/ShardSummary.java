package com.github.wxk6b1203.metadata.common;

/**
 * Per-shard aggregate maintained alongside the file entries so readiness probes and retry ticks
 * read one small key instead of scanning the full file history. {@code pendingCount} tracks files
 * in DIRTY/UPLOADING state; it is updated in the same transaction as the file writes it reflects,
 * so it cannot drift from them absent a bug (a full-scan repair exists via reconcile).
 *
 * @param pendingCount      number of files currently DIRTY or UPLOADING
 * @param latestGeneration  latest published snapshot generation (0 when none)
 * @param updatedAt         wall-clock milliseconds of the last summary write
 */
public record ShardSummary(int pendingCount, long latestGeneration, long updatedAt) {
    public static ShardSummary empty() {
        return new ShardSummary(0, 0, System.currentTimeMillis());
    }
}
