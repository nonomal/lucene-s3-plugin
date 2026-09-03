package com.github.wxk6b1203.metadata.provider.mem;

import com.github.wxk6b1203.metadata.common.IndexFile;
import com.github.wxk6b1203.metadata.common.IndexFileStatus;
import com.github.wxk6b1203.metadata.common.ShardSummary;
import com.github.wxk6b1203.metadata.provider.ManifestMetadataManager.FileVersion;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MemMockProviderTest {
    @Test
    void maintainsShardPendingSummaryAcrossTheUploadStateMachine() {
        MemMockProvider provider = new MemMockProvider();

        provider.commitFile(new IndexFile("books__shard_0", "_0.si", 128, 7));
        provider.commitFile(new IndexFile("books__shard_0", "segments_1", 64, 3));
        assertEquals(2, provider.shardSummary("books__shard_0").pendingCount());

        provider.updateFileStatus("books__shard_0", "_0.si", 1, IndexFileStatus.UPLOADING);
        assertEquals(2, provider.shardSummary("books__shard_0").pendingCount());

        provider.updateFileStatus("books__shard_0", "_0.si", 1, IndexFileStatus.CLEAN);
        assertEquals(1, provider.shardSummary("books__shard_0").pendingCount());

        // Same-epoch re-commit of a still-pending file must not double count.
        provider.commitFile(new IndexFile("books__shard_0", "segments_1", 64, 3));
        assertEquals(1, provider.shardSummary("books__shard_0").pendingCount());

        // Re-commit with different content: old pending entry replaced, count unchanged.
        provider.commitFile(new IndexFile("books__shard_0", "segments_1", 256, 9));
        assertEquals(1, provider.shardSummary("books__shard_0").pendingCount());
    }

    @Test
    void compareAndSetStatusesReportsExactPerEntryOutcomes() {
        MemMockProvider provider = new MemMockProvider();
        provider.commitFile(new IndexFile("books__shard_0", "_0.si", 128, 7));
        provider.commitFile(new IndexFile("books__shard_0", "segments_1", 64, 3));
        var fresh = provider.fileVersionsByName("books__shard_0", List.of("_0.si", "segments_1"));

        // A stale version (epoch bumped by a re-commit) must not flip; the fresh one must.
        provider.commitFile(new IndexFile("books__shard_0", "_0.si", 256, 9));
        var outcomes = provider.compareAndSetStatuses("books__shard_0",
                List.of(fresh.get("_0.si"), fresh.get("segments_1")), IndexFileStatus.UPLOADING);

        assertFalse(outcomes.get(0).flipped());
        assertTrue(outcomes.get(1).flipped());
        assertEquals(IndexFileStatus.UPLOADING,
                provider.fileMetadata("books__shard_0", "segments_1").getStatus());
        assertEquals(2, provider.shardSummary("books__shard_0").pendingCount());
    }

    @Test
    void deletePathsKeepTheSummaryHonest() {
        MemMockProvider provider = new MemMockProvider();
        provider.commitFile(new IndexFile("books__shard_0", "_0.si", 128, 7));
        provider.commitFile(new IndexFile("books__shard_0", "segments_1", 64, 3));

        provider.deleteFile("books__shard_0", "_0.si");
        assertEquals(1, provider.shardSummary("books__shard_0").pendingCount());

        provider.deleteByStatus("books__shard_0", List.of(IndexFileStatus.DIRTY, IndexFileStatus.UPLOADING));
        assertEquals(0, provider.shardSummary("books__shard_0").pendingCount());

        provider.putShardSummary("books__shard_0", new ShardSummary(42, 7, 0));
        provider.reconcileShardSummary("books__shard_0");
        ShardSummary repaired = provider.shardSummary("books__shard_0");
        assertEquals(0, repaired.pendingCount());
        assertEquals(0, repaired.latestGeneration());

        provider.deleteAll("books__shard_0");
        assertNull(provider.shardSummary("books__shard_0"));
    }
}
