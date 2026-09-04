package com.github.wxk6b1203.metadata.provider;

import com.github.wxk6b1203.metadata.common.IndexCommitSnapshot;
import com.github.wxk6b1203.metadata.common.IndexCommitSnapshotHeader;
import com.github.wxk6b1203.metadata.common.IndexFile;
import com.github.wxk6b1203.metadata.common.IndexFileMetadata;
import com.github.wxk6b1203.metadata.common.IndexFileStatus;
import com.github.wxk6b1203.metadata.common.ShardSummary;
import com.github.wxk6b1203.metadata.provider.etcd.EtcdManifestMetadataManager;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.options.GetOption;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class EtcdManifestMetadataManagerTest {
    @Test
    @EnabledIfEnvironmentVariable(named = "ETCD_TEST_ENDPOINTS", matches = ".+")
    public void storesFileManifestMetadata() {
        Client client = Client.builder().endpoints(System.getenv("ETCD_TEST_ENDPOINTS")).build();
        String namespace = "test-manifest/" + UUID.randomUUID();
        EtcdManifestMetadataManager provider = new EtcdManifestMetadataManager(
                EtcdManifestMetadataManager.Options.builder()
                        .namespace(namespace)
                        .build(),
                client
        );
        try {
            long epoch = provider.commitFile(new IndexFile("books__shard_0", "segments_1", 128, 7)).getEpoch();
            provider.updateFileStatus("books__shard_0", "segments_1", epoch, IndexFileStatus.UPLOADING);
            provider.updateFileStatus("books__shard_0", "segments_1", epoch, IndexFileStatus.CLEAN);

            var metadata = provider.fileMetadata("books__shard_0", "segments_1");
            assertNotNull(metadata);
            assertEquals(IndexFileStatus.CLEAN, metadata.getStatus());
            assertEquals(1, provider.listAll("books__shard_0", List.of(IndexFileStatus.CLEAN)).size());

            long generation = provider.publishSnapshot("books__shard_0", "segments_1", List.of(metadata));
            assertEquals(generation, provider.latestSnapshot("books__shard_0").getGeneration());
            assertEquals(1, provider.listSnapshots("books__shard_0").size());

            provider.pinSnapshot("books__shard_0", generation, "pit-1", System.currentTimeMillis() - 1);
            assertEquals(1, provider.snapshotPins("books__shard_0").size());
            provider.deleteExpiredSnapshotPins(System.currentTimeMillis());
            assertTrue(provider.snapshotPins("books__shard_0").isEmpty());

            provider.deleteAll("books__shard_0");
            assertEquals(0, provider.listAll("books__shard_0", List.of(IndexFileStatus.CLEAN)).size());
            assertNull(provider.latestSnapshot("books__shard_0"));
        } finally {
            client.close();
        }
    }

    @Test
    @EnabledIfEnvironmentVariable(named = "ETCD_TEST_ENDPOINTS", matches = ".+")
    public void compareAndSetStatusFollowsUploadStateMachineAndChainsRevisions() {
        Client client = Client.builder().endpoints(System.getenv("ETCD_TEST_ENDPOINTS")).build();
        String namespace = "test-manifest/" + UUID.randomUUID();
        EtcdManifestMetadataManager provider = new EtcdManifestMetadataManager(
                EtcdManifestMetadataManager.Options.builder().namespace(namespace).build(), client);
        try {
            provider.commitFile(new IndexFile("books__shard_0", "segments_1", 128, 7));
            var versions = provider.fileVersionsByName("books__shard_0", List.of("segments_1"));
            var dirty = versions.get("segments_1");
            assertNotNull(dirty);
            assertTrue(dirty.modRevision() >= 1, "etcd must carry a usable revision for CAS");
            assertEquals(IndexFileStatus.DIRTY, dirty.metadata().getStatus());

            var uploading = provider.compareAndSetStatus("books__shard_0", dirty, IndexFileStatus.UPLOADING);
            assertTrue(uploading.flipped());
            assertNotNull(uploading.updated());
            assertEquals(IndexFileStatus.UPLOADING, uploading.updated().metadata().getStatus());
            assertTrue(uploading.updated().modRevision() > dirty.modRevision(), "flip must return the fresh revision");

            // The returned version chains straight into the next flip (uploadCommit does exactly this).
            var clean = provider.compareAndSetStatus("books__shard_0", uploading.updated(), IndexFileStatus.CLEAN);
            assertTrue(clean.flipped());
            assertEquals(IndexFileStatus.CLEAN, clean.updated().metadata().getStatus());

            // A stale revision with the same epoch converges onto the fresh entry (epoch is the
            // real ownership guard); the transition rule is still enforced.
            var staleReplay = provider.compareAndSetStatus("books__shard_0", uploading.updated(), IndexFileStatus.PINNED);
            assertTrue(staleReplay.flipped());
            assertEquals(IndexFileStatus.PINNED, staleReplay.updated().metadata().getStatus());
        } finally {
            client.close();
        }
    }

    @Test
    @EnabledIfEnvironmentVariable(named = "ETCD_TEST_ENDPOINTS", matches = ".+")
    public void compareAndSetStatusRejectsEpochMismatchAndMissingEntries() {
        Client client = Client.builder().endpoints(System.getenv("ETCD_TEST_ENDPOINTS")).build();
        String namespace = "test-manifest/" + UUID.randomUUID();
        EtcdManifestMetadataManager provider = new EtcdManifestMetadataManager(
                EtcdManifestMetadataManager.Options.builder().namespace(namespace).build(), client);
        try {
            provider.commitFile(new IndexFile("books__shard_0", "segments_1", 128, 7));
            var stale = provider.fileVersionsByName("books__shard_0", List.of("segments_1")).get("segments_1");

            // Re-commit with different content bumps the epoch; flips carrying the old epoch must fail.
            provider.commitFile(new IndexFile("books__shard_0", "segments_1", 256, 9));
            var outcome = provider.compareAndSetStatus("books__shard_0", stale, IndexFileStatus.UPLOADING);
            assertFalse(outcome.flipped());
            assertNotNull(outcome.updated());
            assertEquals(stale.metadata().getEpoch() + 1, outcome.updated().metadata().getEpoch());
            assertEquals(IndexFileStatus.DIRTY, outcome.updated().metadata().getStatus());

            // A flip against a deleted entry reports the disappearance instead of resurrecting it.
            provider.deleteFile("books__shard_0", "segments_1");
            var gone = provider.compareAndSetStatus("books__shard_0", stale, IndexFileStatus.UPLOADING);
            assertFalse(gone.flipped());
            assertNull(gone.updated());
        } finally {
            client.close();
        }
    }

    @Test
    @EnabledIfEnvironmentVariable(named = "ETCD_TEST_ENDPOINTS", matches = ".+")
    public void compareAndSetStatusesReportsExactPerEntryOutcomes() {
        Client client = Client.builder().endpoints(System.getenv("ETCD_TEST_ENDPOINTS")).build();
        String namespace = "test-manifest/" + UUID.randomUUID();
        EtcdManifestMetadataManager provider = new EtcdManifestMetadataManager(
                EtcdManifestMetadataManager.Options.builder().namespace(namespace).build(), client);
        try {
            provider.commitFile(new IndexFile("books__shard_0", "_0.si", 128, 7));
            provider.commitFile(new IndexFile("books__shard_0", "segments_1", 64, 3));
            var fresh = provider.fileVersionsByName("books__shard_0", List.of("_0.si", "segments_1"));

            var outcomes = provider.compareAndSetStatuses("books__shard_0",
                    List.of(fresh.get("_0.si"), fresh.get("segments_1")), IndexFileStatus.UPLOADING);
            assertTrue(outcomes.stream().allMatch(outcome -> outcome.flipped()));
            assertTrue(outcomes.stream().allMatch(outcome -> outcome.updated() != null
                    && outcome.updated().metadata().getStatus() == IndexFileStatus.UPLOADING));

            // Re-commit one file with different content; a batch flip carrying the stale version
            // must report that entry as not flipped while still flipping the valid one.
            provider.commitFile(new IndexFile("books__shard_0", "_0.si", 256, 9));
            outcomes = provider.compareAndSetStatuses("books__shard_0",
                    List.of(fresh.get("_0.si"), fresh.get("segments_1")), IndexFileStatus.CLEAN);
            assertFalse(outcomes.get(0).flipped());
            assertTrue(outcomes.get(1).flipped());
            assertEquals(IndexFileStatus.CLEAN, provider.fileMetadata("books__shard_0", "segments_1").getStatus());
        } finally {
            client.close();
        }
    }

    @Test
    @EnabledIfEnvironmentVariable(named = "ETCD_TEST_ENDPOINTS", matches = ".+")
    public void maintainsShardPendingSummaryAndRepairsDrift() {
        Client client = Client.builder().endpoints(System.getenv("ETCD_TEST_ENDPOINTS")).build();
        String namespace = "test-manifest/" + UUID.randomUUID();
        EtcdManifestMetadataManager provider = new EtcdManifestMetadataManager(
                EtcdManifestMetadataManager.Options.builder().namespace(namespace).build(), client);
        try {
            assertNull(provider.shardSummary("books__shard_0"));

            provider.commitFile(new IndexFile("books__shard_0", "_0.si", 128, 7));
            provider.commitFile(new IndexFile("books__shard_0", "segments_1", 64, 3));
            assertEquals(2, provider.shardSummary("books__shard_0").pendingCount());

            var versions = provider.fileVersionsByName("books__shard_0", List.of("_0.si", "segments_1"));
            var outcomes = provider.compareAndSetStatuses("books__shard_0",
                    List.of(versions.get("_0.si"), versions.get("segments_1")), IndexFileStatus.UPLOADING);
            assertTrue(outcomes.stream().allMatch(outcome -> outcome.flipped()));
            assertEquals(2, provider.shardSummary("books__shard_0").pendingCount(),
                    "UPLOADING files are still pending");

            provider.compareAndSetStatuses("books__shard_0",
                    List.of(outcomes.get(0).updated(), outcomes.get(1).updated()), IndexFileStatus.CLEAN);
            assertEquals(0, provider.shardSummary("books__shard_0").pendingCount());

            long generation = provider.publishSnapshot("books__shard_0", "segments_1", List.of(
                    provider.fileMetadata("books__shard_0", "_0.si"),
                    provider.fileMetadata("books__shard_0", "segments_1")));
            assertEquals(generation, provider.shardSummary("books__shard_0").latestGeneration());

            // Full-scan repair restores a wiped or drifted summary.
            provider.putShardSummary("books__shard_0", new ShardSummary(99, 0, 0));
            provider.reconcileShardSummary("books__shard_0");
            ShardSummary repaired = provider.shardSummary("books__shard_0");
            assertEquals(0, repaired.pendingCount());
            assertEquals(generation, repaired.latestGeneration());

            provider.deleteAll("books__shard_0");
            assertNull(provider.shardSummary("books__shard_0"));
        } finally {
            client.close();
        }
    }

    @Test
    @EnabledIfEnvironmentVariable(named = "ETCD_TEST_ENDPOINTS", matches = ".+")
    public void deleteFilesRemovesOnlyTheNamedEntries() {
        Client client = Client.builder().endpoints(System.getenv("ETCD_TEST_ENDPOINTS")).build();
        String namespace = "test-manifest/" + UUID.randomUUID();
        EtcdManifestMetadataManager provider = new EtcdManifestMetadataManager(
                EtcdManifestMetadataManager.Options.builder().namespace(namespace).build(), client);
        try {
            provider.commitFile(new IndexFile("books__shard_0", "_0.si", 128, 7));
            provider.commitFile(new IndexFile("books__shard_0", "segments_1", 64, 3));

            provider.deleteFiles("books__shard_0", List.of("_0.si"));

            assertNull(provider.fileMetadata("books__shard_0", "_0.si"));
            assertNotNull(provider.fileMetadata("books__shard_0", "segments_1"));
        } finally {
            client.close();
        }
    }
    @Test
    @EnabledIfEnvironmentVariable(named = "ETCD_TEST_ENDPOINTS", matches = ".+")
    public void chunkedSnapshotsRoundTripThroughEtcd() {
        Client client = Client.builder().endpoints(System.getenv("ETCD_TEST_ENDPOINTS")).build();
        String namespace = "test-manifest/" + UUID.randomUUID();
        EtcdManifestMetadataManager provider = new EtcdManifestMetadataManager(
                EtcdManifestMetadataManager.Options.builder().namespace(namespace).build(), client);
        try {
            String index = "books__shard_0";
            List<IndexFileMetadata> files = new ArrayList<>();
            for (int i = 0; i < 300; i++) {
                String name = String.format("_%04d.cfs", i);
                provider.commitFile(new IndexFile(index, name, 100, i));
                provider.updateFileStatus(index, name, 1L, IndexFileStatus.UPLOADING);
                provider.updateFileStatus(index, name, 1L, IndexFileStatus.CLEAN);
                files.add(provider.fileMetadata(index, name));
            }

            long generation = provider.publishSnapshot(index, "segments_9", files);
            assertEquals(1, generation);

            // Header carries identity without the file list (3 chunks at 128 names each).
            var header = provider.latestSnapshotHeader(index);
            assertNotNull(header);
            assertEquals(1, header.generation());
            assertEquals(300, header.fileCount());
            assertEquals("segments_9", header.segmentFileName());
            assertEquals(1, provider.snapshotHeader(index, 1).generation());

            // Full snapshot reassembles the files from index_file/ entries.
            IndexCommitSnapshot snapshot = provider.snapshot(index, generation);
            assertNotNull(snapshot);
            assertEquals(300, snapshot.getFiles().size());
            assertEquals("_0007.cfs", snapshot.getFiles().get(7).getName());
            assertEquals(files.getFirst().getObjectKey(), snapshot.getFiles().getFirst().getObjectKey());
            assertEquals(1, provider.listSnapshots(index).size());

            // deleteSnapshot removes the header AND its name chunks.
            provider.deleteSnapshot(index, generation);
            assertNull(provider.snapshot(index, generation));
            assertNull(provider.latestSnapshotHeader(index));
        } finally {
            client.close();
        }
    }

    @Test
    @EnabledIfEnvironmentVariable(named = "ETCD_TEST_ENDPOINTS", matches = ".+")
    public void snapshotBeyondLegacySingleKeyLimitStillPublishes() {
        Client client = Client.builder().endpoints(System.getenv("ETCD_TEST_ENDPOINTS")).build();
        String namespace = "test-manifest/" + UUID.randomUUID();
        EtcdManifestMetadataManager provider = new EtcdManifestMetadataManager(
                EtcdManifestMetadataManager.Options.builder().namespace(namespace).build(), client);
        try {
            String index = "books__shard_0";
            // 6000 files x ~266 B = ~1.56 MB serialized: the legacy full-snapshot value already
            // exceeded etcd's default 1.5 MB --max-request-bytes here and permanently broke
            // every later commit for the shard.
            int count = 6000;
            List<IndexFileMetadata> files = new ArrayList<>(count);
            List<IndexFile> batch = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                batch.add(new IndexFile(index, String.format("_%06d.cfs", i), 100, i));
            }
            // The snapshot reassembles file metadata from index_file/ entries at read time,
            // so the entries must exist for the round trip below.
            provider.commitFiles(batch);
            for (IndexFile file : batch) {
                files.add(provider.fileMetadata(index, file.name()));
            }
            long generation = provider.publishSnapshot(index, "segments_big", files);

            var header = provider.latestSnapshotHeader(index);
            assertNotNull(header);
            assertEquals(count, header.fileCount());

            IndexCommitSnapshot snapshot = provider.snapshot(index, generation);
            assertNotNull(snapshot);
            assertEquals(count, snapshot.getFiles().size());
        } finally {
            client.close();
        }
    }

    @Test
    @EnabledIfEnvironmentVariable(named = "ETCD_TEST_ENDPOINTS", matches = ".+")
    public void orphanSnapshotFileSweepRemovesOnlyUnreferencedChunks() {
        Client client = Client.builder().endpoints(System.getenv("ETCD_TEST_ENDPOINTS")).build();
        String namespace = "test-manifest/" + UUID.randomUUID();
        EtcdManifestMetadataManager provider = new EtcdManifestMetadataManager(
                EtcdManifestMetadataManager.Options.builder().namespace(namespace).build(), client);
        try {
            String index = "books__shard_0";
            provider.commitFile(new IndexFile(index, "segments_1", 128, 7));
            var metadata = provider.fileMetadata(index, "segments_1");
            long generation = provider.publishSnapshot(index, "segments_1", List.of(metadata));

            // Simulate a crash between chunk writes and header creation. Keys built by hand
            // must use the normalized (leading-slash) namespace, exactly like the manager.
            ByteSequence orphanChunk = ByteSequence.from(
                    ("/" + namespace + "/snapshot_files/" + index + "/" + String.format("%020d", 999) + "/0000")
                            .getBytes(StandardCharsets.UTF_8));
            client.getKVClient().put(orphanChunk, ByteSequence.from("[\"ghost.cfs\"]".getBytes(StandardCharsets.UTF_8))).join();

            provider.deleteOrphanSnapshotFiles(index);

            long orphans = client.getKVClient()
                    .get(ByteSequence.from(("/" + namespace + "/snapshot_files/" + index + "/").getBytes(StandardCharsets.UTF_8)),
                            GetOption.builder().isPrefix(true).build())
                    .join().getKvs().stream()
                    .filter(kv -> kv.getKey().toString(StandardCharsets.UTF_8).contains(String.format("%020d", 999)))
                    .count();
            assertEquals(0, orphans);
            // Live snapshot chunks survive the sweep.
            assertNotNull(provider.snapshot(index, generation));
        } finally {
            client.close();
        }
    }
}
