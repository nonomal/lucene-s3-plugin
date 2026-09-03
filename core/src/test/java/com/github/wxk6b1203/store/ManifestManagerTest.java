package com.github.wxk6b1203.store;

import com.github.wxk6b1203.metadata.common.CommittingIndexFile;
import com.github.wxk6b1203.metadata.common.IndexCommitSnapshot;
import com.github.wxk6b1203.metadata.common.IndexFileStatus;
import com.github.wxk6b1203.metadata.provider.mem.MemMockProvider;
import com.github.wxk6b1203.store.manifest.ManifestManager;
import com.github.wxk6b1203.store.manifest.ManifestOptions;
import com.github.wxk6b1203.store.object.RemoteObjectStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ManifestManagerTest {
    @TempDir
    Path tempDir;

    @Test
    public void testMissingRemoteObjectStoreDoesNotMarkClean() throws Exception {
        MemMockProvider metadata = new MemMockProvider();
        ManifestOptions options = new ManifestOptions("bucket");
        ManifestManager manager = new ManifestManager(options, (software.amazon.awssdk.services.s3.S3Client) null, metadata);
        Path file = tempDir.resolve("segments_1");
        Files.write(file, new byte[]{1});

        manager.commit(List.of(new CommittingIndexFile("books", file)));
        Thread.sleep(200);
        manager.close();

        assertEquals(1, metadata.listAll("books", List.of(IndexFileStatus.DIRTY)).size());
        assertEquals(0, metadata.listAll("books", List.of(IndexFileStatus.CLEAN)).size());
    }

    @Test
    public void testSegmentsFileIsUploadedAfterDataFiles() throws Exception {
        MemMockProvider metadata = new MemMockProvider();
        RecordingRemoteObjectStore remote = new RecordingRemoteObjectStore();
        ManifestOptions options = new ManifestOptions("bucket");
        ManifestManager manager = new ManifestManager(options, remote, metadata);
        Path data = tempDir.resolve("_0.si");
        Path segments = tempDir.resolve("segments_1");
        Files.write(data, new byte[]{1});
        Files.write(segments, new byte[]{2});

        manager.commit(List.of(
                new CommittingIndexFile("books", data),
                new CommittingIndexFile("books", segments)
        ));
        waitUntil(() -> metadata.fileMetadata("books", "segments_1").getStatus() == IndexFileStatus.CLEAN);
        manager.close();

        assertEquals(2, remote.puts.size());
        assertTrue(remote.puts.get(0).startsWith("books/_data/_0.si."));
        assertTrue(remote.puts.get(1).startsWith("books/_data/segments_1."));
    }

    @Test
    public void testCleanCommitPublishesSnapshotWithFileObjectKeys() throws Exception {
        MemMockProvider metadata = new MemMockProvider();
        RecordingRemoteObjectStore remote = new RecordingRemoteObjectStore();
        ManifestManager manager = new ManifestManager(new ManifestOptions("bucket"), remote, metadata);
        Path data = tempDir.resolve("_0.si");
        Path segments = tempDir.resolve("segments_1");
        Files.write(data, new byte[]{1});
        Files.write(segments, new byte[]{2});

        manager.commit(List.of(
                new CommittingIndexFile("books", data),
                new CommittingIndexFile("books", segments)
        ));
        waitUntil(() -> metadata.latestSnapshot("books") != null);
        manager.close();

        IndexCommitSnapshot snapshot = metadata.latestSnapshot("books");
        assertEquals(1, snapshot.getGeneration());
        assertEquals("segments_1", snapshot.getSegmentFileName());
        assertEquals(List.of("_0.si", "segments_1"), snapshot.getFiles().stream()
                .map(file -> file.getName())
                .sorted()
                .toList());
        assertTrue(snapshot.getFiles().stream().allMatch(file -> file.getObjectKey().startsWith("books/_data/")));
    }

    @Test
    public void testSegmentsFileStaysDirtyWhenDataFileUploadFails() throws Exception {
        MemMockProvider metadata = new MemMockProvider();
        RecordingRemoteObjectStore remote = new RecordingRemoteObjectStore();
        remote.failName("_0.si");
        ManifestOptions options = new ManifestOptions("bucket");
        ManifestManager manager = new ManifestManager(options, remote, metadata);
        Path data = tempDir.resolve("_0.si");
        Path segments = tempDir.resolve("segments_1");
        Files.write(data, new byte[]{1});
        Files.write(segments, new byte[]{2});

        manager.commit(List.of(
                new CommittingIndexFile("books", data),
                new CommittingIndexFile("books", segments)
        ));
        waitUntil(() -> metadata.fileMetadata("books", "_0.si").getStatus() == IndexFileStatus.UPLOADING);
        manager.close();

        assertEquals(IndexFileStatus.UPLOADING, metadata.fileMetadata("books", "_0.si").getStatus());
        assertEquals(IndexFileStatus.DIRTY, metadata.fileMetadata("books", "segments_1").getStatus());
    }

    @Test
    public void testDirtyFilesCanBeRetriedWithoutNewMetadataEpoch() throws Exception {
        MemMockProvider metadata = new MemMockProvider();
        RecordingRemoteObjectStore remote = new RecordingRemoteObjectStore();
        ManifestOptions options = new ManifestOptions("bucket");
        ManifestManager manager = new ManifestManager(options, remote, metadata);
        Path data = tempDir.resolve("_0.si");
        Path segments = tempDir.resolve("segments_1");
        Files.write(data, new byte[]{1});
        Files.write(segments, new byte[]{2});

        remote.failName("_0.si");
        manager.commit(List.of(
                new CommittingIndexFile("books", data),
                new CommittingIndexFile("books", segments)
        ));
        waitUntil(() -> metadata.fileMetadata("books", "_0.si").getStatus() == IndexFileStatus.UPLOADING);

        remote.failName(null);
        manager.commit(List.of(
                new CommittingIndexFile("books", data),
                new CommittingIndexFile("books", segments)
        ));
        waitUntil(() -> metadata.fileMetadata("books", "segments_1").getStatus() == IndexFileStatus.CLEAN);
        manager.close();

        assertEquals(1, metadata.fileMetadata("books", "_0.si").getEpoch());
        assertEquals(1, metadata.fileMetadata("books", "segments_1").getEpoch());
        assertEquals(3, remote.puts.size());
        assertTrue(remote.puts.get(0).startsWith("books/_data/_0.si."));
        assertEquals(remote.puts.get(0), remote.puts.get(1));
        assertTrue(remote.puts.get(2).startsWith("books/_data/segments_1."));
    }

    @Test
    public void testDuplicatePendingUploadDoesNotUploadSameEpochTwiceOrPublishSegmentsEarly() throws Exception {
        MemMockProvider metadata = new MemMockProvider();
        BlockingRemoteObjectStore remote = new BlockingRemoteObjectStore("_0.si");
        ManifestOptions options = new ManifestOptions("bucket");
        ManifestManager manager = new ManifestManager(options, remote, metadata);
        Path data = tempDir.resolve("_0.si");
        Path segments = tempDir.resolve("segments_1");
        Files.write(data, new byte[]{1});
        Files.write(segments, new byte[]{2});

        manager.commit(List.of(
                new CommittingIndexFile("books", data),
                new CommittingIndexFile("books", segments)
        ));
        assertTrue(remote.awaitBlockedPut());

        manager.commit(List.of(
                new CommittingIndexFile("books", data),
                new CommittingIndexFile("books", segments)
        ));
        Thread.sleep(200);

        assertEquals(1, remote.puts.size());
        assertTrue(remote.puts.getFirst().startsWith("books/_data/_0.si."));
        assertEquals(IndexFileStatus.DIRTY, metadata.fileMetadata("books", "segments_1").getStatus());

        remote.releaseBlockedPut();
        waitUntil(() -> metadata.fileMetadata("books", "segments_1").getStatus() == IndexFileStatus.CLEAN);
        manager.close();

        assertEquals(2, remote.puts.size());
        assertTrue(remote.puts.get(1).startsWith("books/_data/segments_1."));
    }

    @Test
    public void testCloseWaitsForPendingAsyncUpload() throws Exception {
        MemMockProvider metadata = new MemMockProvider();
        BlockingRemoteObjectStore remote = new BlockingRemoteObjectStore("_0.si");
        ManifestManager manager = new ManifestManager(new ManifestOptions("bucket"), remote, metadata);
        Path data = tempDir.resolve("_0.si");
        Path segments = tempDir.resolve("segments_1");
        Files.write(data, new byte[]{1});
        Files.write(segments, new byte[]{2});

        manager.commit(List.of(
                new CommittingIndexFile("books", data),
                new CommittingIndexFile("books", segments)
        ));
        assertTrue(remote.awaitBlockedPut());

        CompletableFuture<Void> close = CompletableFuture.runAsync(manager::close);
        Thread.sleep(200);
        assertFalse(close.isDone());

        remote.releaseBlockedPut();
        close.get(5, TimeUnit.SECONDS);
        assertEquals(IndexFileStatus.CLEAN, metadata.fileMetadata("books", "segments_1").getStatus());
    }

    @Test
    public void testStaleDataUploadDoesNotPublishSegmentsAfterMetadataEpochChanges() throws Exception {
        MemMockProvider metadata = new MemMockProvider();
        BlockingRemoteObjectStore remote = new BlockingRemoteObjectStore("_0.si");
        ManifestOptions options = new ManifestOptions("bucket");
        ManifestManager manager = new ManifestManager(options, remote, metadata);
        Path data = tempDir.resolve("_0.si");
        Path segments = tempDir.resolve("segments_1");
        Files.write(data, new byte[]{1});
        Files.write(segments, new byte[]{2});

        manager.commit(List.of(
                new CommittingIndexFile("books", data),
                new CommittingIndexFile("books", segments)
        ));
        assertTrue(remote.awaitBlockedPut());

        Files.write(data, new byte[]{3});
        manager.commit(List.of(new CommittingIndexFile("books", data)));
        waitUntil(() -> metadata.fileMetadata("books", "_0.si").getEpoch() == 2
                && metadata.fileMetadata("books", "_0.si").getStatus() == IndexFileStatus.CLEAN);

        remote.releaseBlockedPut();
        Thread.sleep(200);
        manager.close();

        assertEquals(IndexFileStatus.DIRTY, metadata.fileMetadata("books", "segments_1").getStatus());
        assertFalse(remote.puts.stream().anyMatch(key -> key.contains("segments_1")));
    }

    @Test
    public void testDirtyMetadataIsReplacedWhenFileNameIsReusedWithDifferentContent() throws Exception {
        MemMockProvider metadata = new MemMockProvider();
        RecordingRemoteObjectStore remote = new RecordingRemoteObjectStore();
        ManifestOptions options = new ManifestOptions("bucket");
        ManifestManager manager = new ManifestManager(options, remote, metadata);
        Path data = tempDir.resolve("_0.si");
        Files.write(data, new byte[]{1});

        remote.failName("_0.si");
        manager.commit(List.of(new CommittingIndexFile("books", data)));
        waitUntil(() -> metadata.fileMetadata("books", "_0.si").getStatus() == IndexFileStatus.UPLOADING);

        remote.failName(null);
        Files.write(data, new byte[]{1, 2});
        manager.commit(List.of(new CommittingIndexFile("books", data)));
        waitUntil(() -> metadata.fileMetadata("books", "_0.si").getStatus() == IndexFileStatus.CLEAN);
        manager.close();

        assertEquals(2, metadata.fileMetadata("books", "_0.si").getEpoch());
        assertEquals(2, metadata.fileMetadata("books", "_0.si").getSize());
    }

    @Test
    public void testDirtyMetadataUsesChecksumWhenFileNameSizeAndModifiedTimeAreReused() throws Exception {
        MemMockProvider metadata = new MemMockProvider();
        RecordingRemoteObjectStore remote = new RecordingRemoteObjectStore();
        ManifestOptions options = new ManifestOptions("bucket");
        ManifestManager manager = new ManifestManager(options, remote, metadata);
        Path data = tempDir.resolve("_0.si");
        Files.write(data, new byte[]{1, 2});
        FileTime modifiedTime = Files.getLastModifiedTime(data);

        remote.failName("_0.si");
        manager.commit(List.of(new CommittingIndexFile("books", data)));
        waitUntil(() -> metadata.fileMetadata("books", "_0.si").getStatus() == IndexFileStatus.UPLOADING);
        long firstChecksum = metadata.fileMetadata("books", "_0.si").getChecksum();

        remote.failName(null);
        Files.write(data, new byte[]{3, 4});
        Files.setLastModifiedTime(data, modifiedTime);
        manager.commit(List.of(new CommittingIndexFile("books", data)));
        waitUntil(() -> metadata.fileMetadata("books", "_0.si").getStatus() == IndexFileStatus.CLEAN);
        manager.close();

        assertEquals(2, metadata.fileMetadata("books", "_0.si").getEpoch());
        assertEquals(2, metadata.fileMetadata("books", "_0.si").getSize());
        assertTrue(firstChecksum != metadata.fileMetadata("books", "_0.si").getChecksum());
    }

    @Test
    public void testDeleteIndexShardsRemovesRemoteObjectsAndMetadata() throws Exception {
        MemMockProvider metadata = new MemMockProvider();
        RecordingRemoteObjectStore remote = new RecordingRemoteObjectStore();
        ManifestManager manager = new ManifestManager(new ManifestOptions("bucket"), remote, metadata);
        Path data = tempDir.resolve("_0.si");
        Path segments = tempDir.resolve("segments_1");
        Files.write(data, new byte[]{1});
        Files.write(segments, new byte[]{2});

        manager.commit(List.of(
                new CommittingIndexFile("books__shard_0", data),
                new CommittingIndexFile("books__shard_0", segments)
        ));
        waitUntil(() -> metadata.fileMetadata("books__shard_0", "segments_1").getStatus() == IndexFileStatus.CLEAN);

        manager.deleteIndexShards("books", 1);
        manager.close();

        assertEquals(2, Set.copyOf(remote.deletes).size());
        assertTrue(remote.deletes.stream().anyMatch(key -> key.startsWith("books__shard_0/_data/_0.si.")));
        assertTrue(remote.deletes.stream().anyMatch(key -> key.startsWith("books__shard_0/_data/segments_1.")));
        assertTrue(metadata.listAll("books__shard_0", List.of(
                IndexFileStatus.DIRTY,
                IndexFileStatus.UPLOADING,
                IndexFileStatus.CLEAN,
                IndexFileStatus.PINNED
        )).isEmpty());
    }

    @Test
    public void testSnapshotGcRetainsLatestAndPinnedSnapshots() throws Exception {
        MemMockProvider metadata = new MemMockProvider();
        RecordingRemoteObjectStore remote = new RecordingRemoteObjectStore();
        ManifestManager manager = new ManifestManager(new ManifestOptions("bucket"), remote, metadata);
        Path data = tempDir.resolve("_0.si");
        Path secondData = tempDir.resolve("_1.si");
        Path firstSegments = tempDir.resolve("segments_1");
        Path secondSegments = tempDir.resolve("segments_2");
        Files.write(data, new byte[]{1});
        Files.write(firstSegments, new byte[]{2});

        manager.commit(List.of(
                new CommittingIndexFile("books", data),
                new CommittingIndexFile("books", firstSegments)
        ));
        waitUntil(() -> metadata.latestSnapshot("books") != null);
        long firstGeneration = metadata.latestSnapshot("books").getGeneration();
        String firstDataObjectKey = metadata.latestSnapshot("books").getFiles().stream()
                .filter(file -> file.getName().equals("_0.si"))
                .findFirst()
                .orElseThrow()
                .getObjectKey();
        String firstSegmentObjectKey = metadata.latestSnapshot("books").getFiles().stream()
                .filter(file -> file.getName().equals("segments_1"))
                .findFirst()
                .orElseThrow()
                .getObjectKey();
        manager.pinSnapshot("books", firstGeneration, "pit-1", System.currentTimeMillis() + 60_000);

        Files.write(secondData, new byte[]{3, 4});
        Files.write(secondSegments, new byte[]{5});
        manager.commit(List.of(
                new CommittingIndexFile("books", secondData),
                new CommittingIndexFile("books", secondSegments)
        ));
        waitUntil(() -> metadata.latestSnapshot("books") != null
                && metadata.latestSnapshot("books").getGeneration() > firstGeneration);

        manager.garbageCollectSnapshots("books", 1);
        assertEquals(2, metadata.listSnapshots("books").size());
        assertFalse(remote.deletes.contains(firstDataObjectKey));

        manager.releaseSnapshotPin("books", "pit-1");
        manager.garbageCollectSnapshots("books", 1);
        manager.close();

        assertEquals(1, metadata.listSnapshots("books").size());
        assertTrue(remote.deletes.contains(firstSegmentObjectKey));
        assertTrue(remote.deletes.contains(firstDataObjectKey));
    }

    @Test
    public void testSnapshotGarbageCollectionCompactsStaleCleanEntries() throws Exception {
        MemMockProvider metadata = new MemMockProvider();
        RecordingRemoteObjectStore remote = new RecordingRemoteObjectStore();
        ManifestManager manager = new ManifestManager(new ManifestOptions("bucket"), remote, metadata);
        long now = System.currentTimeMillis();
        long graceExpired = now - Duration.ofMinutes(11).toMillis();

        // Snapshot generation 1 references _0.si + segments_1 (both CLEAN).
        var data = new com.github.wxk6b1203.metadata.common.IndexFile(
                "books__shard_0", "_0.si", "_data", "books__shard_0/_data/_0.si.aaa.1", 1, 10, now);
        var segments = new com.github.wxk6b1203.metadata.common.IndexFile(
                "books__shard_0", "segments_1", "_data", "books__shard_0/_data/segments_1.bbb.1", 1, 20, now);
        metadata.commitFile(data);
        metadata.commitFile(segments);
        markClean(metadata, "_0.si");
        markClean(metadata, "segments_1");
        metadata.publishSnapshot("books__shard_0", "segments_1", List.of(
                metadata.fileMetadata("books__shard_0", "_0.si"),
                metadata.fileMetadata("books__shard_0", "segments_1")));

        // A stale-history entry: CLEAN, not referenced by any retained snapshot, older than the
        // compaction grace window (its object was already unreferenced and GC'd).
        var stale = new com.github.wxk6b1203.metadata.common.IndexFile(
                "books__shard_0", "_9.old", "_data", "books__shard_0/_data/_9.old.ccc.1", 1, 30, graceExpired);
        metadata.commitFile(stale);
        markClean(metadata, "_9.old");

        manager.garbageCollectSnapshots("books__shard_0", 1);

        // Compacted: stale history entry removed...
        assertNull(metadata.fileMetadata("books__shard_0", "_9.old"));
        // ...while the live lineage's entries survive.
        assertEquals(IndexFileStatus.CLEAN, metadata.fileMetadata("books__shard_0", "_0.si").getStatus());
        assertEquals(IndexFileStatus.CLEAN, metadata.fileMetadata("books__shard_0", "segments_1").getStatus());
        assertEquals(1, metadata.listSnapshots("books__shard_0").size());
        manager.close();
    }

    /** DIRTY → UPLOADING → CLEAN, mirroring the upload state machine. */
    private void markClean(MemMockProvider metadata, String name) {
        metadata.updateFileStatus("books__shard_0", name, 1, IndexFileStatus.UPLOADING);
        metadata.updateFileStatus("books__shard_0", name, 1, IndexFileStatus.CLEAN);
    }

    private void waitUntil(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(20);
        }
        throw new AssertionError("condition was not met before timeout");
    }

    private static class RecordingRemoteObjectStore implements RemoteObjectStore {
        protected final List<String> puts = Collections.synchronizedList(new ArrayList<>());
        protected final List<String> deletes = Collections.synchronizedList(new ArrayList<>());
        private String failedName;

        private void failName(String failedName) {
            this.failedName = failedName;
        }

        @Override
        public void put(String key, Path source) throws IOException {
            puts.add(key);
            if (failedName != null && key.contains(failedName)) {
                throw new IOException("upload intentionally failed");
            }
        }

        @Override
        public void get(String key, Path target) throws IOException {
            throw new IOException("remote get is not configured in this test");
        }

        @Override
        public void delete(Collection<String> keys) {
            deletes.addAll(keys);
        }
    }

    private static class BlockingRemoteObjectStore extends RecordingRemoteObjectStore {
        private final String blockedName;
        private final CountDownLatch blockedPutStarted = new CountDownLatch(1);
        private final CountDownLatch releaseBlockedPut = new CountDownLatch(1);
        private final AtomicBoolean blocked = new AtomicBoolean();

        private BlockingRemoteObjectStore(String blockedName) {
            this.blockedName = blockedName;
        }

        @Override
        public void put(String key, Path source) throws IOException {
            super.put(key, source);
            if (key.contains(blockedName) && blocked.compareAndSet(false, true)) {
                blockedPutStarted.countDown();
                try {
                    if (!releaseBlockedPut.await(5, TimeUnit.SECONDS)) {
                        throw new IOException("blocked put was not released");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("interrupted while waiting for blocked put release", e);
                }
            }
        }

        protected boolean awaitBlockedPut() throws InterruptedException {
            return blockedPutStarted.await(5, TimeUnit.SECONDS);
        }

        protected void releaseBlockedPut() {
            releaseBlockedPut.countDown();
        }
    }

}
