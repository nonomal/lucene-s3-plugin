package com.github.wxk6b1203.index;

import com.github.wxk6b1203.cluster.FieldMapping;
import com.github.wxk6b1203.cluster.ShardId;
import com.github.wxk6b1203.metadata.common.IndexFile;
import com.github.wxk6b1203.metadata.common.IndexFileStatus;
import com.github.wxk6b1203.metadata.provider.mem.MemMockProvider;
import com.github.wxk6b1203.search.ByQueryRequest;
import com.github.wxk6b1203.search.SearchHit;
import com.github.wxk6b1203.search.SearchRequest;
import com.github.wxk6b1203.search.VectorQuery;
import com.github.wxk6b1203.store.common.PathUtil;
import com.github.wxk6b1203.store.object.LocalFileRemoteObjectStore;
import com.github.wxk6b1203.store.object.RemoteObjectStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

public class LuceneLocalShardIndexServiceTest {
    @TempDir
    Path tempDir;

    @AfterEach
    void cleanupTempDirChildren() throws Exception {
        deleteChildrenWithRetry(tempDir);
    }

    @Test
    public void testIndexDocumentCommitsAndUploadsShardFiles() throws Exception {
        MemMockProvider metadata = new MemMockProvider();
        ShardId shardId = new ShardId("books", 0);
        Path remoteRoot = tempDir.resolve("remote");
        try (LuceneLocalShardIndexService service = new LuceneLocalShardIndexService(
                tempDir,
                "bucket",
                metadata,
                new LocalFileRemoteObjectStore(remoteRoot)
        )) {
            IndexDocumentResponse response = service.index(new IndexDocumentRequest(
                    "books",
                    shardId,
                    "doc-1",
                    Map.of("title", "Lucene")
            ));

            assertEquals("indexed", response.result());
            assertEquals("doc-1", response.id());
            waitForCleanSnapshot(metadata, "books__shard_0");
            assertTrue(hasUploadedSegment(remoteRoot.resolve("books__shard_0").resolve("_data")));
        }
    }

    @Test
    public void testSearchLocalShardById() throws Exception {
        MemMockProvider metadata = new MemMockProvider();
        ShardId shardId = new ShardId("books", 0);
        try (LuceneLocalShardIndexService service = new LuceneLocalShardIndexService(
                tempDir,
                "bucket",
                metadata,
                new LocalFileRemoteObjectStore(tempDir.resolve("remote"))
        )) {
            service.index(new IndexDocumentRequest("books", shardId, "doc-1", Map.of("title", "Lucene")));
            service.index(new IndexDocumentRequest("books", shardId, "doc-2", Map.of("title", "S3")));

            var response = service.search(
                    shardId,
                    new SearchRequest("books", Map.of("term", Map.of("_id", "doc-1")), List.of(), null, null, 0, 10)
            );

            assertEquals(1, response.hits().size());
            assertTrue(response.tookMillis() > 0);
            assertEquals("doc-1", response.hits().getFirst().id());
            assertEquals("Lucene", response.hits().getFirst().source().get("title"));
        }
    }

    @Test
    public void existsTermsAndPrefixQueriesFilterMappedFields() throws Exception {
        MemMockProvider metadata = new MemMockProvider();
        ShardId shardId = new ShardId("books", 0);
        Map<String, FieldMapping> mappings = Map.of(
                "category", new FieldMapping("keyword", null, null, true, true),
                "pages", new FieldMapping("integer", null, null, true, true),
                "indexed_only_keyword", new FieldMapping("keyword", null, null, true, true, false),
                "indexed_only_number", new FieldMapping("integer", null, null, true, true, false),
                "embedding", new FieldMapping("dense_vector", 2, "cosine", true, true)
        );
        try (LuceneLocalShardIndexService service = new LuceneLocalShardIndexService(
                tempDir,
                "bucket",
                metadata,
                new LocalFileRemoteObjectStore(tempDir.resolve("remote"))
        )) {
            service.index(new IndexDocumentRequest(
                    "books",
                    shardId,
                    "doc-1",
                    Map.of(
                            "title", "Lucene",
                            "category", "search",
                            "pages", 120,
                            "indexed_only_keyword", "visible",
                            "embedding", List.of(1.0f, 0.0f)
                    ),
                    mappings
            ));
            service.index(new IndexDocumentRequest(
                    "books",
                    shardId,
                    "doc-2",
                    Map.of(
                            "title", "S3",
                            "category", "storage",
                            "pages", 90,
                            "indexed_only_number", 7
                    ),
                    mappings
            ));
            service.index(new IndexDocumentRequest(
                    "books",
                    shardId,
                    "doc-3",
                    Map.of("title", "Draft"),
                    mappings
            ));

            var terms = service.search(
                    shardId,
                    new SearchRequest(
                            "books",
                            Map.of("terms", Map.of("category", List.of("search", "missing"))),
                            List.of(),
                            null,
                            null,
                            0,
                            10,
                            mappings
                    )
            );
            assertEquals(List.of("doc-1"), terms.hits().stream().map(SearchHit::id).toList());

            var prefix = service.search(
                    shardId,
                    new SearchRequest(
                            "books",
                            Map.of("prefix", Map.of("category", Map.of("value", "stor"))),
                            List.of(),
                            null,
                            null,
                            0,
                            10,
                            mappings
                    )
            );
            assertEquals(List.of("doc-2"), prefix.hits().stream().map(SearchHit::id).toList());

            var exists = service.search(
                    shardId,
                    new SearchRequest(
                            "books",
                            Map.of("exists", Map.of("field", "pages")),
                            List.of(),
                            null,
                            null,
                            0,
                            10,
                            mappings
                    )
            );
            assertEquals(List.of("doc-1", "doc-2"), exists.hits().stream().map(SearchHit::id).toList());

            var existsIndexedOnlyKeyword = service.search(
                    shardId,
                    new SearchRequest(
                            "books",
                            Map.of("exists", Map.of("field", "indexed_only_keyword")),
                            List.of(),
                            null,
                            null,
                            0,
                            10,
                            mappings
                    )
            );
            assertEquals(List.of("doc-1"), existsIndexedOnlyKeyword.hits().stream().map(SearchHit::id).toList());

            var existsIndexedOnlyNumber = service.search(
                    shardId,
                    new SearchRequest(
                            "books",
                            Map.of("exists", Map.of("field", "indexed_only_number")),
                            List.of(),
                            null,
                            null,
                            0,
                            10,
                            mappings
                    )
            );
            assertEquals(List.of("doc-2"), existsIndexedOnlyNumber.hits().stream().map(SearchHit::id).toList());

            var existsVector = service.search(
                    shardId,
                    new SearchRequest(
                            "books",
                            Map.of("exists", Map.of("field", "embedding")),
                            List.of(),
                            null,
                            null,
                            0,
                            10,
                            mappings
                    )
            );
            assertEquals(List.of("doc-1"), existsVector.hits().stream().map(SearchHit::id).toList());
        }
    }

    @Test
    public void bulkCreateDeleteCreateSameIdFollowsItemOrder() throws Exception {
        MemMockProvider metadata = new MemMockProvider();
        ShardId shardId = new ShardId("books", 0);
        try (LuceneLocalShardIndexService service = new LuceneLocalShardIndexService(
                tempDir,
                "bucket",
                metadata,
                new LocalFileRemoteObjectStore(tempDir.resolve("remote"))
        )) {
            List<IndexDocumentOperationResult> results = service.bulk(List.of(
                    new IndexDocumentOperation("create", new IndexDocumentRequest(
                            "books",
                            shardId,
                            "doc-1",
                            Map.of("title", "first"),
                            Map.of(),
                            true
                    )),
                    new IndexDocumentOperation("delete", new IndexDocumentRequest(
                            "books",
                            shardId,
                            "doc-1",
                            Map.of()
                    )),
                    new IndexDocumentOperation("create", new IndexDocumentRequest(
                            "books",
                            shardId,
                            "doc-1",
                            Map.of("title", "second"),
                            Map.of(),
                            true
                    ))
            ));

            assertEquals(3, results.size());
            assertFalse(results.get(0).failed());
            assertFalse(results.get(1).failed());
            assertFalse(results.get(2).failed());

            var response = service.search(
                    shardId,
                    new SearchRequest("books", Map.of("term", Map.of("_id", "doc-1")), List.of(), null, null, 0, 10)
            );

            assertEquals(1, response.hits().size());
            assertEquals("second", response.hits().getFirst().source().get("title"));
        }
    }

    @Test
    public void testRemoteReadUsesLastCleanSnapshotWhenNewerCommitIsUploading() throws Exception {
        MemMockProvider metadata = new MemMockProvider();
        ShardId shardId = new ShardId("books", 0);
        ControlledRemoteObjectStore remote = new ControlledRemoteObjectStore(
                new LocalFileRemoteObjectStore(tempDir.resolve("remote"))
        );
        try (LuceneLocalShardIndexService service = new LuceneLocalShardIndexService(
                tempDir,
                "bucket",
                metadata,
                remote
        )) {
            service.index(new IndexDocumentRequest("books", shardId, "doc-1", Map.of("title", "clean")));
            waitForCleanSegment(metadata, "books__shard_0");

            remote.failPuts(true);
            service.index(new IndexDocumentRequest("books", shardId, "doc-2", Map.of("title", "uploading")));
            waitForUncleanSegment(metadata, "books__shard_0");

            var weak = service.search(
                    shardId,
                    new SearchRequest("books", Map.of("match_all", Map.of()), List.of(), null, null, 0, 10)
            );
            var remoteOnly = service.search(
                    shardId,
                    new SearchRequest(
                            "books",
                            Map.of("match_all", Map.of()),
                            List.of(),
                            null,
                            null,
                            0,
                            10,
                            List.of(),
                            List.of(),
                            null,
                            Map.of(),
                            "remote"
                    )
            );

            List<String> weakIds = weak.hits().stream().map(hit -> hit.id()).toList();
            List<String> remoteIds = remoteOnly.hits().stream().map(hit -> hit.id()).toList();
            assertTrue(weakIds.contains("doc-1"));
            assertTrue(weakIds.contains("doc-2"));
            assertEquals(List.of("doc-1"), remoteIds);
        }
    }

    @Test
    public void cleanupIdleResourcesClosesExpiredRemoteSearchersWithoutNewSearch() throws Exception {
        MemMockProvider metadata = new MemMockProvider();
        ShardId shardId = new ShardId("books", 0);
        try (LuceneLocalShardIndexService service = new LuceneLocalShardIndexService(
                tempDir,
                "bucket",
                metadata,
                new LocalFileRemoteObjectStore(tempDir.resolve("remote"))
        )) {
            service.index(new IndexDocumentRequest("books", shardId, "doc-1", Map.of("title", "clean")));
            waitForCleanSnapshot(metadata, "books__shard_0");

            assertEquals(List.of("doc-1"), remoteMatchAll(service, shardId).hits().stream().map(hit -> hit.id()).toList());
            Map<?, ?> remoteSearchers = remoteSearchers(service);
            assertEquals(1, remoteSearchers.size());

            ageRemoteSearchers(remoteSearchers);
            service.cleanupIdleResources();

            assertTrue(remoteSearchers.isEmpty());
        }
    }

    @Test
    public void testFailedSnapshotPinIsReleasedAfterLaterCleanPublish() throws Exception {
        MemMockProvider metadata = new MemMockProvider();
        ShardId shardId = new ShardId("books", 0);
        ControlledRemoteObjectStore remote = new ControlledRemoteObjectStore(
                new LocalFileRemoteObjectStore(tempDir.resolve("remote"))
        );
        Path walPath = PathUtil.walDataPath(tempDir, "books__shard_0");
        try (LuceneLocalShardIndexService service = new LuceneLocalShardIndexService(
                tempDir,
                "bucket",
                metadata,
                remote
        )) {
            remote.failPuts(true);
            service.index(new IndexDocumentRequest("books", shardId, "doc-1", Map.of("title", "failed-upload")));
            waitForUncleanSegment(metadata, "books__shard_0");

            remote.failPuts(false);
            service.index(new IndexDocumentRequest("books", shardId, "doc-2", Map.of("title", "clean-upload")));
            waitForCleanSegment(metadata, "books__shard_0");

            waitForCommittedSegmentFileCount(walPath, 1);
        }
    }

    @Test
    public void faultyRemoteObjectStoreRetriesTransientUploadFailure() throws Exception {
        MemMockProvider metadata = new MemMockProvider();
        ShardId shardId = new ShardId("books", 0);
        FaultyRemoteObjectStore remote = new FaultyRemoteObjectStore(
                new LocalFileRemoteObjectStore(tempDir.resolve("remote"))
        );
        try (LuceneLocalShardIndexService service = new LuceneLocalShardIndexService(
                tempDir,
                "bucket",
                metadata,
                remote
        )) {
            remote.failNextPuts(1);
            service.index(new IndexDocumentRequest("books", shardId, "doc-1", Map.of("title", "transient")));
            waitForUncleanSegment(metadata, "books__shard_0");
            assertEquals(1, remote.putFailures());

            service.retryPendingUploads(List.of(shardId));
            waitForCleanSnapshot(metadata, "books__shard_0");

            var remoteOnly = service.search(
                    shardId,
                    new SearchRequest(
                            "books",
                            Map.of("match_all", Map.of()),
                            List.of(),
                            null,
                            null,
                            0,
                            10,
                            List.of(),
                            List.of(),
                            null,
                            Map.of(),
                            "remote"
                    )
            );
            assertEquals(List.of("doc-1"), remoteOnly.hits().stream().map(SearchHit::id).toList());
        }
    }

    @Test
    public void deferredCommitRefreshesLocallyBeforeRemotePublish() throws Exception {
        MemMockProvider metadata = new MemMockProvider();
        ShardId shardId = new ShardId("books", 0);
        IndexWriteOptions writeOptions = new IndexWriteOptions(
                false,
                2,
                Duration.ofHours(1),
                IndexWriteOptions.RefreshPolicy.INTERVAL,
                Duration.ofMillis(1)
        );
        try (LuceneLocalShardIndexService service = new LuceneLocalShardIndexService(
                tempDir,
                "bucket",
                metadata,
                new LocalFileRemoteObjectStore(tempDir.resolve("remote")),
                new com.github.wxk6b1203.store.manifest.ManifestOptions("bucket"),
                null,
                writeOptions
        )) {
            service.index(new IndexDocumentRequest("books", shardId, "doc-1", Map.of("title", "deferred")));

            assertEquals(0, matchAll(service, shardId).hits().size());
            Thread.sleep(5);
            service.runWriteMaintenance();
            assertEquals(List.of("doc-1"), matchAll(service, shardId).hits().stream().map(hit -> hit.id()).toList());
            assertEquals(0, remoteMatchAll(service, shardId).hits().size());

            service.index(new IndexDocumentRequest("books", shardId, "doc-2", Map.of("title", "published")));
            waitForCleanSnapshot(metadata, "books__shard_0");
            assertEquals(
                    List.of("doc-1", "doc-2"),
                    remoteMatchAll(service, shardId).hits().stream().map(hit -> hit.id()).sorted().toList()
            );
        }
    }

    @Test
    public void testNewOwnerWritesFromLastCleanSnapshotWhenPreviousOwnerHasUnuploadedCommit() throws Exception {
        MemMockProvider metadata = new MemMockProvider();
        ShardId shardId = new ShardId("books", 0);
        ControlledRemoteObjectStore remote = new ControlledRemoteObjectStore(
                new LocalFileRemoteObjectStore(tempDir.resolve("remote"))
        );

        try (LuceneLocalShardIndexService firstOwner = new LuceneLocalShardIndexService(
                tempDir.resolve("node-1"),
                "bucket",
                metadata,
                remote
        )) {
            firstOwner.index(new IndexDocumentRequest("books", shardId, "doc-1", Map.of("title", "clean")));
            waitForCleanSnapshot(metadata, "books__shard_0");

            remote.failPuts(true);
            firstOwner.index(new IndexDocumentRequest("books", shardId, "doc-2", Map.of("title", "lost")));
            waitForUncleanSegment(metadata, "books__shard_0");
        }

        remote.failPuts(false);
        try (LuceneLocalShardIndexService secondOwner = new LuceneLocalShardIndexService(
                tempDir.resolve("node-2"),
                "bucket",
                metadata,
                remote
        )) {
            secondOwner.index(new IndexDocumentRequest("books", shardId, "doc-3", Map.of("title", "recovered")));
            waitForCleanSnapshot(metadata, "books__shard_0");

            var response = secondOwner.search(
                    shardId,
                    new SearchRequest(
                            "books",
                            Map.of("match_all", Map.of()),
                            List.of(),
                            null,
                            null,
                            0,
                            10,
                            List.of(),
                            List.of(),
                            null,
                            Map.of(),
                            "remote"
                    )
            );
            List<String> ids = response.hits().stream().map(hit -> hit.id()).toList();
            assertTrue(ids.contains("doc-1"));
            assertTrue(ids.contains("doc-3"));
            assertFalse(ids.contains("doc-2"));
        }
    }

    @Test
    public void testShardReadsAndWritesFromCleanRemoteSnapshotWhenLocalDataIsLostEverywhere() throws Exception {
        MemMockProvider metadata = new MemMockProvider();
        ShardId shardId = new ShardId("books", 0);
        RemoteObjectStore remote = new LocalFileRemoteObjectStore(tempDir.resolve("remote"));

        try (LuceneLocalShardIndexService firstOwner = new LuceneLocalShardIndexService(
                tempDir.resolve("node-1"),
                "bucket",
                metadata,
                remote
        )) {
            firstOwner.index(new IndexDocumentRequest("books", shardId, "doc-1", Map.of("title", "remote-clean")));
            waitForCleanSnapshot(metadata, "books__shard_0");
        }

        try (LuceneLocalShardIndexService recoveredOwner = new LuceneLocalShardIndexService(
                tempDir.resolve("node-2-empty-local"),
                "bucket",
                metadata,
                remote
        )) {
            var remoteRead = recoveredOwner.search(
                    shardId,
                    new SearchRequest(
                            "books",
                            Map.of("match_all", Map.of()),
                            List.of(),
                            null,
                            null,
                            0,
                            10,
                            List.of(),
                            List.of(),
                            null,
                            Map.of(),
                            "remote"
                    )
            );
            assertEquals(List.of("doc-1"), remoteRead.hits().stream().map(hit -> hit.id()).toList());

            recoveredOwner.index(new IndexDocumentRequest("books", shardId, "doc-2", Map.of("title", "recovered")));
            waitForCleanSnapshot(metadata, "books__shard_0");
            var afterWrite = recoveredOwner.search(
                    shardId,
                    new SearchRequest("books", Map.of("match_all", Map.of()), List.of(), null, null, 0, 10)
            );
            assertEquals(List.of("doc-1", "doc-2"), afterWrite.hits().stream().map(hit -> hit.id()).sorted().toList());
        }
    }

    @Test
    public void testRetryPendingUploadsDiscardsUnrecoverablePendingMetadataWithoutWal() throws Exception {
        MemMockProvider metadata = new MemMockProvider();
        ShardId shardId = new ShardId("books", 0);
        String physicalIndexName = "books__shard_0";
        metadata.commitFile(new IndexFile(physicalIndexName, "_1.si", 1, 7));
        long segmentEpoch = metadata.commitFile(new IndexFile(physicalIndexName, "segments_2", 1, 9)).getEpoch();
        metadata.updateFileStatus(physicalIndexName, "segments_2", segmentEpoch, IndexFileStatus.UPLOADING);

        try (LuceneLocalShardIndexService service = new LuceneLocalShardIndexService(
                tempDir.resolve("node-2"),
                "bucket",
                metadata,
                new LocalFileRemoteObjectStore(tempDir.resolve("remote"))
        )) {
            service.retryPendingUploads(List.of(shardId));
        }

        assertTrue(metadata.listAll(physicalIndexName, List.of(
                IndexFileStatus.DIRTY,
                IndexFileStatus.UPLOADING
        )).isEmpty());
    }

    @Test
    public void testDeleteIndexClosesWritersAndRemovesLocalShardData() throws Exception {
        MemMockProvider metadata = new MemMockProvider();
        ShardId shardId = new ShardId("books", 0);
        try (LuceneLocalShardIndexService service = new LuceneLocalShardIndexService(
                tempDir,
                "bucket",
                metadata,
                new LocalFileRemoteObjectStore(tempDir.resolve("remote"))
        )) {
            service.index(new IndexDocumentRequest("books", shardId, "doc-1", Map.of("title", "Lucene")));
            waitForCleanSnapshot(metadata, "books__shard_0");

            service.deleteIndex("books", 1);

            assertFalse(Files.exists(PathUtil.walDataPath(tempDir, "books__shard_0")));
            assertFalse(Files.exists(PathUtil.sharedDataPath(tempDir, "books__shard_0")));
            assertFalse(Files.exists(PathUtil.sharedTempPath(tempDir, "books__shard_0")));
        }
    }

    @Test
    public void testDeleteByQueryRemovesMatchingDocs() throws Exception {
        MemMockProvider metadata = new MemMockProvider();
        ShardId shardId = new ShardId("books", 0);
        try (LuceneLocalShardIndexService service = new LuceneLocalShardIndexService(
                tempDir,
                "bucket",
                metadata,
                new LocalFileRemoteObjectStore(tempDir.resolve("remote"))
        )) {
            service.index(new IndexDocumentRequest("books", shardId, "doc-1", Map.of("title", "Lucene")));
            service.index(new IndexDocumentRequest("books", shardId, "doc-2", Map.of("title", "S3")));

            var delete = service.deleteByQuery(
                    shardId,
                    new ByQueryRequest("books", Map.of("term", Map.of("_id", "doc-1")), Map.of(), null, false)
            );
            var search = service.search(
                    shardId,
                    new SearchRequest("books", Map.of("match_all", Map.of()), List.of(), null, null, 0, 10)
            );

            assertEquals("deleted=1", delete.status());
            assertEquals(1, search.hits().size());
            assertEquals("doc-2", search.hits().getFirst().id());
        }
    }

    @Test
    public void testUpdateByQueryMergesSource() throws Exception {
        MemMockProvider metadata = new MemMockProvider();
        ShardId shardId = new ShardId("books", 0);
        try (LuceneLocalShardIndexService service = new LuceneLocalShardIndexService(
                tempDir,
                "bucket",
                metadata,
                new LocalFileRemoteObjectStore(tempDir.resolve("remote"))
        )) {
            service.index(new IndexDocumentRequest("books", shardId, "doc-1", Map.of("title", "Lucene")));

            var update = service.updateByQuery(
                    shardId,
                    new ByQueryRequest(
                            "books",
                            Map.of("term", Map.of("_id", "doc-1")),
                            Map.of("title", "Lucene S3", "version", 2),
                            null,
                            false
                    )
            );
            var search = service.search(
                    shardId,
                    new SearchRequest("books", Map.of("term", Map.of("_id", "doc-1")), List.of(), null, null, 0, 10)
            );

            assertEquals("updated=1", update.status());
            assertEquals("Lucene S3", search.hits().getFirst().source().get("title"));
            assertEquals(2, search.hits().getFirst().source().get("version"));
        }
    }

    @Test
    public void testUpdateByQueryWithNoMatchesReturnsZero() throws Exception {
        MemMockProvider metadata = new MemMockProvider();
        ShardId shardId = new ShardId("books", 0);
        try (LuceneLocalShardIndexService service = new LuceneLocalShardIndexService(
                tempDir,
                "bucket",
                metadata,
                new LocalFileRemoteObjectStore(tempDir.resolve("remote"))
        )) {
            service.index(new IndexDocumentRequest("books", shardId, "doc-1", Map.of("title", "Lucene")));

            var update = service.updateByQuery(
                    shardId,
                    new ByQueryRequest(
                            "books",
                            Map.of("term", Map.of("_id", "missing")),
                            Map.of("title", "Changed"),
                            null,
                            false
                    )
            );
            var search = service.search(
                    shardId,
                    new SearchRequest("books", Map.of("term", Map.of("_id", "doc-1")), List.of(), null, null, 0, 10)
            );

            assertEquals("updated=0", update.status());
            assertEquals("Lucene", search.hits().getFirst().source().get("title"));
        }
    }

    @Test
    public void testKnnSearchUsesNumericVectorSourceFields() throws Exception {
        MemMockProvider metadata = new MemMockProvider();
        ShardId shardId = new ShardId("books", 0);
        try (LuceneLocalShardIndexService service = new LuceneLocalShardIndexService(
                tempDir,
                "bucket",
                metadata,
                new LocalFileRemoteObjectStore(tempDir.resolve("remote"))
        )) {
            Map<String, FieldMapping> mappings = Map.of(
                    "embedding", new FieldMapping("dense_vector", 2, "cosine", true, true)
            );
            service.index(new IndexDocumentRequest("books", shardId, "doc-1", Map.of(
                    "title", "Lucene",
                    "embedding", List.of(1.0, 0.0)
            ), mappings));
            service.index(new IndexDocumentRequest("books", shardId, "doc-2", Map.of(
                    "title", "S3",
                    "embedding", List.of(0.0, 1.0)
            ), mappings));

            var response = service.search(
                    shardId,
                    new SearchRequest(
                            "books",
                            Map.of("match_all", Map.of()),
                            List.of(),
                            new VectorQuery("embedding", List.of(1.0f, 0.0f), 1, 2),
                            null,
                            0,
                            1,
                            mappings
                    )
            );

            assertEquals(1, response.hits().size());
            assertEquals("doc-1", response.hits().getFirst().id());
            assertEquals("Lucene", response.hits().getFirst().source().get("title"));
        }
    }

    @Test
    public void testKnnSearchRejectsWrongQueryDimension() throws Exception {
        MemMockProvider metadata = new MemMockProvider();
        ShardId shardId = new ShardId("books", 0);
        Map<String, FieldMapping> mappings = Map.of(
                "embedding", new FieldMapping("dense_vector", 2, "cosine", true, true)
        );
        try (LuceneLocalShardIndexService service = new LuceneLocalShardIndexService(
                tempDir,
                "bucket",
                metadata,
                new LocalFileRemoteObjectStore(tempDir.resolve("remote"))
        )) {
            service.index(new IndexDocumentRequest("books", shardId, "doc-1", Map.of(
                    "embedding", List.of(1.0, 0.0)
            ), mappings));

            assertThrows(IllegalArgumentException.class, () -> service.search(
                    shardId,
                    new SearchRequest(
                            "books",
                            Map.of("match_all", Map.of()),
                            List.of(),
                            new VectorQuery("embedding", List.of(1.0f), 1, 2),
                            null,
                            0,
                            1,
                            mappings
                    )
            ));
        }
    }

    @Test
    public void testKnnSearchCanUseScalarFilter() throws Exception {
        MemMockProvider metadata = new MemMockProvider();
        ShardId shardId = new ShardId("books", 0);
        Map<String, FieldMapping> mappings = Map.of(
                "category", new FieldMapping("keyword", null, null, true, true),
                "embedding", new FieldMapping("dense_vector", 2, "cosine", true, true)
        );
        try (LuceneLocalShardIndexService service = new LuceneLocalShardIndexService(
                tempDir,
                "bucket",
                metadata,
                new LocalFileRemoteObjectStore(tempDir.resolve("remote"))
        )) {
            service.index(new IndexDocumentRequest("books", shardId, "doc-1", Map.of(
                    "category", "hidden",
                    "embedding", List.of(1.0, 0.0)
            ), mappings));
            service.index(new IndexDocumentRequest("books", shardId, "doc-2", Map.of(
                    "category", "visible",
                    "embedding", List.of(0.0, 1.0)
            ), mappings));

            var response = service.search(
                    shardId,
                    new SearchRequest(
                            "books",
                            Map.of("term", Map.of("category", "visible")),
                            List.of(),
                            new VectorQuery("embedding", List.of(1.0f, 0.0f), 2, 10),
                            null,
                            0,
                            2,
                            mappings
                    )
            );

            assertEquals(1, response.hits().size());
            assertEquals("doc-2", response.hits().getFirst().id());
        }
    }

    @Test
    public void testKnnSearchCanApplyMinimumScore() throws Exception {
        MemMockProvider metadata = new MemMockProvider();
        ShardId shardId = new ShardId("books", 0);
        Map<String, FieldMapping> mappings = Map.of(
                "embedding", new FieldMapping("dense_vector", 2, "cosine", true, true)
        );
        try (LuceneLocalShardIndexService service = new LuceneLocalShardIndexService(
                tempDir,
                "bucket",
                metadata,
                new LocalFileRemoteObjectStore(tempDir.resolve("remote"))
        )) {
            service.index(new IndexDocumentRequest("books", shardId, "doc-1", Map.of(
                    "embedding", List.of(1.0, 0.0)
            ), mappings));
            service.index(new IndexDocumentRequest("books", shardId, "doc-2", Map.of(
                    "embedding", List.of(0.0, 1.0)
            ), mappings));

            var response = service.search(
                    shardId,
                    new SearchRequest(
                            "books",
                            Map.of("match_all", Map.of()),
                            List.of(),
                            new VectorQuery("embedding", List.of(1.0f, 0.0f), 2, 10, 0.99f),
                            null,
                            0,
                            2,
                            mappings
                    )
            );

            assertEquals(1, response.hits().size());
            assertEquals("doc-1", response.hits().getFirst().id());
        }
    }

    @Test
    public void testDenseVectorRejectsInvalidSourceValue() throws Exception {
        MemMockProvider metadata = new MemMockProvider();
        ShardId shardId = new ShardId("books", 0);
        Map<String, FieldMapping> mappings = Map.of(
                "embedding", new FieldMapping("dense_vector", 2, "cosine", true, true)
        );
        try (LuceneLocalShardIndexService service = new LuceneLocalShardIndexService(
                tempDir,
                "bucket",
                metadata,
                new LocalFileRemoteObjectStore(tempDir.resolve("remote"))
        )) {
            assertThrows(IllegalArgumentException.class, () -> service.index(new IndexDocumentRequest(
                    "books",
                    shardId,
                    "doc-1",
                    Map.of("embedding", List.of("bad", "vector")),
                    mappings
            )));
        }
    }

    @Test
    public void testMappedScalarFieldsAreSearchable() throws Exception {
        MemMockProvider metadata = new MemMockProvider();
        ShardId shardId = new ShardId("books", 0);
        Map<String, FieldMapping> mappings = Map.of(
                "title", new FieldMapping("text", null, null, true, true),
                "category", new FieldMapping("keyword", null, null, true, true),
                "pages", new FieldMapping("long", null, null, true, true)
        );
        try (LuceneLocalShardIndexService service = new LuceneLocalShardIndexService(
                tempDir,
                "bucket",
                metadata,
                new LocalFileRemoteObjectStore(tempDir.resolve("remote"))
        )) {
            service.index(new IndexDocumentRequest("books", shardId, "doc-1", Map.of(
                    "title", "Lucene in Action",
                    "category", "search",
                    "pages", 320
            ), mappings));
            service.index(new IndexDocumentRequest("books", shardId, "doc-2", Map.of(
                    "title", "S3 Storage Notes",
                    "category", "storage",
                    "pages", 120
            ), mappings));

            var keyword = service.search(
                    shardId,
                    new SearchRequest("books", Map.of("term", Map.of("category", "search")), List.of(), null, null, 0, 10, mappings)
            );
            var text = service.search(
                    shardId,
                    new SearchRequest("books", Map.of("match", Map.of("title", "lucene")), List.of(), null, null, 0, 10, mappings)
            );
            var range = service.search(
                    shardId,
                    new SearchRequest("books", Map.of("range", Map.of("pages", Map.of("gte", 300))), List.of(), null, null, 0, 10, mappings)
            );
            var bool = service.search(
                    shardId,
                    new SearchRequest(
                            "books",
                            Map.of("bool", Map.of("filter", List.of(
                                    Map.of("term", Map.of("category", "search")),
                                    Map.of("range", Map.of("pages", Map.of("gte", 300)))
                            ))),
                            List.of(),
                            null,
                            null,
                            0,
                            10,
                            mappings
                    )
            );

            assertEquals("doc-1", keyword.hits().getFirst().id());
            assertEquals("doc-1", text.hits().getFirst().id());
            assertEquals("doc-1", range.hits().getFirst().id());
            assertEquals("doc-1", bool.hits().getFirst().id());
        }
    }

    @Test
    public void testTextFieldUsesConfiguredAnalyzerForIndexAndMatchQuery() throws Exception {
        MemMockProvider metadata = new MemMockProvider();
        ShardId shardId = new ShardId("books", 0);
        Map<String, FieldMapping> mappings = Map.of(
                "title", new FieldMapping("text", null, null, true, true, null, null, "keyword", "keyword")
        );
        try (LuceneLocalShardIndexService service = new LuceneLocalShardIndexService(
                tempDir,
                "bucket",
                metadata,
                new LocalFileRemoteObjectStore(tempDir.resolve("remote"))
        )) {
            service.index(new IndexDocumentRequest("books", shardId, "doc-1", Map.of(
                    "title", "Lucene in Action"
            ), mappings));

            var tokenSearch = service.search(
                    shardId,
                    new SearchRequest("books", Map.of("match", Map.of("title", "lucene")), List.of(), null, null, 0, 10, mappings)
            );
            var exactSearch = service.search(
                    shardId,
                    new SearchRequest("books", Map.of("match", Map.of("title", "Lucene in Action")), List.of(), null, null, 0, 10, mappings)
            );

            assertTrue(tokenSearch.hits().isEmpty());
            assertEquals(List.of("doc-1"), exactSearch.hits().stream().map(hit -> hit.id()).toList());
        }
    }

    @Test
    public void testMatchQueryWithNoAnalyzerTokensMatchesNoDocuments() throws Exception {
        MemMockProvider metadata = new MemMockProvider();
        ShardId shardId = new ShardId("books", 0);
        Map<String, FieldMapping> mappings = Map.of(
                "title", new FieldMapping("text", null, null, true, true, null, null, "standard", "standard")
        );
        try (LuceneLocalShardIndexService service = new LuceneLocalShardIndexService(
                tempDir,
                "bucket",
                metadata,
                new LocalFileRemoteObjectStore(tempDir.resolve("remote"))
        )) {
            service.index(new IndexDocumentRequest("books", shardId, "doc-1", Map.of(
                    "title", "Lucene in Action"
            ), mappings));

            var response = service.search(
                    shardId,
                    new SearchRequest("books", Map.of("match", Map.of("title", "")), List.of(), null, null, 0, 10, mappings)
            );

            assertTrue(response.hits().isEmpty());
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testExtendedMappedFieldsAreSearchable() throws Exception {
        MemMockProvider metadata = new MemMockProvider();
        ShardId shardId = new ShardId("books", 0);
        Map<String, FieldMapping> mappings = Map.ofEntries(
                Map.entry("published_at", new FieldMapping("date", null, null, true, true)),
                Map.entry("client_ip", new FieldMapping("ip", null, null, true, true)),
                Map.entry("payload", new FieldMapping("binary", null, null, true, true)),
                Map.entry("location", new FieldMapping("geo_point", null, null, true, true)),
                Map.entry("page_span", new FieldMapping("long_range", null, null, true, true)),
                Map.entry("score_span", new FieldMapping("double_range", null, null, true, true)),
                Map.entry("ip_span", new FieldMapping("ip_range", null, null, true, true)),
                Map.entry("tags", new FieldMapping("keyword", null, null, true, true, true, true)),
                Map.entry("ratings", new FieldMapping("long", null, null, true, true, true, true)),
                Map.entry("boosts", new FieldMapping("double", null, null, true, true, true, true)),
                Map.entry("byte_embedding", new FieldMapping("byte_vector", 4, "cosine", true, true))
        );
        try (LuceneLocalShardIndexService service = new LuceneLocalShardIndexService(
                tempDir,
                "bucket",
                metadata,
                new LocalFileRemoteObjectStore(tempDir.resolve("remote"))
        )) {
            service.index(new IndexDocumentRequest("books", shardId, "doc-1", Map.ofEntries(
                    Map.entry("published_at", "2026-05-23T10:00:00Z"),
                    Map.entry("client_ip", "192.168.1.10"),
                    Map.entry("payload", "aGVsbG8="),
                    Map.entry("location", Map.of("lat", 31.2304, "lon", 121.4737)),
                    Map.entry("page_span", Map.of("gte", 10, "lte", 20)),
                    Map.entry("score_span", List.of(1.5, 2.5)),
                    Map.entry("ip_span", Map.of("gte", "192.168.1.0", "lte", "192.168.1.255")),
                    Map.entry("tags", List.of("search", "lucene")),
                    Map.entry("ratings", List.of(5, 4)),
                    Map.entry("boosts", List.of(-2.0, 5.5)),
                    Map.entry("byte_embedding", List.of(1, 2, 3, 4))
            ), mappings));
            service.index(new IndexDocumentRequest("books", shardId, "doc-2", Map.ofEntries(
                    Map.entry("published_at", "2025-01-01T08:00:00+08:00"),
                    Map.entry("client_ip", "10.0.0.1"),
                    Map.entry("payload", "d29ybGQ="),
                    Map.entry("location", Map.of("lat", 39.9042, "lon", 116.4074)),
                    Map.entry("page_span", Map.of("gte", 100, "lte", 200)),
                    Map.entry("score_span", List.of(9.0, 10.0)),
                    Map.entry("ip_span", Map.of("gte", "10.0.0.0", "lte", "10.0.0.255")),
                    Map.entry("tags", List.of("storage")),
                    Map.entry("ratings", List.of(2)),
                    Map.entry("boosts", List.of(-10.0, 7.0)),
                    Map.entry("byte_embedding", List.of(4, 3, 2, 1))
            ), mappings));

            assertEquals("doc-1", service.search(
                    shardId,
                    new SearchRequest("books", Map.of("term", Map.of("client_ip", "192.168.1.10")), List.of(), null, null, 0, 10, mappings)
            ).hits().getFirst().id());
            assertEquals("doc-1", service.search(
                    shardId,
                    new SearchRequest("books", Map.of("term", Map.of("payload", "aGVsbG8=")), List.of(), null, null, 0, 10, mappings)
            ).hits().getFirst().id());
            assertEquals("doc-1", service.search(
                    shardId,
                    new SearchRequest("books", Map.of("range", Map.of("published_at", Map.of("gte", "2026-01-01"))), List.of(), null, null, 0, 10, mappings)
            ).hits().getFirst().id());
            assertEquals("doc-1", service.search(
                    shardId,
                    new SearchRequest("books", Map.of("range", Map.of("client_ip", Map.of("gte", "192.168.1.0", "lte", "192.168.1.255"))), List.of(), null, null, 0, 10, mappings)
            ).hits().getFirst().id());
            assertEquals("doc-1", service.search(
                    shardId,
                    new SearchRequest("books", Map.of("range", Map.of("page_span", Map.of("gte", 15, "lte", 16))), List.of(), null, null, 0, 10, mappings)
            ).hits().getFirst().id());
            assertEquals("doc-1", service.search(
                    shardId,
                    new SearchRequest("books", Map.of("range", Map.of("score_span", Map.of("gte", 2.0, "lte", 2.1))), List.of(), null, null, 0, 10, mappings)
            ).hits().getFirst().id());
            assertEquals("doc-1", service.search(
                    shardId,
                    new SearchRequest("books", Map.of("range", Map.of("ip_span", Map.of("gte", "192.168.1.20", "lte", "192.168.1.30"))), List.of(), null, null, 0, 10, mappings)
            ).hits().getFirst().id());
            assertEquals("doc-1", service.search(
                    shardId,
                    new SearchRequest("books", Map.of(
                            "geo_distance",
                            Map.of("distance", "2km", "location", Map.of("lat", 31.2304, "lon", 121.4737))
                    ), List.of(), null, null, 0, 10, mappings)
            ).hits().getFirst().id());
            assertEquals("doc-1", service.search(
                    shardId,
                    new SearchRequest("books", Map.of(
                            "geo_bounding_box",
                            Map.of("location", Map.of("top", 31.3, "bottom", 31.2, "left", 121.4, "right", 121.6))
                    ), List.of(), null, null, 0, 10, mappings)
            ).hits().getFirst().id());

            var sorted = service.search(
                    shardId,
                    new SearchRequest(
                            "books",
                            Map.of("match_all", Map.of()),
                            List.of(),
                            null,
                            null,
                            0,
                            10,
                            List.of(Map.of("published_at", Map.of("order", "asc"))),
                            mappings
                    )
            );
            assertEquals("doc-2", sorted.hits().getFirst().id());

            var vector = service.search(
                    shardId,
                    new SearchRequest(
                            "books",
                            Map.of("match_all", Map.of()),
                            List.of(),
                            new VectorQuery("byte_embedding", List.of(1f, 2f, 3f, 4f), 1, 10),
                            null,
                            0,
                            10,
                            mappings
                    )
            );
            assertEquals("doc-1", vector.hits().getFirst().id());

            var filteredVector = service.search(
                    shardId,
                    new SearchRequest(
                            "books",
                            Map.of("term", Map.of("tags", "storage")),
                            List.of(),
                            new VectorQuery("byte_embedding", List.of(1f, 2f, 3f, 4f), 1, 10),
                            null,
                            0,
                            10,
                            mappings
                    )
            );
            assertEquals("doc-2", filteredVector.hits().getFirst().id());

            assertEquals("doc-1", service.search(
                    shardId,
                    new SearchRequest(
                            "books",
                            Map.of("match_all", Map.of()),
                            List.of(),
                            null,
                            null,
                            0,
                            10,
                            List.of(Map.of("tags", Map.of("order", "asc"))),
                            mappings
                    )
            ).hits().getFirst().id());
            assertEquals("doc-2", service.search(
                    shardId,
                    new SearchRequest(
                            "books",
                            Map.of("match_all", Map.of()),
                            List.of(),
                            null,
                            null,
                            0,
                            10,
                            List.of(Map.of("ratings", Map.of("order", "asc"))),
                            mappings
                    )
            ).hits().getFirst().id());
            assertEquals("doc-2", service.search(
                    shardId,
                    new SearchRequest(
                            "books",
                            Map.of("match_all", Map.of()),
                            List.of(),
                            null,
                            null,
                            0,
                            10,
                            List.of(Map.of("boosts", Map.of("order", "desc"))),
                            mappings
                    )
            ).hits().getFirst().id());

            var aggregation = service.search(
                    shardId,
                    new SearchRequest(
                            "books",
                            Map.of("match_all", Map.of()),
                            List.of(
                                    Map.of("name", "by_tag", "terms", Map.of("field", "tags")),
                                    Map.of("name", "rating_count", "value_count", Map.of("field", "ratings"))
                            ),
                            null,
                            null,
                            0,
                            10,
                            mappings
                    )
            );
            Map<String, Object> byTag = (Map<String, Object>) aggregation.aggregations().get("by_tag");
            List<Map<String, Object>> buckets = (List<Map<String, Object>>) byTag.get("buckets");
            Map<String, Object> ratingCount = (Map<String, Object>) aggregation.aggregations().get("rating_count");
            assertTrue(buckets.stream().anyMatch(bucket -> "search".equals(bucket.get("key"))));
            assertEquals(3L, ratingCount.get("count"));
            assertEquals(3L, ratingCount.get("value"));

            assertThrows(IllegalArgumentException.class, () -> service.index(new IndexDocumentRequest(
                    "books",
                    shardId,
                    "doc-bad-date",
                    Map.of("published_at", "not-a-date"),
                    mappings
            )));
            assertThrows(IllegalArgumentException.class, () -> service.index(new IndexDocumentRequest(
                    "books",
                    shardId,
                    "doc-bad-ip",
                    Map.of("client_ip", "example.com"),
                    mappings
            )));
        }
    }

    @Test
    public void testSearchCanSortByMappedSourceField() throws Exception {
        MemMockProvider metadata = new MemMockProvider();
        ShardId shardId = new ShardId("books", 0);
        Map<String, FieldMapping> mappings = Map.of(
                "pages", new FieldMapping("long", null, null, true, true)
        );
        try (LuceneLocalShardIndexService service = new LuceneLocalShardIndexService(
                tempDir,
                "bucket",
                metadata,
                new LocalFileRemoteObjectStore(tempDir.resolve("remote"))
        )) {
            service.index(new IndexDocumentRequest("books", shardId, "doc-1", Map.of("pages", 300), mappings));
            service.index(new IndexDocumentRequest("books", shardId, "doc-2", Map.of("pages", 100), mappings));

            var response = service.search(
                    shardId,
                    new SearchRequest(
                            "books",
                            Map.of("match_all", Map.of()),
                            List.of(),
                            null,
                            null,
                            0,
                            10,
                            List.of(Map.of("pages", Map.of("order", "asc"))),
                            mappings
                    )
            );

            assertEquals("doc-2", response.hits().getFirst().id());
            assertEquals(100, ((Number) response.hits().getFirst().sortValues().getFirst()).intValue());
            assertEquals("doc-1", response.hits().get(1).id());
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void termsAndRangeAggregationsSupportCommonOptions() throws Exception {
        MemMockProvider metadata = new MemMockProvider();
        ShardId shardId = new ShardId("books", 0);
        Map<String, FieldMapping> mappings = Map.of(
                "category", new FieldMapping("keyword", null, null, true, true),
                "pages", new FieldMapping("long", null, null, true, true)
        );
        try (LuceneLocalShardIndexService service = new LuceneLocalShardIndexService(
                tempDir,
                "bucket",
                metadata,
                new LocalFileRemoteObjectStore(tempDir.resolve("remote"))
        )) {
            service.index(new IndexDocumentRequest("books", shardId, "doc-1", Map.of(
                    "category", "search",
                    "pages", 80
            ), mappings));
            service.index(new IndexDocumentRequest("books", shardId, "doc-2", Map.of(
                    "category", "storage",
                    "pages", 160
            ), mappings));
            service.index(new IndexDocumentRequest("books", shardId, "doc-3", Map.of(
                    "pages", 260
            ), mappings));
            service.index(new IndexDocumentRequest("books", shardId, "doc-4", Map.of(
                    "category", "search",
                    "pages", List.of(80, 90)
            ), mappings));

            var response = service.search(
                    shardId,
                    new SearchRequest(
                            "books",
                            Map.of("match_all", Map.of()),
                            List.of(
                                    Map.of(
                                            "name", "by_category",
                                            "terms", Map.of(
                                                    "field", "category",
                                                    "missing", "__missing__",
                                                    "order", Map.of("_key", "asc"),
                                                    "min_doc_count", 1,
                                                    "size", 10
                                            )
                                    ),
                                    Map.of(
                                            "name", "page_ranges",
                                            "range", Map.of(
                                                    "field", "pages",
                                                    "ranges", List.of(
                                                            Map.of("key", "short", "to", 100),
                                                            Map.of("key", "medium", "from", 100, "to", 200),
                                                            Map.of("key", "long", "from", 200)
                                                    )
                                            )
                                    )
                            ),
                            null,
                            null,
                            0,
                            10,
                            mappings
                    )
            );

            Map<String, Object> byCategory = (Map<String, Object>) response.aggregations().get("by_category");
            List<Map<String, Object>> categoryBuckets = (List<Map<String, Object>>) byCategory.get("buckets");
            assertEquals(List.of("__missing__", "search", "storage"),
                    categoryBuckets.stream().map(bucket -> bucket.get("key")).toList());

            Map<String, Object> pageRanges = (Map<String, Object>) response.aggregations().get("page_ranges");
            List<Map<String, Object>> rangeBuckets = (List<Map<String, Object>>) pageRanges.get("buckets");
            assertEquals(List.of(2L, 1L, 1L), rangeBuckets.stream().map(bucket -> bucket.get("doc_count")).toList());
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void termsAggregationKeepsShardBucketsBelowGlobalMinDocCount() throws Exception {
        MemMockProvider metadata = new MemMockProvider();
        ShardId shardId = new ShardId("books", 0);
        Map<String, FieldMapping> mappings = Map.of(
                "category", new FieldMapping("keyword", null, null, true, true)
        );
        try (LuceneLocalShardIndexService service = new LuceneLocalShardIndexService(
                tempDir,
                "bucket",
                metadata,
                new LocalFileRemoteObjectStore(tempDir.resolve("remote"))
        )) {
            service.index(new IndexDocumentRequest("books", shardId, "doc-1", Map.of(
                    "category", "common"
            ), mappings));

            var response = service.search(
                    shardId,
                    new SearchRequest(
                            "books",
                            Map.of("match_all", Map.of()),
                            List.of(Map.of(
                                    "name", "by_category",
                                    "terms", Map.of(
                                            "field", "category",
                                            "min_doc_count", 2
                                    )
                            )),
                            null,
                            null,
                            0,
                            10,
                            mappings
                    )
            );

            Map<String, Object> byCategory = (Map<String, Object>) response.aggregations().get("by_category");
            List<Map<String, Object>> buckets = (List<Map<String, Object>>) byCategory.get("buckets");
            assertEquals(List.of("common"), buckets.stream().map(bucket -> bucket.get("key")).toList());
            assertEquals(List.of(1L), buckets.stream().map(bucket -> bucket.get("doc_count")).toList());
        }
    }

    @Test
    public void rangeAggregationRejectsInvalidPresentBounds() throws Exception {
        MemMockProvider metadata = new MemMockProvider();
        ShardId shardId = new ShardId("books", 0);
        Map<String, FieldMapping> mappings = Map.of(
                "pages", new FieldMapping("long", null, null, true, true),
                "published_at", new FieldMapping("date", null, null, true, true)
        );
        try (LuceneLocalShardIndexService service = new LuceneLocalShardIndexService(
                tempDir,
                "bucket",
                metadata,
                new LocalFileRemoteObjectStore(tempDir.resolve("remote"))
        )) {
            service.index(new IndexDocumentRequest("books", shardId, "doc-1", Map.of(
                    "pages", 80,
                    "published_at", "2026-01-01"
            ), mappings));

            IllegalArgumentException numeric = assertThrows(IllegalArgumentException.class, () -> service.search(
                    shardId,
                    new SearchRequest(
                            "books",
                            Map.of("match_all", Map.of()),
                            List.of(Map.of(
                                    "name", "page_ranges",
                                    "range", Map.of(
                                            "field", "pages",
                                            "ranges", List.of(Map.of("from", "bad", "to", 100))
                                    )
                            )),
                            null,
                            null,
                            0,
                            10,
                            mappings
                    )
            ));
            assertTrue(numeric.getMessage().contains("from bound"));

            IllegalArgumentException date = assertThrows(IllegalArgumentException.class, () -> service.search(
                    shardId,
                    new SearchRequest(
                            "books",
                            Map.of("match_all", Map.of()),
                            List.of(Map.of(
                                    "name", "published_ranges",
                                    "range", Map.of(
                                            "field", "published_at",
                                            "ranges", List.of(Map.of("from", "2026-01-01", "to", "bad"))
                                    )
                            )),
                            null,
                            null,
                            0,
                            10,
                            mappings
                    )
            ));
            assertTrue(date.getMessage().contains("to bound"));
        }
    }

    @Test
    public void testSearchAfterUsesSortCursor() throws Exception {
        MemMockProvider metadata = new MemMockProvider();
        ShardId shardId = new ShardId("books", 0);
        Map<String, FieldMapping> mappings = Map.of(
                "pages", new FieldMapping("long", null, null, true, true)
        );
        List<Map<String, Object>> sort = List.of(
                Map.of("pages", Map.of("order", "asc")),
                Map.of("_id", Map.of("order", "asc"))
        );
        try (LuceneLocalShardIndexService service = new LuceneLocalShardIndexService(
                tempDir,
                "bucket",
                metadata,
                new LocalFileRemoteObjectStore(tempDir.resolve("remote"))
        )) {
            service.index(new IndexDocumentRequest("books", shardId, "doc-1", Map.of("pages", 300), mappings));
            service.index(new IndexDocumentRequest("books", shardId, "doc-2", Map.of("pages", 100), mappings));

            var firstPage = service.search(
                    shardId,
                    new SearchRequest(
                            "books",
                            Map.of("match_all", Map.of()),
                            List.of(),
                            null,
                            null,
                            0,
                            1,
                            sort,
                            mappings
                    )
            );
            var secondPage = service.search(
                    shardId,
                    new SearchRequest(
                            "books",
                            Map.of("match_all", Map.of()),
                            List.of(),
                            null,
                            null,
                            0,
                            1,
                            sort,
                            firstPage.hits().getFirst().sortValues(),
                            mappings
                    )
            );

            assertEquals("doc-2", firstPage.hits().getFirst().id());
            assertEquals("doc-1", secondPage.hits().getFirst().id());
        }
    }

    @Test
    public void testPointInTimeKeepsReaderSnapshot() throws Exception {
        MemMockProvider metadata = new MemMockProvider();
        ShardId shardId = new ShardId("books", 0);
        try (LuceneLocalShardIndexService service = new LuceneLocalShardIndexService(
                tempDir,
                "bucket",
                metadata,
                new LocalFileRemoteObjectStore(tempDir.resolve("remote"))
        )) {
            service.index(new IndexDocumentRequest("books", shardId, "doc-1", Map.of("title", "Lucene")));
            String pitId = service.openPointInTime(shardId, "books", Duration.ofMinutes(1)).id();
            service.index(new IndexDocumentRequest("books", shardId, "doc-2", Map.of("title", "S3")));

            var pitSearch = service.search(
                    shardId,
                    new SearchRequest(
                            "books",
                            Map.of("match_all", Map.of()),
                            List.of(),
                            null,
                            null,
                            0,
                            10,
                            List.of(),
                            List.of(),
                            pitId,
                            Map.of()
                    )
            );
            var normalSearch = service.search(
                    shardId,
                    new SearchRequest("books", Map.of("match_all", Map.of()), List.of(), null, null, 0, 10)
            );

            assertEquals(1, pitSearch.hits().size());
            assertEquals("doc-1", pitSearch.hits().getFirst().id());
            assertEquals(2, normalSearch.hits().size());
            assertTrue(service.closePointInTime(pitId));
        }
    }

    @Test
    public void testRemotePointInTimeReadsCleanSnapshotWithoutOpeningWriter() throws Exception {
        MemMockProvider metadata = new MemMockProvider();
        ShardId shardId = new ShardId("books", 0);
        RemoteObjectStore remote = new LocalFileRemoteObjectStore(tempDir.resolve("remote"));
        try (LuceneLocalShardIndexService owner = new LuceneLocalShardIndexService(
                tempDir.resolve("node-1"),
                "bucket",
                metadata,
                remote
        )) {
            owner.index(new IndexDocumentRequest("books", shardId, "doc-1", Map.of("title", "clean")));
            waitForCleanSnapshot(metadata, "books__shard_0");
        }

        Path readerNode = tempDir.resolve("node-2");
        try (LuceneLocalShardIndexService service = new LuceneLocalShardIndexService(
                readerNode,
                "bucket",
                metadata,
                remote
        )) {
            String pitId = service.openPointInTime(shardId, "books", Duration.ofMinutes(1), "remote").id();
            assertEquals(1, metadata.snapshotPins("books__shard_0").size());

            var response = service.search(
                    shardId,
                    new SearchRequest(
                            "books",
                            Map.of("match_all", Map.of()),
                            List.of(),
                            null,
                            null,
                            0,
                            10,
                            List.of(),
                            List.of(),
                            pitId,
                            Map.of(),
                            "remote"
                    )
            );

            assertEquals(List.of("doc-1"), response.hits().stream().map(hit -> hit.id()).toList());
            assertFalse(hasFiles(PathUtil.walDataPath(readerNode, "books__shard_0")));
            assertTrue(service.closePointInTime(pitId));
            assertTrue(metadata.snapshotPins("books__shard_0").isEmpty());
        }
    }

    @Test
    public void testRemotePointInTimeOnEmptySnapshotDoesNotCreateWriter() throws Exception {
        MemMockProvider metadata = new MemMockProvider();
        ShardId shardId = new ShardId("books", 0);
        try (LuceneLocalShardIndexService service = new LuceneLocalShardIndexService(
                tempDir,
                "bucket",
                metadata,
                new LocalFileRemoteObjectStore(tempDir.resolve("remote"))
        )) {
            String pitId = service.openPointInTime(shardId, "books", Duration.ofMinutes(1), "remote").id();

            var response = service.search(
                    shardId,
                    new SearchRequest(
                            "books",
                            Map.of("match_all", Map.of()),
                            List.of(),
                            null,
                            null,
                            0,
                            10,
                            List.of(),
                            List.of(),
                            pitId,
                            Map.of(),
                            "remote"
                    )
            );

            assertTrue(response.hits().isEmpty());
            assertFalse(hasFiles(PathUtil.walDataPath(tempDir, "books__shard_0")));
            assertTrue(service.closePointInTime(pitId));
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testAggregationsRunOnMatchedDocuments() throws Exception {
        MemMockProvider metadata = new MemMockProvider();
        ShardId shardId = new ShardId("books", 0);
        Map<String, FieldMapping> mappings = Map.of(
                "category", new FieldMapping("keyword", null, null, true, true),
                "pages", new FieldMapping("long", null, null, true, true)
        );
        try (LuceneLocalShardIndexService service = new LuceneLocalShardIndexService(
                tempDir,
                "bucket",
                metadata,
                new LocalFileRemoteObjectStore(tempDir.resolve("remote"))
        )) {
            service.index(new IndexDocumentRequest("books", shardId, "doc-1", Map.of(
                    "category", "search",
                    "pages", 100
            ), mappings));
            service.index(new IndexDocumentRequest("books", shardId, "doc-2", Map.of(
                    "category", "search",
                    "pages", 300
            ), mappings));
            service.index(new IndexDocumentRequest("books", shardId, "doc-3", Map.of(
                    "category", "storage",
                    "pages", 900
            ), mappings));

            var response = service.search(
                    shardId,
                    new SearchRequest(
                            "books",
                            Map.of("range", Map.of("pages", Map.of("lt", 500))),
                            List.of(
                                    Map.of("name", "by_category", "terms", Map.of("field", "category")),
                                    Map.of("name", "avg_pages", "avg", Map.of("field", "pages")),
                                    Map.of("name", "sum_pages", "sum", Map.of("field", "pages"))
                            ),
                            null,
                            null,
                            0,
                            10,
                            mappings
                    )
            );

            Map<String, Object> byCategory = (Map<String, Object>) response.aggregations().get("by_category");
            List<Map<String, Object>> buckets = (List<Map<String, Object>>) byCategory.get("buckets");
            Map<String, Object> avgPages = (Map<String, Object>) response.aggregations().get("avg_pages");
            Map<String, Object> sumPages = (Map<String, Object>) response.aggregations().get("sum_pages");

            assertEquals("search", buckets.getFirst().get("key"));
            assertEquals(2L, buckets.getFirst().get("doc_count"));
            assertEquals(200.0, avgPages.get("value"));
            assertEquals(400.0, sumPages.get("value"));
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testAggregationCanUseDocValuesWithoutIndexingField() throws Exception {
        MemMockProvider metadata = new MemMockProvider();
        ShardId shardId = new ShardId("books", 0);
        Map<String, FieldMapping> mappings = Map.of(
                "pages", new FieldMapping("long", null, null, false, true, true)
        );
        try (LuceneLocalShardIndexService service = new LuceneLocalShardIndexService(
                tempDir,
                "bucket",
                metadata,
                new LocalFileRemoteObjectStore(tempDir.resolve("remote"))
        )) {
            service.index(new IndexDocumentRequest("books", shardId, "doc-1", Map.of("pages", 100), mappings));
            service.index(new IndexDocumentRequest("books", shardId, "doc-2", Map.of("pages", 300), mappings));

            var response = service.search(
                    shardId,
                    new SearchRequest(
                            "books",
                            Map.of("match_all", Map.of()),
                            List.of(Map.of("name", "sum_pages", "sum", Map.of("field", "pages"))),
                            null,
                            null,
                            0,
                            10,
                            mappings
                    )
            );

            Map<String, Object> sumPages = (Map<String, Object>) response.aggregations().get("sum_pages");
            assertEquals(400.0, sumPages.get("value"));
            assertEquals(2L, sumPages.get("count"));
        }
    }

    private void waitForCleanSegment(MemMockProvider metadata, String physicalIndexName) throws InterruptedException {
        long deadline = System.nanoTime() + 5_000_000_000L;
        while (System.nanoTime() < deadline) {
            boolean cleanSegment = metadata.listAll(physicalIndexName, List.of(IndexFileStatus.CLEAN)).stream()
                    .anyMatch(file -> file.getName().startsWith("segments_"));
            if (cleanSegment) {
                return;
            }
            Thread.sleep(50);
        }
        throw new AssertionError("segments file was not marked CLEAN");
    }

    private com.github.wxk6b1203.search.SearchResponse matchAll(
            LuceneLocalShardIndexService service,
            ShardId shardId
    ) throws IOException {
        return service.search(
                shardId,
                new SearchRequest("books", Map.of("match_all", Map.of()), List.of(), null, null, 0, 10)
        );
    }

    private com.github.wxk6b1203.search.SearchResponse remoteMatchAll(
            LuceneLocalShardIndexService service,
            ShardId shardId
    ) throws IOException {
        return service.search(
                shardId,
                new SearchRequest(
                        "books",
                        Map.of("match_all", Map.of()),
                        List.of(),
                        null,
                        null,
                        0,
                        10,
                        List.of(),
                        List.of(),
                        null,
                        Map.of(),
                        "remote"
                )
        );
    }

    @SuppressWarnings("unchecked")
    private Map<?, ?> remoteSearchers(LuceneLocalShardIndexService service) throws Exception {
        Field field = LuceneLocalShardIndexService.class.getDeclaredField("remoteSearchers");
        field.setAccessible(true);
        return (Map<?, ?>) field.get(service);
    }

    private void ageRemoteSearchers(Map<?, ?> remoteSearchers) throws Exception {
        long expiredAccessNanos = System.nanoTime() - Duration.ofMinutes(10).toNanos();
        for (Object searcher : remoteSearchers.values()) {
            Field field = searcher.getClass().getDeclaredField("lastAccessNanos");
            field.setAccessible(true);
            field.setLong(searcher, expiredAccessNanos);
        }
    }

    private void waitForCleanSnapshot(MemMockProvider metadata, String physicalIndexName) throws InterruptedException {
        long deadline = System.nanoTime() + 5_000_000_000L;
        while (System.nanoTime() < deadline) {
            boolean cleanSegment = metadata.listAll(physicalIndexName, List.of(IndexFileStatus.CLEAN)).stream()
                    .anyMatch(file -> file.getName().startsWith("segments_"));
            boolean hasUnclean = !metadata.listAll(physicalIndexName, List.of(
                    IndexFileStatus.DIRTY,
                    IndexFileStatus.UPLOADING
            )).isEmpty();
            if (cleanSegment && !hasUnclean) {
                return;
            }
            Thread.sleep(50);
        }
        throw new AssertionError("remote snapshot was not fully marked CLEAN");
    }

    private void waitForUncleanSegment(MemMockProvider metadata, String physicalIndexName) throws InterruptedException {
        long deadline = System.nanoTime() + 5_000_000_000L;
        while (System.nanoTime() < deadline) {
            boolean uncleanSegment = metadata.listAll(physicalIndexName, List.of(
                            IndexFileStatus.DIRTY,
                            IndexFileStatus.UPLOADING
                    )).stream()
                    .anyMatch(file -> file.getName().startsWith("segments_"));
            if (uncleanSegment) {
                return;
            }
            Thread.sleep(50);
        }
        throw new AssertionError("segments file was not left DIRTY/UPLOADING");
    }

    private boolean hasUploadedSegment(Path remoteDataPath) throws Exception {
        if (!Files.isDirectory(remoteDataPath)) {
            return false;
        }
        try (var files = Files.list(remoteDataPath)) {
            return files.anyMatch(path -> path.getFileName().toString().startsWith("segments_"));
        }
    }

    private void waitForCommittedSegmentFileCount(Path walPath, int expectedCount) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (System.nanoTime() < deadline) {
            if (committedSegmentFileCount(walPath) == expectedCount) {
                return;
            }
            Thread.sleep(50);
        }
        assertEquals(expectedCount, committedSegmentFileCount(walPath));
    }

    private long committedSegmentFileCount(Path walPath) throws IOException {
        if (!Files.isDirectory(walPath)) {
            return 0;
        }
        try (var files = Files.list(walPath)) {
            return files
                    .map(path -> path.getFileName().toString())
                    .filter(name -> name.startsWith("segments_") && !name.startsWith("pending_segments_"))
                    .count();
        }
    }

    private boolean hasFiles(Path path) throws IOException {
        if (!Files.isDirectory(path)) {
            return false;
        }
        try (var files = Files.list(path)) {
            return files.findAny().isPresent();
        }
    }

    private void deleteChildrenWithRetry(Path root) throws Exception {
        IOException failure = null;
        for (int attempt = 0; attempt < 20; attempt++) {
            try {
                deleteChildren(root);
                return;
            } catch (IOException e) {
                failure = e;
                Thread.sleep(50L * (attempt + 1));
            }
        }
        throw failure;
    }

    private void deleteChildren(Path root) throws IOException {
        if (!Files.isDirectory(root)) {
            return;
        }
        IOException failure = null;
        try (var children = Files.list(root)) {
            for (Path child : children.toList()) {
                try {
                    deleteRecursively(child);
                } catch (IOException e) {
                    failure = addFailure(failure, e);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private void deleteRecursively(Path path) throws IOException {
        if (!Files.exists(path)) {
            return;
        }
        IOException failure = null;
        try (var stream = Files.walk(path)) {
            for (Path item : stream.sorted(Comparator.reverseOrder()).toList()) {
                try {
                    Files.deleteIfExists(item);
                } catch (IOException e) {
                    failure = addFailure(failure, e);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private IOException addFailure(IOException failure, IOException e) {
        if (failure == null) {
            return e;
        }
        failure.addSuppressed(e);
        return failure;
    }

    private static class ControlledRemoteObjectStore implements RemoteObjectStore {
        private final RemoteObjectStore delegate;
        private final AtomicBoolean failPuts = new AtomicBoolean();

        private ControlledRemoteObjectStore(RemoteObjectStore delegate) {
            this.delegate = delegate;
        }

        private void failPuts(boolean fail) {
            failPuts.set(fail);
        }

        @Override
        public void put(String key, Path source) throws IOException {
            if (failPuts.get()) {
                throw new IOException("upload intentionally blocked");
            }
            delegate.put(key, source);
        }

        @Override
        public void get(String key, Path target) throws IOException {
            delegate.get(key, target);
        }

        @Override
        public void delete(Collection<String> keys) throws IOException {
            delegate.delete(keys);
        }
    }

    private static final class FaultyRemoteObjectStore implements RemoteObjectStore {
        private final RemoteObjectStore delegate;
        private final AtomicInteger failedPutsRemaining = new AtomicInteger();
        private final AtomicInteger putFailures = new AtomicInteger();

        private FaultyRemoteObjectStore(RemoteObjectStore delegate) {
            this.delegate = delegate;
        }

        private void failNextPuts(int count) {
            failedPutsRemaining.set(count);
        }

        private int putFailures() {
            return putFailures.get();
        }

        @Override
        public void put(String key, Path source) throws IOException {
            while (true) {
                int remaining = failedPutsRemaining.get();
                if (remaining <= 0) {
                    break;
                }
                if (failedPutsRemaining.compareAndSet(remaining, remaining - 1)) {
                    putFailures.incrementAndGet();
                    throw new IOException("injected transient upload failure");
                }
            }
            delegate.put(key, source);
        }

        @Override
        public void get(String key, Path target) throws IOException {
            delegate.get(key, target);
        }

        @Override
        public void delete(Collection<String> keys) throws IOException {
            delegate.delete(keys);
        }
    }
}
