package com.github.wxk6b1203.http;

import com.github.wxk6b1203.cluster.FieldMapping;
import com.github.wxk6b1203.cluster.NodeRole;
import com.github.wxk6b1203.config.ServerOptions;
import com.github.wxk6b1203.index.IndexWriteOptions;
import com.github.wxk6b1203.search.SearchRequest;
import com.github.wxk6b1203.search.VectorQuery;
import com.github.wxk6b1203.util.JsonUtil;
import com.google.protobuf.MessageLite;
import com.google.protobuf.Value;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32;

import static org.junit.jupiter.api.Assertions.*;

class HttpApiServerTest {
    @TempDir
    Path tempDir;

    private HttpApiServer server;
    private int port;

    @Test
    void writeMaintenanceIntervalUsesConfiguredSubSecondCadence() {
        assertEquals(100, HttpApiServer.writeMaintenanceIntervalMillis(new IndexWriteOptions(
                false,
                0,
                Duration.ofMillis(100),
                IndexWriteOptions.RefreshPolicy.IMMEDIATE,
                Duration.ofSeconds(1)
        )));
        assertEquals(50, HttpApiServer.writeMaintenanceIntervalMillis(new IndexWriteOptions(
                false,
                0,
                Duration.ofMillis(500),
                IndexWriteOptions.RefreshPolicy.INTERVAL,
                Duration.ofMillis(50)
        )));
        assertEquals(0, HttpApiServer.writeMaintenanceIntervalMillis(IndexWriteOptions.defaults()));
        assertEquals(-1, HttpApiServer.bodyLimit(0));
        assertEquals(-1, HttpApiServer.bodyLimit(-1));
        assertEquals(1024, HttpApiServer.bodyLimit(1024));
    }

    @AfterEach
    void closeServer() {
        if (server != null) {
            server.close();
        }
        try {
            deleteChildrenWithRetry(tempDir);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    @Timeout(60)
    void httpSearchSupportsKnnFilterPaginationPitAndInternalRequest() throws Exception {
        startServer();
        createBooksIndex();
        indexBook("doc-1", "hidden", 100, List.of(1.0, 0.0));
        indexBook("doc-2", "visible", 200, List.of(0.0, 1.0));

        Map<String, Object> knn = post("/books/_search", Map.of(
                "knn", Map.of(
                        "field", "embedding",
                        "query_vector", List.of(1.0, 0.0),
                        "k", 1,
                        "num_candidates", 10,
                        "filter", Map.of("term", Map.of("category", "visible"))
                )
        ), 200);
        assertEquals(List.of("doc-2"), hitIds(knn));
        assertTrue(((Number) knn.get("tookMillis")).longValue() > 0);

        Map<String, Object> firstPage = post("/books/_search", Map.of(
                "query", Map.of("match_all", Map.of()),
                "sort", List.of(
                        Map.of("pages", Map.of("order", "asc")),
                        Map.of("_id", Map.of("order", "asc"))
                ),
                "size", 1
        ), 200);
        assertEquals(List.of("doc-1"), hitIds(firstPage));

        Map<String, Object> secondPage = post("/books/_search", Map.of(
                "query", Map.of("match_all", Map.of()),
                "sort", List.of(
                        Map.of("pages", Map.of("order", "asc")),
                        Map.of("_id", Map.of("order", "asc"))
                ),
                "search_after", sortValues(firstPage),
                "size", 1
        ), 200);
        assertEquals(List.of("doc-2"), hitIds(secondPage));

        String pitId = stringValue(post("/books/_pit?keep_alive=1m", Map.of(), 200).get("id"));
        indexBook("doc-3", "visible", 150, List.of(1.0, 0.0));

        Map<String, Object> pitSearch = post("/books/_search", Map.of(
                "pit", Map.of("id", pitId),
                "query", Map.of("match_all", Map.of()),
                "size", 10
        ), 200);
        assertFalse(hitIds(pitSearch).contains("doc-3"));

        Map<String, Object> currentSearch = post("/books/_search", Map.of(
                "query", Map.of("match_all", Map.of()),
                "size", 10
        ), 200);
        assertTrue(hitIds(currentSearch).contains("doc-3"));
        assertTrue((Boolean) delete("/_pit", Map.of("id", pitId), 200).get("succeeded"));

        SearchRequest internalRequest = new SearchRequest(
                "books",
                Map.of("term", Map.of("category", "visible")),
                List.of(),
                new VectorQuery("embedding", List.of(1.0f, 0.0f), 2, 10),
                null,
                0,
                2,
                List.of(),
                List.of(),
                null,
                bookMappings()
        );
        Map<String, Object> internalSearch = post("/_internal/books/0/_search", internalRequest, 200);
        assertEquals("doc-3", hitIds(internalSearch).getFirst());
    }

    @Test
    @Timeout(60)
    void knnSearchHonorsPointInTime() throws Exception {
        startServer();
        createBooksIndex();
        // doc-1 is a near match for the query vector; doc-2 is far.
        indexBook("doc-1", "visible", 100, List.of(0.9, 0.1));
        indexBook("doc-2", "visible", 200, List.of(0.0, 1.0));

        // Open a PIT pinning the current generation before doc-3 exists.
        String pitId = stringValue(post("/books/_pit?keep_alive=1m", Map.of(), 200).get("id"));

        // doc-3 is an exact match for the query vector, but is indexed after the PIT was opened.
        indexBook("doc-3", "visible", 150, List.of(1.0, 0.0));

        Map<String, Object> knn = Map.of(
                "field", "embedding",
                "query_vector", List.of(1.0, 0.0),
                "k", 1,
                "num_candidates", 10
        );

        // A PIT-scoped _knn_search must read the pinned generation and exclude doc-3, so the
        // nearest visible document is doc-1. (Before the fix, _knn_search ignored the PIT and
        // read the latest generation, returning doc-3.)
        Map<String, Object> pitKnnSearch = post("/books/_knn_search", Map.of(
                "knn", knn,
                "pit", Map.of("id", pitId)
        ), 200);
        assertEquals(List.of("doc-1"), hitIds(pitKnnSearch));

        // A current _knn_search reads the latest generation and returns the exact match doc-3.
        Map<String, Object> currentKnnSearch = post("/books/_knn_search", Map.of(
                "knn", knn
        ), 200);
        assertEquals(List.of("doc-3"), hitIds(currentKnnSearch));

        assertTrue((Boolean) delete("/_pit", Map.of("id", pitId), 200).get("succeeded"));
    }

    @Test
    @Timeout(60)
    @SuppressWarnings("unchecked")
    void lifecyclePolicyStoresParsedPhaseMinAgesAndCanBeAttached() throws Exception {
        startServer();
        createBooksIndex();

        put("/_ilm/policy/books-retention", Map.of(
                "policy", Map.of(
                        "phases", Map.of(
                                "hot", Map.of("min_age", "0ms"),
                                "warm", Map.of("min_age", "1h"),
                                "delete", Map.of("min_age", "7d")
                        )
                )
        ), 200);
        put("/books/_ilm/policy/books-retention", Map.of(), 200);

        Map<String, Object> state = get("/_cluster/state", 200);
        Map<String, Object> policies = (Map<String, Object>) state.get("lifecyclePolicies");
        Map<String, Object> policy = (Map<String, Object>) policies.get("books-retention");
        Map<String, Object> minAges = (Map<String, Object>) policy.get("minAgeMillisByPhase");
        Map<String, Object> indices = (Map<String, Object>) state.get("indices");
        Map<String, Object> books = (Map<String, Object>) indices.get("books");

        assertEquals(0, ((Number) minAges.get("HOT")).longValue());
        assertEquals(Duration.ofHours(1).toMillis(), ((Number) minAges.get("WARM")).longValue());
        assertEquals(Duration.ofDays(7).toMillis(), ((Number) minAges.get("DELETE")).longValue());
        assertEquals("books-retention", books.get("lifecyclePolicy"));
    }

    @Test
    @Timeout(60)
    @SuppressWarnings("unchecked")
    void lifecycleDeletePhaseRemovesExpiredIndex() throws Exception {
        startServer();
        createBooksIndex();

        put("/_ilm/policy/delete-now", Map.of(
                "policy", Map.of(
                        "phases", Map.of(
                                "delete", Map.of("min_age", "0ms")
                        )
                )
        ), 200);
        put("/books/_ilm/policy/delete-now", Map.of(), 200);

        // The lifecycle delete rides on 1s maintenance ticks; give it headroom under full-suite load.
        waitUntil(() -> {
            Map<String, Object> state = get("/_cluster/state", 200);
            Map<String, Object> indices = (Map<String, Object>) state.get("indices");
            return !indices.containsKey("books");
        }, 30);
    }

    @Test
    @Timeout(60)
    void textMappingAcceptsConfiguredAnalyzer() throws Exception {
        startServer();
        put("/analyzed_books", Map.of(
                "number_of_shards", 1,
                "mappings", Map.of("properties", Map.of(
                        "title", Map.of(
                                "type", "text",
                                "analyzer", "keyword",
                                "search_analyzer", "keyword"
                        )
                ))
        ), 200);
        post("/analyzed_books/_doc/doc-1", Map.of("title", "Lucene in Action"), 201);

        Map<String, Object> tokenSearch = post("/analyzed_books/_search", Map.of(
                "query", Map.of("match", Map.of("title", "lucene")),
                "size", 10
        ), 200);
        Map<String, Object> exactSearch = post("/analyzed_books/_search", Map.of(
                "query", Map.of("match", Map.of("title", "Lucene in Action")),
                "size", 10
        ), 200);

        assertTrue(hitIds(tokenSearch).isEmpty());
        assertEquals(List.of("doc-1"), hitIds(exactSearch));
    }

    @Test
    @Timeout(60)
    void expiredPointInTimeIsClosedByMaintenanceTask() throws Exception {
        startServer();
        createBooksIndex();

        String pitId = stringValue(post("/books/_pit?keep_alive=10ms", Map.of(), 200).get("id"));

        TimeUnit.MILLISECONDS.sleep(2_500);
        assertEquals(404, status("DELETE", "/_pit", Map.of("id", pitId)));
    }

    @Test
    @Timeout(60)
    void strongPointInTimeUsesRemoteSnapshotReader() throws Exception {
        startServer();
        createBooksIndex();
        indexBook("doc-1", "visible", 100, List.of(1.0, 0.0));
        waitUntil(() -> hitIds(post("/books/_search?read_preference=strong", Map.of(
                "query", Map.of("match_all", Map.of()),
                "size", 10
        ), 200)).contains("doc-1"));

        String pitId = stringValue(post("/books/_pit?keep_alive=1m&read_preference=strong", Map.of(), 200).get("id"));
        indexBook("doc-2", "visible", 200, List.of(0.0, 1.0));

        Map<String, Object> pitSearch = post("/books/_search?read_preference=strong", Map.of(
                "pit", Map.of("id", pitId),
                "query", Map.of("match_all", Map.of()),
                "size", 10
        ), 200);

        assertEquals(List.of("doc-1"), hitIds(pitSearch));
        assertTrue((Boolean) delete("/_pit", Map.of("id", pitId), 200).get("succeeded"));
    }

    @Test
    @Timeout(60)
    void snapshotGarbageCollectionRetainsPinnedPitAndDeletesOldUnpinnedGenerations() throws Exception {
        startServer(1);
        createBooksIndex();
        indexBook("doc-1", "visible", 100, List.of(1.0, 0.0));
        waitUntil(() -> hitIds(post("/books/_search?read_preference=strong", Map.of(
                "query", Map.of("match_all", Map.of()),
                "size", 10
        ), 200)).equals(List.of("doc-1")));

        String pitId = stringValue(post("/books/_pit?keep_alive=1m&read_preference=strong", Map.of(), 200).get("id"));
        indexBook("doc-2", "visible", 200, List.of(0.0, 1.0));
        waitUntil(() -> hitIds(post("/books/_search?read_preference=strong", Map.of(
                "query", Map.of("match_all", Map.of()),
                "size", 10
        ), 200)).contains("doc-2"));
        waitUntil(() -> remoteSegmentObjectCount("books", 0) >= 2);

        server.runSnapshotGarbageCollection();
        assertTrue(remoteSegmentObjectCount("books", 0) >= 2);

        assertTrue((Boolean) delete("/_pit", Map.of("id", pitId), 200).get("succeeded"));
        server.runSnapshotGarbageCollection();
        waitUntil(() -> {
            List<String> names = remoteSegmentObjectNames("books", 0);
            assertEquals(1, names.size(), "remote segment objects: " + names);
            return true;
        });
    }

    @Test
    @Timeout(60)
    @SuppressWarnings("unchecked")
    void uploadStatusReportsShardSnapshotAndManualRetry() throws Exception {
        startServer();
        createBooksIndex();
        indexBook("doc-1", "visible", 100, List.of(1.0, 0.0));

        waitUntil(() -> {
            Map<String, Object> status = get("/books/_uploads", 200);
            Map<String, Object> summary = (Map<String, Object>) status.get("summary");
            return ((Number) summary.get("pending_files")).intValue() == 0;
        });

        Map<String, Object> status = get("/books/_uploads", 200);
        Map<String, Object> summary = (Map<String, Object>) status.get("summary");
        assertEquals(0, ((Number) summary.get("pending_files")).intValue());
        List<Map<String, Object>> indices = (List<Map<String, Object>>) status.get("indices");
        List<Map<String, Object>> shards = (List<Map<String, Object>>) indices.getFirst().get("shards");
        Map<String, Object> shard = shards.getFirst();
        assertTrue((Boolean) shard.get("remote_snapshot_ready"));
        assertTrue(((Number) shard.get("latest_snapshot_generation")).longValue() > 0);

        Map<String, Object> retry = post("/books/_uploads/_retry", Map.of(), 200);
        Map<String, Object> retrySummary = (Map<String, Object>) retry.get("summary");
        assertEquals(0, ((Number) retrySummary.get("pending_files")).intValue());
    }

    @Test
    @Timeout(60)
    @SuppressWarnings("unchecked")
    void observabilityEndpointsReportRoutingSnapshotsAndNodeStats() throws Exception {
        startServer();
        createBooksIndex(2);
        indexBook("doc-1", "visible", 100, List.of(1.0, 0.0));
        waitUntil(() -> hitIds(post("/books/_search?read_preference=strong", Map.of(
                "query", Map.of("match_all", Map.of()),
                "size", 10
        ), 200)).contains("doc-1"));

        Map<String, Object> health = get("/_cluster/health", 200);
        assertEquals("green", health.get("status"));
        assertEquals(2, ((Number) health.get("active_shards")).intValue());

        Map<String, Object> shards = get("/_shards", 200);
        List<Map<String, Object>> shardRows = (List<Map<String, Object>>) shards.get("shards");
        assertEquals(2, shardRows.size());
        assertTrue(shardRows.stream().allMatch(row -> row.containsKey("owner_node")));
        assertTrue(shardRows.stream().anyMatch(row -> row.get("latest_snapshot_generation") != null));

        Map<String, Object> indices = get("/_indices", 200);
        List<Map<String, Object>> indexRows = (List<Map<String, Object>>) indices.get("indices");
        assertEquals(1, indexRows.size());
        Map<String, Object> booksIndex = indexRows.getFirst();
        assertEquals("books", booksIndex.get("name"));
        assertEquals("green", booksIndex.get("status"));
        assertEquals(2, ((Number) booksIndex.get("number_of_shards")).intValue());
        assertEquals(3, ((Number) booksIndex.get("mapping_fields")).intValue());
        Map<String, Object> indexShardSummary = (Map<String, Object>) booksIndex.get("shards");
        assertEquals(2, ((Number) indexShardSummary.get("total")).intValue());
        assertEquals(2, ((Number) indexShardSummary.get("started")).intValue());

        Map<String, Object> indexDetail = get("/books", 200);
        Map<String, Object> booksDetail = (Map<String, Object>) indexDetail.get("books");
        Map<String, Object> mappings = (Map<String, Object>) booksDetail.get("mappings");
        Map<String, Object> properties = (Map<String, Object>) mappings.get("properties");
        Map<String, Object> category = (Map<String, Object>) properties.get("category");
        assertEquals("keyword", category.get("type"));

        Map<String, Object> snapshotStatus = get("/_snapshot_status", 200);
        Map<String, Object> snapshotSummary = (Map<String, Object>) snapshotStatus.get("summary");
        assertEquals(0, ((Number) snapshotSummary.get("stuck_shards")).intValue());

        Map<String, Object> nodeStats = get("/_nodes/stats", 200);
        Map<String, Object> nodes = (Map<String, Object>) nodeStats.get("nodes");
        Map<String, Object> stats = (Map<String, Object>) nodes.values().iterator().next();
        assertTrue(stats.containsKey("cache"));
        assertTrue(stats.containsKey("cache_cleanup"));
        assertTrue(stats.containsKey("s3"));

        Map<String, Object> plan = post("/books/_search_plan?read_preference=strong", Map.of(
                "query", Map.of("match_all", Map.of())
        ), 200);
        List<Map<String, Object>> targets = (List<Map<String, Object>>) plan.get("targets");
        assertTrue(targets.stream().allMatch(target -> Boolean.TRUE.equals(target.get("remoteSnapshot"))));
        assertTrue(targets.stream().anyMatch(target -> target.get("remoteSnapshotGeneration") != null));
    }

    @Test
    @Timeout(15)
    void startFailsWhenEtcdIsUnavailableBeyondTimeout() throws Exception {
        int unavailableEtcdPort = freePort();
        port = freePort();
        server = new HttpApiServer(new ServerOptions(
                port,
                "test-cluster",
                "node-1",
                "node-1",
                "127.0.0.1",
                Set.of(NodeRole.MASTER, NodeRole.DATA, NodeRole.COORDINATING),
                "http://127.0.0.1:" + unavailableEtcdPort,
                "test/ns/unavailable",
                tempDir.toString(),
                null,
                null,
                "https",
                null,
                false,
                null,
                null,
                2,
                1
        ));

        assertThrows(Exception.class, () ->
                server.start().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS));
    }

    @Test
    @Timeout(60)
    @SuppressWarnings("unchecked")
    void createIndexDoesNotExposeShardCopySettings() throws Exception {
        startServer();

        put("/books", Map.of("number_of_shards", 1), 200);
        assertEquals(400, status("PUT", "/bad-books", Map.of(
                "number_of_shards", 1,
                "number_of_replicas", 1
        )));

        Map<String, Object> state = get("/_cluster/state", 200);
        Map<String, Object> indices = (Map<String, Object>) state.get("indices");
        Map<String, Object> books = (Map<String, Object>) indices.get("books");
        assertFalse(books.containsKey("numberOfReplicas"));
    }

    @Test
    @Timeout(60)
    @SuppressWarnings("unchecked")
    void createIndexAcceptsSettingsObjectAndRejectsNestedReplicas() throws Exception {
        startServer();

        put("/books", Map.of(
                "settings", Map.of(
                        "number_of_shards", 3,
                        "number_of_replicas", 0
                ),
                "mappings", Map.of("properties", Map.of(
                        "title", Map.of("type", "text")
                ))
        ), 200);

        Map<String, Object> state = get("/_cluster/state", 200);
        Map<String, Object> indices = (Map<String, Object>) state.get("indices");
        Map<String, Object> books = (Map<String, Object>) indices.get("books");
        assertEquals(3, ((Number) books.get("numberOfShards")).intValue());

        assertEquals(400, status("PUT", "/bad-nested-books", Map.of(
                "settings", Map.of(
                        "index", Map.of(
                                "number_of_shards", 1,
                                "number_of_replicas", 1
                        )
                )
        )));
        assertEquals(400, status("PUT", "/bad-dotted-books", Map.of(
                "settings", Map.of(
                        "index.number_of_replicas", 1
                )
        )));
    }

    @Test
    @Timeout(60)
    void createIndexRejectsInvalidNames() throws Exception {
        startServer();

        // Uppercase, leading underscore/dash and overlong names must all map to 400. Slash and
        // dot-segment names never reach the handler (Vert.x routing rejects them first); those
        // are covered by the validation unit test in ClusterIndexServiceTest.
        assertEquals(400, status("PUT", "/Books", Map.of("number_of_shards", 1)));
        assertEquals(400, status("PUT", "/_bad", Map.of("number_of_shards", 1)));
        assertEquals(400, status("PUT", "/-bad", Map.of("number_of_shards", 1)));
        assertEquals(400, status("PUT", "/" + "a".repeat(201), Map.of("number_of_shards", 1)));

        put("/books", Map.of("number_of_shards", 1), 200);
    }

    @Test
    @Timeout(60)
    @SuppressWarnings("unchecked")
    void autoGeneratedDocumentIdsAreUsedForShardRouting() throws Exception {
        startServer();
        createBooksIndex(3);

        for (int i = 0; i < 8; i++) {
            Map<String, Object> response = post("/books/_doc", Map.of(
                    "category", "auto",
                    "pages", i
            ), 201);
            String id = stringValue(response.get("id"));
            Map<String, Object> shardId = (Map<String, Object>) response.get("shardId");
            assertEquals(expectedShard(id, 3), ((Number) shardId.get("shardNumber")).intValue());
        }

        Map<String, Object> search = post("/books/_search", Map.of(
                "query", Map.of("term", Map.of("category", "auto")),
                "size", 20
        ), 200);
        assertEquals(8, hitIds(search).size());
    }

    @Test
    @Timeout(60)
    @SuppressWarnings("unchecked")
    void bulkIndexesDeletesAndRoutesAutoGeneratedIds() throws Exception {
        startServer();
        createBooksIndex(3);

        String body = """
                {"index":{"_index":"books","_id":"doc-1"}}
                {"category":"bulk","pages":1,"embedding":[1.0,0.0]}
                {"create":{"_index":"books","_id":"doc-2"}}
                {"category":"bulk","pages":2,"embedding":[0.0,1.0]}
                {"index":{"_index":"books"}}
                {"category":"bulk","pages":3,"embedding":[1.0,1.0]}
                {"delete":{"_index":"books","_id":"doc-1"}}
                """;
        Map<String, Object> response = postRaw("/_bulk", body, 200);

        assertEquals(false, response.get("errors"));
        List<Map<String, Object>> items = (List<Map<String, Object>>) response.get("items");
        assertEquals(4, items.size());
        Map<String, Object> auto = (Map<String, Object>) items.get(2).get("index");
        String autoId = stringValue(auto.get("_id"));
        Map<String, Object> autoShard = (Map<String, Object>) auto.get("shardId");
        assertEquals(expectedShard(autoId, 3), ((Number) autoShard.get("shardNumber")).intValue());
        Map<String, Object> created = (Map<String, Object>) items.get(1).get("create");
        assertEquals("created", created.get("result"));

        Map<String, Object> search = post("/books/_search", Map.of(
                "query", Map.of("term", Map.of("category", "bulk")),
                "size", 10
        ), 200);
        List<String> ids = hitIds(search);
        assertFalse(ids.contains("doc-1"));
        assertTrue(ids.contains("doc-2"));
        assertTrue(ids.contains(autoId));

        Map<String, Object> conflict = postRaw("/books/_bulk", """
                {"create":{"_id":"doc-2"}}
                {"category":"bulk","pages":4,"embedding":[1.0,0.0]}
                """, 200);
        assertEquals(true, conflict.get("errors"));
        List<Map<String, Object>> conflictItems = (List<Map<String, Object>>) conflict.get("items");
        Map<String, Object> conflictItem = (Map<String, Object>) conflictItems.getFirst().get("create");
        assertEquals(409, ((Number) conflictItem.get("status")).intValue());
    }

    @Test
    @Timeout(60)
    @SuppressWarnings("unchecked")
    void protobufSearchAndBulkUseBinaryRequestAndResponseBodies() throws Exception {
        startServer();
        createBooksIndex();

        Map<String, Object> bulk = requestProtobuf("POST", "/_bulk",
                com.github.wxk6b1203.http.proto.BulkRequest.newBuilder()
                        .addItems(com.github.wxk6b1203.http.proto.BulkItem.newBuilder()
                                .setAction(com.github.wxk6b1203.http.proto.BulkAction.BULK_ACTION_INDEX)
                                .setIndex("books")
                                .setId("doc-1")
                                .setSource(HttpApiProtobuf.toStruct(Map.of(
                                        "category", "proto",
                                        "pages", 10,
                                        "embedding", List.of(1.0, 0.0)
                                ))))
                        .addItems(com.github.wxk6b1203.http.proto.BulkItem.newBuilder()
                                .setAction(com.github.wxk6b1203.http.proto.BulkAction.BULK_ACTION_INDEX)
                                .setIndex("books")
                                .setId("doc-2")
                                .setSource(HttpApiProtobuf.toStruct(Map.of(
                                        "category", "json",
                                        "pages", 20,
                                        "embedding", List.of(0.0, 1.0)
                                ))))
                        .build(),
                200);
        assertEquals(false, bulk.get("errors"));
        List<Map<String, Object>> bulkItems = (List<Map<String, Object>>) bulk.get("items");
        assertEquals(2, bulkItems.size());
        Map<String, Object> firstItem = (Map<String, Object>) bulkItems.getFirst().get("index");
        assertTrue(firstItem.get("shardId") instanceof Map<?, ?>);
        Map<String, Object> firstShard = (Map<String, Object>) firstItem.get("shardId");
        assertEquals("books", firstShard.get("indexName"));
        assertEquals(expectedShard("doc-1", 1), ((Number) firstShard.get("shardNumber")).intValue());

        Map<String, Object> malformedBulk = requestProtobuf("POST", "/_bulk",
                com.github.wxk6b1203.http.proto.BulkRequest.newBuilder()
                        .addItems(com.github.wxk6b1203.http.proto.BulkItem.newBuilder()
                                .setAction(com.github.wxk6b1203.http.proto.BulkAction.BULK_ACTION_INDEX)
                                .setIndex("books")
                                .setId("missing-source"))
                        .build(),
                400);
        assertEquals("IllegalArgumentException", malformedBulk.get("type"));

        Map<String, Object> search = requestProtobuf("POST", "/books/_search",
                com.github.wxk6b1203.http.proto.SearchRequest.newBuilder()
                        .setQuery(com.github.wxk6b1203.http.proto.Query.newBuilder()
                                .setTerm(com.github.wxk6b1203.http.proto.FieldValueQuery.newBuilder()
                                        .setField("category")
                                        .setValue(Value.newBuilder().setStringValue("proto"))))
                        .setSize(10)
                        .build(),
                200);
        assertEquals(List.of("doc-1"), hitIds(search));
        assertTrue(((Number) search.get("tookMillis")).longValue() > 0);

        Map<String, Object> invalidReadPreference = requestProtobuf("POST", "/books/_search",
                com.github.wxk6b1203.http.proto.SearchRequest.newBuilder()
                        .setReadPreference("owner")
                        .build(),
                400);
        assertEquals("IllegalArgumentException", invalidReadPreference.get("type"));
        assertEquals("read_preference must be weak or strong", invalidReadPreference.get("error"));

        Map<String, Object> invalidQueryReadPreference = requestProtobuf(
                "POST",
                "/books/_search?read_preference=remote",
                com.github.wxk6b1203.http.proto.SearchRequest.newBuilder().build(),
                400);
        assertEquals("IllegalArgumentException", invalidQueryReadPreference.get("type"));

        Map<String, Object> filteredVectorSearch = requestProtobuf("POST", "/books/_search",
                com.github.wxk6b1203.http.proto.SearchRequest.newBuilder()
                        .setQuery(com.github.wxk6b1203.http.proto.Query.newBuilder()
                                .setTerm(com.github.wxk6b1203.http.proto.FieldValueQuery.newBuilder()
                                        .setField("category")
                                        .setValue(Value.newBuilder().setStringValue("proto"))))
                        .setVector(com.github.wxk6b1203.http.proto.VectorQuery.newBuilder()
                                .setField("embedding")
                                .addVector(0.0f)
                                .addVector(1.0f)
                                .setK(10)
                                .setNumCandidates(10)
                                .setFilter(com.github.wxk6b1203.http.proto.Query.newBuilder()
                                        .setRange(com.github.wxk6b1203.http.proto.RangeQuery.newBuilder()
                                                .setField("pages")
                                                .setGte(Value.newBuilder().setNumberValue(15)))))
                        .setSize(10)
                        .build(),
                200);
        assertEquals(List.of(), hitIds(filteredVectorSearch));

        Map<String, Object> defaultKnnSearch = requestProtobuf("POST", "/books/_knn_search",
                com.github.wxk6b1203.http.proto.SearchRequest.newBuilder()
                        .setVector(com.github.wxk6b1203.http.proto.VectorQuery.newBuilder()
                                .setField("embedding")
                                .addVector(1.0f)
                                .addVector(0.0f))
                        .build(),
                200);
        assertEquals(List.of("doc-1", "doc-2"), hitIds(defaultKnnSearch));

        Map<String, Object> malformedKnnSearch = requestProtobuf("POST", "/books/_knn_search",
                com.github.wxk6b1203.http.proto.SearchRequest.newBuilder().build(),
                400);
        assertEquals("IllegalArgumentException", malformedKnnSearch.get("type"));
        assertEquals("knn search requires vector", malformedKnnSearch.get("error"));

        Map<String, Object> jsonSearch = post("/books/_search", Map.of(
                "query", Map.of("term", Map.of("category", "json")),
                "size", 10
        ), 200);
        assertEquals(List.of("doc-2"), hitIds(jsonSearch));

        Map<String, Object> nodes = requestProtobuf("GET", "/_nodes", null, 200);
        assertFalse(nodes.isEmpty());
        assertTrue(nodes.values().iterator().next() instanceof Map<?, ?>);
    }

    @Test
    @Timeout(60)
    void bulkBackpressureRejectsOversizedRequests() throws Exception {
        startServer(2, "async", true, 0, 0, "immediate", 0, 1, 0);
        createBooksIndex();

        postRaw("/_bulk", """
                {"index":{"_index":"books","_id":"doc-1"}}
                {"category":"bulk","pages":1,"embedding":[1.0,0.0]}
                {"index":{"_index":"books","_id":"doc-2"}}
                {"category":"bulk","pages":2,"embedding":[0.0,1.0]}
                """, 429);

        postRaw("/_bulk", """
                {"index":{"_index":"books","_id":"doc-4"}}
                {"category":"ok","pages":4,"embedding":[1.0,0.0]}
                """, 200);
        Map<String, Object> search = post("/books/_search", Map.of(
                "query", Map.of("term", Map.of("category", "ok"))
        ), 200);
        assertEquals(List.of("doc-4"), hitIds(search));
    }

    @Test
    @Timeout(60)
    void bulkBodyLimitRejectsBeforeBulkHandler() throws Exception {
        startServer(2, "async", true, 0, 0, "immediate", 0, 0, 120);
        createBooksIndex();

        assertEquals(413, responseRaw("POST", "/_bulk", """
                {"index":{"_index":"books","_id":"doc-3"}}
                {"category":"bulk","pages":333,"embedding":[1.0,1.0],"description":"this body should exceed the configured bulk byte limit"}
                """).status());

        postRaw("/_bulk", """
                {"index":{"_index":"books","_id":"doc-4"}}
                {"category":"ok","pages":4,"embedding":[1.0,0.0]}
                """, 200);
    }

    @Test
    @Timeout(60)
    void httpErrorsUseSpecificStatusCodes() throws Exception {
        startServer();

        assertEquals(404, status("GET", "/missing/_mapping", null));
        assertEquals(404, status("GET", "/missing", null));
        assertEquals(404, status("POST", "/missing/_search", Map.of(
                "query", Map.of("match_all", Map.of())
        )));

        createBooksIndex();
        assertEquals(409, status("PUT", "/books", Map.of("number_of_shards", 1)));
        indexBook("doc-1", "visible", 100, List.of(1.0, 0.0));
        assertEquals(409, status("POST", "/books/_doc/doc-1?op_type=create", Map.of(
                "category", "visible",
                "pages", 100,
                "embedding", List.of(1.0, 0.0)
        )));
        assertEquals(400, status("POST", "/books/_search", Map.of(
                "query", Map.of("match_all", Map.of()),
                "sort", List.of(Map.of("unknown", Map.of()))
        )));
    }

    @Test
    @Timeout(60)
    @SuppressWarnings("unchecked")
    void internalShardBulkRequiresFenceAndExecutesItems() throws Exception {
        startServer();
        createBooksIndex();

        assertEquals(400, status("POST", "/_internal/books/0/_bulk", Map.of(
                "items", List.of(Map.of(
                        "action", "index",
                        "index", "books",
                        "id", "doc-1",
                        "source", Map.of("category", "internal", "pages", 1)
                ))
        )));

        Map<String, Object> route = get("/books/_write_route?routing=doc-1", 200);
        Map<String, Object> bulk = post("/_internal/books/0/_bulk", Map.of(
                "owner_term", route.get("ownerTerm"),
                "allocation_epoch", route.get("allocationEpoch"),
                "items", List.of(
                        Map.of(
                                "action", "index",
                                "index", "books",
                                "id", "doc-1",
                                "source", Map.of("category", "internal", "pages", 1)
                        ),
                        Map.of(
                                "action", "delete",
                                "index", "books",
                                "id", "doc-1",
                                "source", Map.of()
                        ),
                        Map.of(
                                "action", "create",
                                "index", "books",
                                "id", "doc-1",
                                "source", Map.of("category", "internal", "pages", 3)
                        ),
                        Map.of(
                                "action", "create",
                                "index", "books",
                                "id", "doc-2",
                                "source", Map.of("category", "internal", "pages", 2)
                        )
                )
        ), 200);
        assertEquals(false, bulk.get("errors"));
        List<Map<String, Object>> items = (List<Map<String, Object>>) bulk.get("items");
        assertEquals(4, items.size());

        Map<String, Object> search = post("/books/_search", Map.of(
                "query", Map.of("term", Map.of("category", "internal")),
                "size", 10
        ), 200);
        assertEquals(List.of("doc-1", "doc-2"), hitIds(search).stream().sorted().toList());
    }

    @Test
    @Timeout(60)
    void internalShardBulkSkipsPublicBodySizeLimit() throws Exception {
        startServer(2, "async", true, 0, 0, "immediate", 0, 10, 20);
        createBooksIndex();

        Map<String, Object> route = get("/books/_write_route?routing=doc-1", 200);
        Map<String, Object> bulk = post("/_internal/books/0/_bulk", Map.of(
                "owner_term", route.get("ownerTerm"),
                "allocation_epoch", route.get("allocationEpoch"),
                "items", List.of(Map.of(
                        "action", "index",
                        "index", "books",
                        "id", "doc-1",
                        "source", Map.of(
                                "category", "internal",
                                "pages", 1,
                                "description", "internal envelope is larger than public NDJSON body limit"
                        )
                ))
        ), 200);

        assertEquals(false, bulk.get("errors"));
    }

    @Test
    @Timeout(60)
    void deleteIndexClearsLocalAndRemoteShardDataBeforeSameNameRecreate() throws Exception {
        startServer();
        createBooksIndex();
        indexBook("doc-1", "visible", 100, List.of(1.0, 0.0));

        delete("/books", Map.of(), 200);
        createBooksIndex();

        Map<String, Object> response = post("/books/_search", Map.of(
                "query", Map.of("match_all", Map.of()),
                "size", 10
        ), 200);
        assertTrue(hitIds(response).isEmpty());
    }

    @Test
    @Timeout(60)
    void byQueryWritesCarryShardFenceAndInternalWritesRequireIt() throws Exception {
        startServer(2, "async", true, 0, 0, "immediate", 1, 0, 0);
        createBooksIndex();
        indexBook("doc-1", "visible", 100, List.of(1.0, 0.0));

        Map<String, Object> update = post("/books/_update_by_query", Map.of(
                "query", Map.of("term", Map.of("category", "visible")),
                "doc", Map.of("pages", 101)
        ), 200);
        assertEquals("updated=1", update.get("status"));

        assertEquals(400, status("POST", "/_internal/books/0/_update_by_query", Map.of(
                "query", Map.of("term", Map.of("category", "visible")),
                "doc", Map.of("pages", 102)
        )));

        Map<String, Object> route = get("/books/_write_route?routing=doc-1", 200);
        Map<String, Object> delete = post("/_internal/books/0/_delete_by_query", Map.of(
                "query", Map.of("term", Map.of("category", "visible")),
                "owner_term", route.get("ownerTerm"),
                "allocation_epoch", route.get("allocationEpoch")
        ), 200);
        assertEquals("deleted=1", delete.get("status"));
    }

    @Test
    @Timeout(60)
    void httpSearchSupportsByteVectorKnn() throws Exception {
        startServer();
        put("/byte_books", Map.of(
                "number_of_shards", 1,
                "mappings", Map.of("properties", Map.of(
                        "category", Map.of("type", "keyword"),
                        "byte_embedding", Map.of(
                                "type", "byte_vector",
                                "dimension", 4,
                                "similarity", "cosine"
                        )
                ))
        ), 200);
        post("/byte_books/_doc/doc-1", Map.of(
                "category", "left",
                "byte_embedding", List.of(10, 0, 0, 0)
        ), 201);
        post("/byte_books/_doc/doc-2", Map.of(
                "category", "right",
                "byte_embedding", List.of(0, 10, 0, 0)
        ), 201);

        Map<String, Object> response = post("/byte_books/_search", Map.of(
                "knn", Map.of(
                        "field", "byte_embedding",
                        "query_vector", List.of(10, 0, 0, 0),
                        "k", 1,
                        "num_candidates", 10
                )
        ), 200);
        assertEquals(List.of("doc-1"), hitIds(response));
    }

    @Test
    @Timeout(60)
    void uploadWaitStrategyMakesStrongReadVisibleAfterWriteReturns() throws Exception {
        startServer(2, "wait_for_upload");
        createBooksIndex();
        indexBook("doc-1", "visible", 100, List.of(1.0, 0.0));

        Map<String, Object> response = post("/books/_search?read_preference=strong", Map.of(
                "query", Map.of("term", Map.of("category", "visible")),
                "size", 10
        ), 200);
        assertEquals(List.of("doc-1"), hitIds(response));
    }

    @Test
    @Timeout(60)
    @SuppressWarnings("unchecked")
    void weakSearchUsesLocalOwnerWhenCommitsAreDeferred() throws Exception {
        startServer(2, "wait_for_upload", false, 60_000, 2, "immediate");
        createBooksIndex();
        indexBook("doc-1", "visible", 100, List.of(1.0, 0.0));
        indexBook("doc-2", "visible", 200, List.of(0.0, 1.0));

        Map<String, Object> committedPlan = post("/books/_search_plan", Map.of(
                "query", Map.of("match_all", Map.of())
        ), 200);
        List<Map<String, Object>> committedTargets = (List<Map<String, Object>>) committedPlan.get("targets");
        assertTrue(committedTargets.stream().noneMatch(target -> Boolean.TRUE.equals(target.get("remoteSnapshot"))));

        indexBook("doc-3", "fresh", 300, List.of(1.0, 1.0));

        Map<String, Object> weakSearch = post("/books/_search", Map.of(
                "query", Map.of("term", Map.of("category", "fresh")),
                "size", 10
        ), 200);
        assertEquals(List.of("doc-3"), hitIds(weakSearch));

        Map<String, Object> strongSearch = post("/books/_search?read_preference=strong", Map.of(
                "query", Map.of("term", Map.of("category", "fresh")),
                "size", 10
        ), 200);
        assertTrue(hitIds(strongSearch).isEmpty());
    }

    private void startServer() throws Exception {
        startServer(2);
    }

    private void startServer(int snapshotRetainLatest) throws Exception {
        startServer(snapshotRetainLatest, "async");
    }

    private void startServer(int snapshotRetainLatest, String uploadWaitStrategy) throws Exception {
        startServer(snapshotRetainLatest, uploadWaitStrategy, true, 0, 0, "immediate");
    }

    private void startServer(
            int snapshotRetainLatest,
            String uploadWaitStrategy,
            boolean commitEveryRequest,
            int commitIntervalMillis,
            int commitAfterDocs,
            String refreshPolicy
    ) throws Exception {
        startServer(snapshotRetainLatest, uploadWaitStrategy, commitEveryRequest, commitIntervalMillis,
                commitAfterDocs, refreshPolicy, 0, 0, 0);
    }

    private void startServer(
            int snapshotRetainLatest,
            String uploadWaitStrategy,
            boolean commitEveryRequest,
            int commitIntervalMillis,
            int commitAfterDocs,
            String refreshPolicy,
            int maxWriteRequests,
            int maxBulkItems,
            long maxBulkBytes
    ) throws Exception {
        port = freePort();
        server = new HttpApiServer(new ServerOptions(
                port,
                "test-cluster",
                "node-1",
                "node-1",
                "127.0.0.1",
                Set.of(NodeRole.MASTER, NodeRole.DATA, NodeRole.COORDINATING),
                null,
                "test/ns",
                tempDir.toString(),
                null,
                null,
                "https",
                null,
                false,
                false,
                null,
                null,
                snapshotRetainLatest,
                10,
                10,
                uploadWaitStrategy,
                5,
                commitEveryRequest,
                commitIntervalMillis,
                commitAfterDocs,
                refreshPolicy,
                1000,
                null,
                0,
                60,
                0,
                maxWriteRequests,
                maxBulkItems,
                maxBulkBytes,
                0,
                0,
                0,
                0
        ));
        server.start().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
    }

    private int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private void createBooksIndex() throws Exception {
        createBooksIndex(1);
    }

    private void createBooksIndex(int numberOfShards) throws Exception {
        put("/books", Map.of(
                "number_of_shards", numberOfShards,
                "mappings", Map.of("properties", Map.of(
                        "category", Map.of("type", "keyword"),
                        "pages", Map.of("type", "long"),
                        "embedding", Map.of(
                                "type", "dense_vector",
                                "dimension", 2,
                                "similarity", "cosine"
                        )
                ))
        ), 200);
    }

    private int expectedShard(String routing, int numberOfShards) {
        CRC32 crc32 = new CRC32();
        crc32.update(routing.getBytes(StandardCharsets.UTF_8));
        return Math.toIntExact(Math.floorMod(crc32.getValue(), numberOfShards));
    }

    private long remoteSegmentObjectCount(String indexName, int shard) throws IOException {
        return remoteSegmentObjectNames(indexName, shard).size();
    }

    private List<String> remoteSegmentObjectNames(String indexName, int shard) throws IOException {
        Path dataPath = tempDir.resolve("remote-objects")
                .resolve(indexName + "__shard_" + shard)
                .resolve("_data");
        if (!Files.isDirectory(dataPath)) {
            return List.of();
        }
        try (var paths = Files.walk(dataPath)) {
            return paths
                    .filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().startsWith("segments_"))
                    .map(path -> path.getFileName().toString())
                    .sorted()
                    .toList();
        }
    }

    private void indexBook(String id, String category, int pages, List<Double> embedding) throws Exception {
        post("/books/_doc/" + id, Map.of(
                "category", category,
                "pages", pages,
                "embedding", embedding
        ), 201);
    }

    private Map<String, FieldMapping> bookMappings() {
        return Map.of(
                "category", new FieldMapping("keyword", null, null, true, true, true),
                "pages", new FieldMapping("long", null, null, true, true, true),
                "embedding", new FieldMapping("dense_vector", 2, "cosine", true, true, false)
        );
    }

    private Map<String, Object> put(String path, Object body, int expectedStatus) throws Exception {
        return request("PUT", path, body, expectedStatus);
    }

    private Map<String, Object> post(String path, Object body, int expectedStatus) throws Exception {
        return request("POST", path, body, expectedStatus);
    }

    private Map<String, Object> postRaw(String path, String body, int expectedStatus) throws Exception {
        Response response = responseRaw("POST", path, body);
        if (response.status() != expectedStatus) {
            fail("POST " + path + " returned " + response.status() + ": " + response.body());
        }
        return response.body() == null || response.body().isBlank()
                ? Map.of()
                : JsonUtil.readValueAsMap(response.body());
    }

    private Map<String, Object> requestProtobuf(String method, String path, MessageLite body, int expectedStatus) throws Exception {
        ProtobufResponse response = responseProtobuf(method, path, body);
        if (response.status() != expectedStatus) {
            fail(method + " " + path + " returned " + response.status() + ": " + response.body());
        }
        return response.body();
    }

    private Map<String, Object> delete(String path, Object body, int expectedStatus) throws Exception {
        return request("DELETE", path, body, expectedStatus);
    }

    private Map<String, Object> get(String path, int expectedStatus) throws Exception {
        return request("GET", path, null, expectedStatus);
    }

    private Map<String, Object> request(String method, String path, Object body, int expectedStatus) throws Exception {
        Response response = response(method, path, body);
        if (response.status() != expectedStatus) {
            fail(method + " " + path + " returned " + response.status() + ": " + response.body());
        }
        return response.body() == null || response.body().isBlank()
                ? Map.of()
                : JsonUtil.readValueAsMap(response.body());
    }

    private int status(String method, String path, Object body) throws Exception {
        return response(method, path, body).status();
    }

    private Response response(String method, String path, Object body) throws Exception {
        URL url = URI.create("http://127.0.0.1:" + port + path).toURL();
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod(method);
        connection.setConnectTimeout((int) Duration.ofSeconds(5).toMillis());
        connection.setReadTimeout((int) Duration.ofSeconds(10).toMillis());
        connection.setRequestProperty("content-type", "application/json");
        connection.setDoInput(true);
        if (body != null) {
            connection.setDoOutput(true);
            try (OutputStream outputStream = connection.getOutputStream()) {
                outputStream.write(JsonUtil.writeValueAsBytes(body));
            }
        }
        int status = connection.getResponseCode();
        String responseBody;
        try (InputStream inputStream = status >= 400 ? connection.getErrorStream() : connection.getInputStream()) {
            responseBody = inputStream == null
                    ? ""
                    : new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
        } finally {
            connection.disconnect();
        }
        return new Response(status, responseBody);
    }

    private ProtobufResponse responseProtobuf(String method, String path, MessageLite body) throws Exception {
        URL url = URI.create("http://127.0.0.1:" + port + path).toURL();
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod(method);
        connection.setConnectTimeout((int) Duration.ofSeconds(5).toMillis());
        connection.setReadTimeout((int) Duration.ofSeconds(10).toMillis());
        connection.setRequestProperty("content-type", HttpApiProtobuf.MEDIA_TYPE);
        connection.setRequestProperty("accept", HttpApiProtobuf.MEDIA_TYPE);
        connection.setDoInput(true);
        if (body != null) {
            connection.setDoOutput(true);
            try (OutputStream outputStream = connection.getOutputStream()) {
                outputStream.write(body.toByteArray());
            }
        }
        int status = connection.getResponseCode();
        byte[] responseBody;
        try (InputStream inputStream = status >= 400 ? connection.getErrorStream() : connection.getInputStream()) {
            responseBody = inputStream == null ? new byte[0] : inputStream.readAllBytes();
        } finally {
            connection.disconnect();
        }
        Map<String, Object> parsed = protobufResponseBody(path, status, responseBody);
        return new ProtobufResponse(status, parsed);
    }

    private Map<String, Object> protobufResponseBody(String path, int status, byte[] responseBody) throws Exception {
        if (responseBody.length == 0) {
            return Map.of();
        }
        if (status >= 400) {
            return HttpApiProtobuf.toMap(com.github.wxk6b1203.http.proto.ErrorResponse.parseFrom(responseBody));
        }
        if (path.contains("_bulk")) {
            return HttpApiProtobuf.toMap(com.github.wxk6b1203.http.proto.BulkResponse.parseFrom(responseBody));
        }
        if (path.contains("_search")) {
            return HttpApiProtobuf.toMap(com.github.wxk6b1203.http.proto.SearchResponse.parseFrom(responseBody));
        }
        return HttpApiProtobuf.toMap(com.google.protobuf.Struct.parseFrom(responseBody));
    }

    private Response responseRaw(String method, String path, String body) throws Exception {
        URL url = URI.create("http://127.0.0.1:" + port + path).toURL();
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod(method);
        connection.setConnectTimeout((int) Duration.ofSeconds(5).toMillis());
        connection.setReadTimeout((int) Duration.ofSeconds(10).toMillis());
        connection.setRequestProperty("content-type", "application/x-ndjson");
        connection.setDoInput(true);
        if (body != null) {
            connection.setDoOutput(true);
            try (OutputStream outputStream = connection.getOutputStream()) {
                outputStream.write(body.getBytes(StandardCharsets.UTF_8));
            }
        }
        int status = connection.getResponseCode();
        String responseBody;
        try (InputStream inputStream = status >= 400 ? connection.getErrorStream() : connection.getInputStream()) {
            responseBody = inputStream == null
                    ? ""
                    : new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
        } finally {
            connection.disconnect();
        }
        return new Response(status, responseBody);
    }

    private void waitUntil(CheckedCondition condition) throws Exception {
        waitUntil(condition, 10);
    }

    private void waitUntil(CheckedCondition condition, long timeoutSeconds) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(timeoutSeconds).toNanos();
        AssertionError lastFailure = null;
        while (System.nanoTime() < deadline) {
            try {
                if (condition.evaluate()) {
                    return;
                }
            } catch (AssertionError e) {
                lastFailure = e;
            }
            TimeUnit.MILLISECONDS.sleep(100);
        }
        if (lastFailure != null) {
            throw lastFailure;
        }
        fail("condition was not met before timeout");
    }

    @SuppressWarnings("unchecked")
    private List<String> hitIds(Map<String, Object> response) {
        return ((List<Map<String, Object>>) response.get("hits")).stream()
                .map(hit -> stringValue(hit.get("id")))
                .toList();
    }

    @SuppressWarnings("unchecked")
    private List<Object> sortValues(Map<String, Object> response) {
        List<Map<String, Object>> hits = (List<Map<String, Object>>) response.get("hits");
        return (List<Object>) hits.getFirst().get("sortValues");
    }

    private String stringValue(Object value) {
        return value == null ? null : value.toString();
    }

    private void deleteChildrenWithRetry(Path root) throws Exception {
        IOException failure = null;
        for (int attempt = 0; attempt < 20; attempt++) {
            try {
                deleteChildren(root);
                return;
            } catch (IOException e) {
                failure = e;
                TimeUnit.MILLISECONDS.sleep(50L * (attempt + 1));
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

    private interface CheckedCondition {
        boolean evaluate() throws Exception;
    }

    private record Response(int status, String body) {
    }

    private record ProtobufResponse(int status, Map<String, Object> body) {
    }
}
