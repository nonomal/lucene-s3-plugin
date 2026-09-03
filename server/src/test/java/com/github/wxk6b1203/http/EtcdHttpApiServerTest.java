package com.github.wxk6b1203.http;

import com.github.wxk6b1203.cluster.ClusterState;
import com.github.wxk6b1203.cluster.NodeRole;
import com.github.wxk6b1203.cluster.ShardRouting;
import com.github.wxk6b1203.cluster.ShardState;
import com.github.wxk6b1203.cluster.etcd.EtcdClusterStateRepository;
import com.github.wxk6b1203.config.ServerOptions;
import com.github.wxk6b1203.util.JsonUtil;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.options.DeleteOption;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class EtcdHttpApiServerTest {
    @TempDir
    Path tempDir;

    private final List<ServerHandle> servers = new ArrayList<>();

    @Test
    @Timeout(90)
    @EnabledIfEnvironmentVariable(named = "ETCD_TEST_ENDPOINTS", matches = ".+")
    @SuppressWarnings("unchecked")
    public void nonMasterWriteReroutesStaleShardOwnerThroughMaster() throws Exception {
        String namespace = "test-http/" + UUID.randomUUID();
        Client client = Client.builder().endpoints(System.getenv("ETCD_TEST_ENDPOINTS")).build();
        try {
            ServerHandle node1 = startServer(namespace, "node-1", Set.of(NodeRole.MASTER, NodeRole.DATA, NodeRole.COORDINATING));
            ServerHandle node2 = startServer(namespace, "node-2", Set.of(NodeRole.DATA, NodeRole.COORDINATING));
            ServerHandle node3 = startServer(namespace, "node-3", Set.of(NodeRole.DATA, NodeRole.COORDINATING));

            waitUntil(() -> get(node3, "/_nodes", 200).keySet().containsAll(Set.of("node-1", "node-2", "node-3")));
            waitUntil(() -> "node-1".equals(get(node1, "/_cluster/state", 200).get("masterNodeId")));
            put(node1, "/books", Map.of(
                    "number_of_shards", 2,
                    "mappings", Map.of("properties", Map.of(
                            "category", Map.of("type", "keyword")
                    ))
            ), 200);
            waitUntil(() -> shardOwner(node1, "books", 1).equals("node-2"));

            closeServer(node2);
            waitUntil(() -> !get(node3, "/_nodes", 200).containsKey("node-2"));

            String routing = routingForShard(1, 2);
            Map<String, Object> indexed = post(node3, "/books/_doc/rerouted-doc?routing=" + routing, Map.of(
                    "category", "rerouted"
            ), 201);
            Map<String, Object> shardId = (Map<String, Object>) indexed.get("shardId");
            assertEquals(1, ((Number) shardId.get("shardNumber")).intValue());
            assertNotEquals("node-2", shardOwner(node1, "books", 1));

            Map<String, Object> search = post(node3, "/books/_search", Map.of(
                    "query", Map.of("term", Map.of("category", "rerouted")),
                    "size", 10
            ), 200);
            assertEquals(List.of("rerouted-doc"), hitIds(search));
        } finally {
            closeServers();
            deletePrefix(client, namespace);
            client.close();
        }
    }

    @Test
    @Timeout(90)
    @EnabledIfEnvironmentVariable(named = "ETCD_TEST_ENDPOINTS", matches = ".+")
    @SuppressWarnings("unchecked")
    public void coordinatingNodeExecutesRemoteShardRpcPaths() throws Exception {
        String namespace = "test-http/" + UUID.randomUUID();
        Client client = Client.builder().endpoints(System.getenv("ETCD_TEST_ENDPOINTS")).build();
        try {
            ServerHandle node1 = startServer(namespace, "node-1", Set.of(NodeRole.MASTER, NodeRole.DATA, NodeRole.COORDINATING));
            startServer(namespace, "node-2", Set.of(NodeRole.DATA, NodeRole.COORDINATING));
            ServerHandle node3 = startServer(namespace, "node-3", Set.of(NodeRole.COORDINATING));

            waitUntil(() -> get(node3, "/_nodes", 200).keySet().containsAll(Set.of("node-1", "node-2", "node-3")));
            waitUntil(() -> "node-1".equals(get(node1, "/_cluster/state", 200).get("masterNodeId")));
            put(node1, "/books", Map.of(
                    "number_of_shards", 1,
                    "mappings", Map.of("properties", Map.of(
                            "category", Map.of("type", "keyword"),
                            "pages", Map.of("type", "long")
                    ))
            ), 200);
            waitUntil(() -> hasShardOwner(node3, "books", 0));

            int shardNumber = 0;
            assertNotEquals("node-3", shardOwner(node3, "books", shardNumber));
            String routing = routingForShard(shardNumber, 1);
            String bulkBody = """
                    {"index":{"_index":"books","_id":"bulk-doc","routing":"%s"}}
                    {"category":"remote-bulk","pages":1}
                    """.formatted(routing);
            Map<String, Object> bulk = postRaw(node3, "/_bulk", bulkBody, 200);
            assertFalse((Boolean) bulk.get("errors"));

            Map<String, Object> search = post(node3, "/books/_search", Map.of(
                    "query", Map.of("term", Map.of("category", "remote-bulk")),
                    "routing", routing,
                    "size", 10
            ), 200);
            assertEquals(List.of("bulk-doc"), hitIds(search));

            String pitId = stringValue(post(node3, "/books/_pit?keep_alive=1m", Map.of(), 200).get("id"));
            Map<String, Object> pitSearch = post(node3, "/books/_search", Map.of(
                    "pit", Map.of("id", pitId),
                    "query", Map.of("term", Map.of("category", "remote-bulk")),
                    "size", 10
            ), 200);
            assertEquals(List.of("bulk-doc"), hitIds(pitSearch));
            assertTrue((Boolean) delete(node3, "/_pit", Map.of("id", pitId), 200).get("succeeded"));

            Map<String, Object> update = post(node3, "/books/_update_by_query", Map.of(
                    "query", Map.of("term", Map.of("category", "remote-bulk")),
                    "routing", routing,
                    "doc", Map.of("pages", 11)
            ), 200);
            assertEquals("updated=1", update.get("status"));
            Map<String, Object> updated = post(node3, "/books/_search", Map.of(
                    "query", Map.of("term", Map.of("category", "remote-bulk")),
                    "routing", routing,
                    "size", 10
            ), 200);
            Map<String, Object> source = (Map<String, Object>) ((List<Map<String, Object>>) updated.get("hits")).getFirst().get("source");
            assertEquals(11, ((Number) source.get("pages")).intValue());

            Map<String, Object> delete = post(node3, "/books/_delete_by_query", Map.of(
                    "query", Map.of("term", Map.of("category", "remote-bulk")),
                    "routing", routing
            ), 200);
            assertEquals("deleted=1", delete.get("status"));
            waitUntil(() -> hitIds(post(node3, "/books/_search", Map.of(
                    "query", Map.of("term", Map.of("category", "remote-bulk")),
                    "routing", routing,
                    "size", 10
            ), 200)).isEmpty());
        } finally {
            closeServers();
            deletePrefix(client, namespace);
            client.close();
        }
    }

    @Test
    @Timeout(120)
    @EnabledIfEnvironmentVariable(named = "ETCD_TEST_ENDPOINTS", matches = ".+")
    public void masterFailoverKeepsCoordinatingWritesAvailable() throws Exception {
        String namespace = "test-http/" + UUID.randomUUID();
        Client client = Client.builder().endpoints(System.getenv("ETCD_TEST_ENDPOINTS")).build();
        try {
            ServerHandle node1 = startServer(namespace, "node-1", Set.of(NodeRole.MASTER, NodeRole.DATA, NodeRole.COORDINATING));
            ServerHandle node2 = startServer(namespace, "node-2", Set.of(NodeRole.MASTER, NodeRole.DATA, NodeRole.COORDINATING));
            ServerHandle node3 = startServer(namespace, "node-3", Set.of(NodeRole.COORDINATING));

            waitUntil(() -> get(node3, "/_nodes", 200).keySet().containsAll(Set.of("node-1", "node-2", "node-3")));
            waitUntil(() -> get(node3, "/_cluster/state", 200).get("masterNodeId") != null);
            String currentMaster = String.valueOf(get(node3, "/_cluster/state", 200).get("masterNodeId"));
            ServerHandle master = "node-1".equals(currentMaster) ? node1 : node2;
            ServerHandle survivor = "node-1".equals(currentMaster) ? node2 : node1;
            put(master, "/books", Map.of(
                    "number_of_shards", 1,
                    "mappings", Map.of("properties", Map.of(
                            "category", Map.of("type", "keyword")
                    ))
            ), 200);
            waitUntil(() -> hasShardOwner(node3, "books", 0));

            closeServer(master);
            waitUntil(() -> survivor.nodeId().equals(get(survivor, "/_cluster/state", 200).get("masterNodeId")));
            waitUntil(() -> survivor.nodeId().equals(shardOwner(node3, "books", 0)));

            post(node3, "/books/_doc/failover-doc", Map.of("category", "failover"), 201);
            waitUntil(() -> hitIds(post(node3, "/books/_search", Map.of(
                    "query", Map.of("term", Map.of("category", "failover")),
                    "size", 10
            ), 200)).equals(List.of("failover-doc")));
        } finally {
            closeServers();
            deletePrefix(client, namespace);
            client.close();
        }
    }

    private ServerHandle startServer(String namespace, String nodeId, Set<NodeRole> roles) throws Exception {
        int port = freePort();
        HttpApiServer server = new HttpApiServer(new ServerOptions(
                port,
                "test-cluster",
                nodeId,
                nodeId,
                "127.0.0.1",
                roles,
                System.getenv("ETCD_TEST_ENDPOINTS"),
                namespace,
                tempDir.resolve(nodeId).toString(),
                null,
                null,
                "https",
                null,
                false,
                null,
                null,
                2
        ));
        server.start().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
        ServerHandle handle = new ServerHandle(nodeId, port, server);
        servers.add(handle);
        return handle;
    }

    private void closeServer(ServerHandle handle) {
        if (servers.remove(handle)) {
            handle.server().close();
        }
    }

    private void closeServers() {
        for (ServerHandle server : List.copyOf(servers)) {
            closeServer(server);
        }
    }

    @SuppressWarnings("unchecked")
    private String shardOwner(ServerHandle server, String indexName, int shardNumber) throws Exception {
        Map<String, Object> state = get(server, "/_cluster/state", 200);
        List<Map<String, Object>> routingTable = (List<Map<String, Object>>) state.get("routingTable");
        return routingTable.stream()
                .filter(routing -> {
                    Map<String, Object> shardId = (Map<String, Object>) routing.get("shardId");
                    return indexName.equals(shardId.get("indexName"))
                            && shardNumber == ((Number) shardId.get("shardNumber")).intValue();
                })
                .map(routing -> String.valueOf(routing.get("nodeId")))
                .findFirst()
                .orElseThrow();
    }

    private String routingForShard(int shardNumber, int numberOfShards) {
        for (int i = 0; i < 10_000; i++) {
            String routing = "routing-" + i;
            if (expectedShard(routing, numberOfShards) == shardNumber) {
                return routing;
            }
        }
        throw new AssertionError("failed to find routing for shard " + shardNumber);
    }

    private int expectedShard(String routing, int numberOfShards) {
        CRC32 crc32 = new CRC32();
        crc32.update(routing.getBytes(StandardCharsets.UTF_8));
        return Math.toIntExact(Math.floorMod(crc32.getValue(), numberOfShards));
    }

    private Map<String, Object> put(ServerHandle server, String path, Object body, int expectedStatus) throws Exception {
        return request(server, "PUT", path, body, expectedStatus);
    }

    private Map<String, Object> post(ServerHandle server, String path, Object body, int expectedStatus) throws Exception {
        return request(server, "POST", path, body, expectedStatus);
    }

    private Map<String, Object> postRaw(ServerHandle server, String path, String body, int expectedStatus) throws Exception {
        Response response = responseRaw(server, "POST", path, body);
        if (response.status() != expectedStatus) {
            fail("POST " + path + " returned " + response.status() + ": " + response.body());
        }
        return response.body() == null || response.body().isBlank()
                ? Map.of()
                : JsonUtil.readValueAsMap(response.body());
    }

    private Map<String, Object> delete(ServerHandle server, String path, Object body, int expectedStatus) throws Exception {
        return request(server, "DELETE", path, body, expectedStatus);
    }

    private Map<String, Object> get(ServerHandle server, String path, int expectedStatus) throws Exception {
        return request(server, "GET", path, null, expectedStatus);
    }

    private Map<String, Object> request(ServerHandle server, String method, String path, Object body, int expectedStatus) throws Exception {
        Response response = response(server, method, path, body);
        if (response.status() != expectedStatus) {
            fail(method + " " + path + " returned " + response.status() + ": " + response.body());
        }
        return response.body() == null || response.body().isBlank()
                ? Map.of()
                : JsonUtil.readValueAsMap(response.body());
    }

    private Response response(ServerHandle server, String method, String path, Object body) throws Exception {
        URL url = URI.create("http://127.0.0.1:" + server.port() + path).toURL();
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod(method);
        connection.setConnectTimeout((int) Duration.ofSeconds(5).toMillis());
        connection.setReadTimeout((int) Duration.ofSeconds(20).toMillis());
        connection.setRequestProperty("content-type", "application/json");
        connection.setRequestProperty("connection", "close");
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

    private Response responseRaw(ServerHandle server, String method, String path, String body) throws Exception {
        URL url = URI.create("http://127.0.0.1:" + server.port() + path).toURL();
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod(method);
        connection.setConnectTimeout((int) Duration.ofSeconds(5).toMillis());
        connection.setReadTimeout((int) Duration.ofSeconds(20).toMillis());
        connection.setRequestProperty("content-type", "application/x-ndjson");
        connection.setRequestProperty("connection", "close");
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

    private int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private void waitUntil(CheckedCondition condition) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
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
                .map(hit -> String.valueOf(hit.get("id")))
                .toList();
    }

    @Test
    @Timeout(120)
    @EnabledIfEnvironmentVariable(named = "ETCD_TEST_ENDPOINTS", matches = ".+")
    @SuppressWarnings("unchecked")
    public void retireDeposedOwnerDrainsUploadsOnlyAndQuarantinesWal() throws Exception {
        String namespace = "test-http/" + UUID.randomUUID();
        Client client = Client.builder().endpoints(System.getenv("ETCD_TEST_ENDPOINTS")).build();
        try {
            ServerHandle node1 = startServer(namespace, "node-1", Set.of(NodeRole.MASTER, NodeRole.DATA, NodeRole.COORDINATING));
            // Real S3 is shared between nodes; point node-2's local "remote" store at node-1's
            // so objects uploaded by node-1 are visible to node-2 (as in production).
            Files.createDirectories(tempDir.resolve("node-1/remote-objects"));
            Files.createDirectories(tempDir.resolve("node-2"));
            Files.createSymbolicLink(tempDir.resolve("node-2/remote-objects"),
                    tempDir.resolve("node-1/remote-objects").toAbsolutePath());
            ServerHandle node2 = startServer(namespace, "node-2", Set.of(NodeRole.DATA, NodeRole.COORDINATING));
            waitUntil(() -> get(node1, "/_nodes", 200).keySet().containsAll(Set.of("node-1", "node-2")));
            waitUntil(() -> "node-1".equals(get(node1, "/_cluster/state", 200).get("masterNodeId")));
            put(node1, "/books", Map.of(
                    "number_of_shards", 1,
                    "mappings", Map.of("properties", Map.of("category", Map.of("type", "keyword")))), 200);
            waitUntil(() -> hasShardOwner(node1, "books", 0));
            // Deterministic allocation: with equal load the lowest node id wins.
            assertEquals("node-1", shardOwner(node1, "books", 0));

            String routing = routingForShard(0, 1);
            post(node2, "/books/_doc/retire-doc?routing=" + routing, Map.of("category", "retire"), 201);
            waitUntil(() -> shardUploadReady(node1, "books", 0));

            // Simulate the master deciding to move the shard to node-2 while node-1 is alive:
            // rewrite the routing entry with a bumped owner term and allocation epoch.
            EtcdClusterStateRepository repository = new EtcdClusterStateRepository(
                    EtcdClusterStateRepository.Options.builder()
                            .endpoints(System.getenv("ETCD_TEST_ENDPOINTS"))
                            .clusterName("test-cluster")
                            .namespace(namespace)
                            .build(),
                    client);
            repository.update(state -> new ClusterState(
                    state.clusterName(),
                    state.version(),
                    state.masterNodeId(),
                    state.nodes(),
                    state.indices(),
                    state.routingTable().stream()
                            .map(routingEntry -> "books".equals(routingEntry.shardId().indexName())
                                    ? new ShardRouting(
                                            routingEntry.shardId(),
                                            ShardState.STARTED,
                                            "node-2",
                                            routingEntry.ownerTerm() + 1,
                                            routingEntry.allocationEpoch() + 1)
                                    : routingEntry)
                            .toList(),
                    state.lifecyclePolicies(),
                    state.updatedAt()));

            // The deposed node's reconcile tick must drain (upload-only), close its writer, and
            // quarantine the WAL so stale same-name files can never shadow the new owner's data.
            waitUntil(() -> !Files.exists(tempDir.resolve("node-1/_wal/books__shard_0"))
                    && hasFiles(tempDir.resolve("node-1/orphan-wal")));
            assertEquals("node-2", shardOwner(node1, "books", 0));

            // New owner accepts writes for the moved shard and both documents stay visible.
            post(node1, "/books/_doc/after-move?routing=" + routing, Map.of("category", "retire"), 201);
            waitUntil(() -> Files.isDirectory(tempDir.resolve("node-2/_wal/books__shard_0/_data")));
            // Weak reads tolerate staleness bounded by the readiness refresh tick (the cached
            // ready flag may predate the newest write by <1s); owner and strong are strict.
            waitUntil(() -> {
                try {
                    return Set.of("retire-doc", "after-move")
                            .equals(Set.copyOf(hitIds(post(node2, "/books/_search", Map.of(
                                    "query", Map.of("term", Map.of("category", "retire")),
                                    "size", 10
                            ), 200))));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            Map<String, Object> ownerSearch = post(node2, "/_internal/books/0/_search", Map.of(
                    "query", Map.of("term", Map.of("category", "retire")),
                    "from", 0,
                    "size", 10,
                    "read_preference", "owner"
            ), 200);
            assertEquals(Set.of("retire-doc", "after-move"), Set.copyOf(hitIds(ownerSearch)));
            Map<String, Object> strongSearch = post(node2, "/books/_search?read_preference=strong", Map.of(
                    "query", Map.of("term", Map.of("category", "retire")),
                    "size", 10
            ), 200);
            assertEquals(Set.of("retire-doc", "after-move"), Set.copyOf(hitIds(strongSearch)));
        } finally {
            closeServers();
            deletePrefix(client, namespace);
            client.close();
        }
    }

    @SuppressWarnings("unchecked")
    private boolean shardUploadReady(ServerHandle server, String indexName, int shardNumber) throws Exception {
        Map<String, Object> status = get(server, "/_uploads", 200);
        List<Map<String, Object>> indices = (List<Map<String, Object>>) status.get("indices");
        for (Map<String, Object> index : indices) {
            if (!indexName.equals(index.get("index"))) {
                continue;
            }
            List<Map<String, Object>> shards = (List<Map<String, Object>>) index.get("shards");
            for (Map<String, Object> shard : shards) {
                if (shardNumber == ((Number) shard.get("shard")).intValue()) {
                    return Boolean.TRUE.equals(shard.get("remote_snapshot_ready"));
                }
            }
        }
        return false;
    }

    private boolean hasFiles(Path path) throws IOException {
        if (!Files.isDirectory(path)) {
            return false;
        }
        try (var files = Files.list(path)) {
            return files.findAny().isPresent();
        }
    }

    private boolean hasShardOwner(ServerHandle server, String indexName, int shardNumber) throws Exception {
        try {
            shardOwner(server, indexName, shardNumber);
            return true;
        } catch (RuntimeException e) {
            return false;
        }
    }

    private String stringValue(Object value) {
        return value == null ? null : value.toString();
    }

    private void deletePrefix(Client client, String namespace) throws Exception {
        String normalized = namespace;
        while (normalized.startsWith("/")) {
            normalized = normalized.substring(1);
        }
        ByteSequence prefix = ByteSequence.from(("/" + normalized).getBytes(StandardCharsets.UTF_8));
        client.getKVClient()
                .delete(prefix, DeleteOption.builder().isPrefix(true).build())
                .get(5, TimeUnit.SECONDS);
    }

    private interface CheckedCondition {
        boolean evaluate() throws Exception;
    }

    private record ServerHandle(String nodeId, int port, HttpApiServer server) {
    }

    private record Response(int status, String body) {
    }
}
