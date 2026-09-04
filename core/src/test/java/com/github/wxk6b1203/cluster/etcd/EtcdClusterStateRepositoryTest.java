package com.github.wxk6b1203.cluster.etcd;

import com.github.wxk6b1203.cluster.ClusterState;
import com.github.wxk6b1203.cluster.FieldMapping;
import com.github.wxk6b1203.cluster.IndexLifecyclePolicy;
import com.github.wxk6b1203.cluster.IndexSettings;
import com.github.wxk6b1203.cluster.LifecyclePhase;
import com.github.wxk6b1203.cluster.ShardId;
import com.github.wxk6b1203.cluster.ShardRouting;
import com.github.wxk6b1203.cluster.ShardState;
import com.github.wxk6b1203.cluster.etcd.EtcdClusterStateRepository;
import com.github.wxk6b1203.util.JsonUtil;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.options.GetOption;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import tools.jackson.databind.JsonNode;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class EtcdClusterStateRepositoryTest {
    @Test
    @EnabledIfEnvironmentVariable(named = "ETCD_TEST_ENDPOINTS", matches = ".+")
    public void roundTripsSplitClusterStateThroughEtcd() throws Exception {
        Client client = Client.builder().endpoints(System.getenv("ETCD_TEST_ENDPOINTS")).build();
        // Leading slash matches the repository's normalized namespace for raw-key assertions.
        String namespace = "/test-cluster/" + UUID.randomUUID();
        EtcdClusterStateRepository repository = repository(client, namespace);
        try {
            // Create one index with mappings, routing and a lifecycle policy in one update.
            repository.update(current -> new ClusterState(
                    current.clusterName(), current.version(), current.masterNodeId(), current.nodes(),
                    Map.of("books", index("books", Map.of(
                            "title", new FieldMapping("text", null, null, null, null),
                            "views", new FieldMapping("long", null, null, null, null)))),
                    List.of(routing("books", 0, "node-1", 1, 1)),
                    Map.of("ilm-1", new IndexLifecyclePolicy("ilm-1", Map.of(LifecyclePhase.WARM, 1000L))),
                    Instant.now()));

            ClusterState state = repository.current();
            assertEquals(1, state.version());
            assertEquals("books", state.indices().keySet().iterator().next());
            assertEquals(2, state.indices().get("books").mappings().size());
            assertEquals("node-1", state.routingTable().getFirst().nodeId());
            assertEquals("ilm-1", state.lifecyclePolicies().keySet().iterator().next());

            // The state key holds only the head (no "indices" field); content lives in split keys.
            JsonNode head = JsonUtil.readValue(rawState(client, namespace), JsonNode.class);
            assertFalse(head.has("indices"));
            assertFalse(head.has("routingTable"));
            assertEquals(1, head.get("version").asLong());
            assertEquals(1, prefixCount(client, namespace, "indices/"));
            assertEquals(1, prefixCount(client, namespace, "routing/"));
            assertEquals(1, prefixCount(client, namespace, "lifecycle/"));

            // Update in place (routing owner term) bumps the version and touches only routing.
            repository.update(current -> new ClusterState(
                    current.clusterName(), current.version(), current.masterNodeId(), current.nodes(),
                    current.indices(),
                    List.of(routing("books", 0, "node-2", 2, 3)),
                    current.lifecyclePolicies(),
                    Instant.now()));
            assertEquals("node-2", repository.current().routingTable().getFirst().nodeId());
            assertEquals(2, repository.current().version());

            // Deleting the index removes its content keys.
            repository.update(current -> new ClusterState(
                    current.clusterName(), current.version(), current.masterNodeId(), current.nodes(),
                    Map.of(), List.of(), current.lifecyclePolicies(), Instant.now()));
            ClusterState empty = repository.current();
            assertTrue(empty.indices().isEmpty());
            assertTrue(empty.routingTable().isEmpty());
            assertEquals(0, prefixCount(client, namespace, "indices/"));
            assertEquals(0, prefixCount(client, namespace, "routing/"));
        } finally {
            client.close();
        }
    }

    @Test
    @EnabledIfEnvironmentVariable(named = "ETCD_TEST_ENDPOINTS", matches = ".+")
    public void handlesStateBeyondLegacySingleKeyLimit() throws Exception {
        Client client = Client.builder().endpoints(System.getenv("ETCD_TEST_ENDPOINTS")).build();
        // Leading slash matches the repository's normalized namespace for raw-key assertions.
        String namespace = "/test-cluster/" + UUID.randomUUID();
        EtcdClusterStateRepository repository = repository(client, namespace);
        try {
            // 1200 indices x 30 fields ~= 6 MB of state. The legacy single-key layout failed at
            // ~1000 indices (5 MB > grpc 2 MB / etcd 1.5 MB request limits) on WRITE.
            Map<String, IndexSettings> indices = new LinkedHashMap<>();
            List<ShardRouting> routings = new java.util.ArrayList<>();
            for (int i = 0; i < 1200; i++) {
                String name = "index_" + i;
                Map<String, FieldMapping> mappings = new LinkedHashMap<>();
                for (int f = 0; f < 30; f++) {
                    mappings.put("field_" + f, new FieldMapping(f % 2 == 0 ? "keyword" : "long", null, null, null, null));
                }
                indices.put(name, index(name, mappings));
                routings.add(routing(name, 0, "node-1", 1, 1));
            }
            repository.update(current -> new ClusterState(
                    current.clusterName(), current.version(), current.masterNodeId(), current.nodes(),
                    indices, routings, Map.of(), Instant.now()));

            ClusterState state = repository.current();
            assertEquals(1200, state.indices().size());
            assertEquals(1200, state.routingTable().size());
            assertEquals(30, state.indices().get("index_7").mappings().size());

            // Incremental update at scale: the diff path rewrites only the touched keys.
            repository.update(current -> {
                Map<String, IndexSettings> updated = new LinkedHashMap<>(current.indices());
                updated.put("index_extra", index("index_extra", Map.of()));
                List<ShardRouting> routing = new java.util.ArrayList<>(current.routingTable());
                routing.add(routing("index_extra", 0, "node-1", 1, 1));
                return new ClusterState(
                        current.clusterName(), current.version(), current.masterNodeId(), current.nodes(),
                        updated, routing, current.lifecyclePolicies(), Instant.now());
            });
            ClusterState after = repository.current();
            assertEquals(1201, after.indices().size());
            assertEquals(2, after.version());
        } finally {
            client.close();
        }
    }

    @Test
    @EnabledIfEnvironmentVariable(named = "ETCD_TEST_ENDPOINTS", matches = ".+")
    public void readsLegacyFullStateKeyAndMigratesOnUpdate() throws Exception {
        Client client = Client.builder().endpoints(System.getenv("ETCD_TEST_ENDPOINTS")).build();
        // Leading slash matches the repository's normalized namespace for raw-key assertions.
        String namespace = "/test-cluster/" + UUID.randomUUID();
        EtcdClusterStateRepository repository = repository(client, namespace);
        try {
            // Seed the pre-split layout: the whole state serialized into the single state key.
            ClusterState legacy = new ClusterState(
                    "lucene-s3", 7, "node-legacy", Map.of(),
                    Map.of("books", index("books", Map.of())),
                    List.of(routing("books", 0, "node-1", 1, 1)),
                    Map.of(), Instant.parse("2026-01-01T00:00:00Z"));
            client.getKVClient().put(
                    ByteSequence.from((namespace + "/state").getBytes(StandardCharsets.UTF_8)),
                    ByteSequence.from(JsonUtil.writeValueAsBytes(legacy))).join();

            ClusterState read = repository.current();
            assertEquals(7, read.version());
            assertEquals("node-legacy", read.masterNodeId());
            assertEquals("books", read.indices().keySet().iterator().next());

            // First update writes the split layout and overwrites the legacy value with the head.
            repository.update(current -> {
                Map<String, IndexSettings> indices = new LinkedHashMap<>(current.indices());
                indices.put("magazines", index("magazines", Map.of()));
                List<ShardRouting> routing = new java.util.ArrayList<>(current.routingTable());
                routing.add(routing("magazines", 0, "node-1", 1, 1));
                return new ClusterState(
                        current.clusterName(), current.version(), current.masterNodeId(), current.nodes(),
                        indices, routing, current.lifecyclePolicies(), Instant.now());
            });

            JsonNode head = JsonUtil.readValue(rawState(client, namespace), JsonNode.class);
            assertFalse(head.has("indices"));
            assertEquals(8, head.get("version").asLong());
            assertEquals(2, prefixCount(client, namespace, "indices/"));

            ClusterState migrated = repository.current();
            assertEquals(8, migrated.version());
            assertEquals(Set.of("books", "magazines"), migrated.indices().keySet());
        } finally {
            client.close();
        }
    }

    private EtcdClusterStateRepository repository(Client client, String namespace) {
        return new EtcdClusterStateRepository(
                EtcdClusterStateRepository.Options.builder()
                        .namespace(namespace)
                        .clusterName("test-cluster")
                        .build(),
                client
        );
    }

    private IndexSettings index(String name, Map<String, FieldMapping> mappings) {
        return new IndexSettings(name, 1, null, Instant.now(), mappings);
    }

    private ShardRouting routing(String indexName, int shard, String nodeId, long ownerTerm, long epoch) {
        return new ShardRouting(new ShardId(indexName, shard), ShardState.STARTED, nodeId, ownerTerm, epoch);
    }

    private byte[] rawState(Client client, String namespace) {
        KeyValue kv = client.getKVClient()
                .get(ByteSequence.from((namespace + "/state").getBytes(StandardCharsets.UTF_8)))
                .join().getKvs().getFirst();
        return kv.getValue().getBytes();
    }

    private int prefixCount(Client client, String namespace, String prefix) {
        return client.getKVClient()
                .get(ByteSequence.from((namespace + "/" + prefix).getBytes(StandardCharsets.UTF_8)),
                        GetOption.builder().isPrefix(true).build())
                .join().getKvs().size();
    }
}
