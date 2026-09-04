package com.github.wxk6b1203.cluster;

import com.github.wxk6b1203.search.SearchPlanner;
import com.github.wxk6b1203.search.SearchRequest;
import com.github.wxk6b1203.util.JsonUtil;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ClusterIndexServiceTest {
    @Test
    public void testIndexSettingsDeserializesStateWrittenBeforeDeletePendingField() {
        ClusterState state = JsonUtil.readValue("""
                {
                  "clusterName": "test",
                  "version": 1,
                  "masterNodeId": "node-1",
                  "nodes": {},
                  "indices": {
                    "books": {
                      "name": "books",
                      "numberOfShards": 1,
                      "lifecyclePolicy": null,
                      "createdAt": "2026-05-23T00:00:00Z",
                      "mappings": {}
                    }
                  },
                  "routingTable": [],
                  "lifecyclePolicies": {},
                  "updatedAt": "2026-05-23T00:00:00Z"
                }
                """, ClusterState.class);

        assertEquals(false, state.indices().get("books").deletePending());
        assertEquals(null, state.indices().get("books").deleteStartedAt());
    }

    @Test
    public void rejectsInvalidIndexNames() throws Exception {
        InMemoryClusterStateRepository repository = new InMemoryClusterStateRepository(
                "test", new ClusterNode("node-1", "node-1", "127.0.0.1", 9200,
                        Set.of(NodeRole.MASTER, NodeRole.DATA), Instant.now()));
        DefaultClusterIndexService service = new DefaultClusterIndexService(repository);

        // Uppercase, whitespace, separators, shell/URL hazards, dot-only names, overlong.
        List<String> invalid = List.of(
                "Books", "bad name", "-lead", "_lead", "+lead", ".lead",
                "a/b", "a\\b", "a?b", "a*b", "a:b", "..", ".", "", "   ",
                "a".repeat(201));
        for (String name : invalid) {
            assertThrows(IllegalArgumentException.class,
                    () -> service.createIndex(new IndexSettings(name, 1, null, Instant.now())),
                    "index name should be rejected: [" + name + "]");
        }

        // Valid names still create cleanly.
        for (String name : List.of("books", "log-2024.01_01", "a1", "x")) {
            service.createIndex(new IndexSettings(name, 1, null, Instant.now()));
        }
    }

    @Test
    public void testCreateIndexAllocatesShardOwnersToDataNode() throws Exception {
        ClusterNode node = new ClusterNode(
                "node-1",
                "node-1",
                "127.0.0.1",
                9200,
                Set.of(NodeRole.MASTER, NodeRole.DATA),
                Instant.now()
        );
        InMemoryClusterStateRepository repository = new InMemoryClusterStateRepository("test", node);
        DefaultClusterIndexService service = new DefaultClusterIndexService(repository);

        ClusterState state = service.createIndex(new IndexSettings("books", 2, null, Instant.now()));

        assertEquals(2, state.routingTable().size());
        assertEquals(2, shardsOn(state, "node-1"));
    }

    @Test
    public void testAllocatorDistributesShardOwnersAndReassignsAfterNodeLoss() {
        ClusterNode node1 = dataNode("node-1");
        ClusterNode node2 = dataNode("node-2");
        ClusterState state = new ClusterState(
                "test",
                1,
                "node-1",
                Map.of("node-1", node1, "node-2", node2),
                Map.of("books", new IndexSettings("books", 4, null, Instant.now())),
                List.of(
                        shard("books", 0),
                        shard("books", 1),
                        shard("books", 2),
                        shard("books", 3)
                ),
                Map.of(),
                Instant.now()
        );
        BalancedShardAllocator allocator = new BalancedShardAllocator();

        ClusterState allocated = allocator.rebalance(state);
        assertEquals(2, shardsOn(allocated, "node-1"));
        assertEquals(2, shardsOn(allocated, "node-2"));

        ClusterState node2Lost = new ClusterState(
                allocated.clusterName(),
                allocated.version(),
                allocated.masterNodeId(),
                Map.of("node-1", node1),
                allocated.indices(),
                allocated.routingTable(),
                allocated.lifecyclePolicies(),
                allocated.updatedAt()
        );
        ClusterState reallocated = allocator.rebalance(node2Lost);
        assertEquals(4, shardsOn(reallocated, "node-1"));
    }

    @Test
    public void testWriteRouterReturnsShardOwnerNode() throws Exception {
        ClusterNode node = dataNode("node-1");
        InMemoryClusterStateRepository repository = new InMemoryClusterStateRepository("test", node);
        DefaultClusterIndexService service = new DefaultClusterIndexService(repository);
        ClusterState state = service.createIndex(new IndexSettings("books", 2, null, Instant.now()));
        WriteRouter router = new WriteRouter(new HashShardRouter());

        WriteRoute route = router.route("books", "doc-1", state);

        assertNotEquals(null, route.shardId());
        assertEquals("node-1", route.nodeId());
        assertEquals("127.0.0.1", route.host());
        assertEquals(9200, route.httpPort());
        ShardRouting routing = state.routingTable().stream()
                .filter(shard -> shard.shardId().equals(route.shardId()))
                .findFirst()
                .orElseThrow();
        assertEquals(routing.ownerTerm(), route.ownerTerm());
        assertEquals(routing.allocationEpoch(), route.allocationEpoch());
    }

    @Test
    public void testPutMappingAppendsAndRejectsConflictingDefinitions() throws Exception {
        ClusterNode node = dataNode("node-1");
        InMemoryClusterStateRepository repository = new InMemoryClusterStateRepository("test", node);
        DefaultClusterIndexService service = new DefaultClusterIndexService(repository);
        service.createIndex(new IndexSettings("books", 1, null, Instant.now()));

        FieldMapping title = new FieldMapping("text", null, null, true, true);
        ClusterState mapped = service.putMapping("books", Map.of("title", title));

        assertEquals(title, mapped.indices().get("books").mappings().get("title"));
        assertThrows(
                IllegalArgumentException.class,
                () -> service.putMapping("books", Map.of("title", new FieldMapping("keyword", null, null, true, true)))
        );
    }

    @Test
    public void testMarkIndexDeletingKeepsTombstoneUntilFinalRemoval() throws Exception {
        ClusterNode node = dataNode("node-1");
        InMemoryClusterStateRepository repository = new InMemoryClusterStateRepository("test", node);
        DefaultClusterIndexService service = new DefaultClusterIndexService(repository);
        service.createIndex(new IndexSettings("books", 1, null, Instant.now()));

        ClusterState deleting = service.markIndexDeleting("books");

        assertEquals(true, deleting.indices().get("books").deletePending());
        assertThrows(
                IllegalArgumentException.class,
                () -> service.putMapping("books", Map.of("title", new FieldMapping("keyword", null, null, true, true)))
        );

        ClusterState removed = service.deleteIndex("books");

        assertEquals(false, removed.indices().containsKey("books"));
    }

    @Test
    public void testSearchRouterSelectsLiveShardOwner() {
        ClusterState state = new ClusterState(
                "test",
                1,
                "node-1",
                Map.of("node-1", dataNode("node-1")),
                Map.of("books", new IndexSettings("books", 1, null, Instant.now())),
                List.of(new ShardRouting(new ShardId("books", 0), ShardState.STARTED, "node-1", 1, 1)),
                Map.of(),
                Instant.now()
        );

        List<ShardRouting> selected = new HashShardRouter().searchShards("books", state);

        assertEquals(1, selected.size());
        assertEquals("node-1", selected.getFirst().nodeId());
    }

    @Test
    public void testSearchPlannerFallsBackToLiveDataNodeWhenShardOwnerIsGone() {
        ClusterState state = new ClusterState(
                "test",
                1,
                "node-1",
                Map.of("node-2", dataNode("node-2")),
                Map.of("books", new IndexSettings("books", 1, null, Instant.now())),
                List.of(new ShardRouting(new ShardId("books", 0), ShardState.STARTED, "node-1", 1, 1)),
                Map.of(),
                Instant.now()
        );

        var plan = new SearchPlanner(new HashShardRouter()).plan(
                new SearchRequest("books", Map.of(), List.of(), null, null, 0, 10),
                state
        );

        assertEquals(1, plan.targets().size());
        assertEquals("node-2", plan.targets().getFirst().nodeId());
    }

    @Test
    public void testSearchPlannerIncludesNodeAddressAndClusterStateVersion() {
        ClusterState state = new ClusterState(
                "test",
                7,
                "node-1",
                Map.of("node-1", dataNode("node-1")),
                Map.of("books", new IndexSettings("books", 1, null, Instant.now())),
                List.of(new ShardRouting(new ShardId("books", 0), ShardState.STARTED, "node-1", 1, 3)),
                Map.of(),
                Instant.now()
        );

        var plan = new SearchPlanner(new HashShardRouter()).plan(
                new SearchRequest("books", Map.of(), List.of(), null, null, 0, 10),
                state
        );

        assertEquals(7, plan.clusterStateVersion());
        assertEquals(1, plan.targets().size());
        assertEquals("node-1", plan.targets().getFirst().nodeId());
        assertEquals("127.0.0.1", plan.targets().getFirst().host());
        assertEquals(9200, plan.targets().getFirst().httpPort());
    }

    @Test
    public void testSearchPlannerHonorsRoutingKey() {
        ClusterState state = new ClusterState(
                "test",
                7,
                "node-1",
                Map.of("node-1", dataNode("node-1")),
                Map.of("books", new IndexSettings("books", 4, null, Instant.now())),
                List.of(
                        new ShardRouting(new ShardId("books", 0), ShardState.STARTED, "node-1", 1, 1),
                        new ShardRouting(new ShardId("books", 1), ShardState.STARTED, "node-1", 1, 1),
                        new ShardRouting(new ShardId("books", 2), ShardState.STARTED, "node-1", 1, 1),
                        new ShardRouting(new ShardId("books", 3), ShardState.STARTED, "node-1", 1, 1)
                ),
                Map.of(),
                Instant.now()
        );
        HashShardRouter router = new HashShardRouter();
        ShardId routedShard = router.route("books", "user-1", state);

        var plan = new SearchPlanner(router).plan(
                new SearchRequest("books", Map.of(), List.of(), null, "user-1", 0, 10),
                state
        );

        assertEquals(1, plan.targets().size());
        assertEquals(routedShard, plan.targets().getFirst().shardId());
    }

    @Test
    public void testRoutedSearchDoesNotRequireUnrelatedShardsToBeLive() {
        ClusterState state = new ClusterState(
                "test",
                7,
                "node-1",
                Map.of("node-1", dataNode("node-1")),
                Map.of("books", new IndexSettings("books", 2, null, Instant.now())),
                List.of(
                        new ShardRouting(new ShardId("books", 0), ShardState.STARTED, "node-1", 1, 1),
                        new ShardRouting(new ShardId("books", 1), ShardState.STARTED, "node-2", 1, 1)
                ),
                Map.of(),
                Instant.now()
        );
        HashShardRouter router = new HashShardRouter();
        String routing = routingKeyForShard(router, state, 0);

        var plan = new SearchPlanner(router).plan(
                new SearchRequest("books", Map.of(), List.of(), null, routing, 0, 10),
                state
        );

        assertEquals(1, plan.targets().size());
        assertEquals(new ShardId("books", 0), plan.targets().getFirst().shardId());
    }

    @Test
    public void testAllocatorIsStableOnRepeatedRebalance() {
        ClusterNode node1 = dataNode("node-1");
        ClusterNode node2 = dataNode("node-2");
        ClusterState state = new ClusterState(
                "test",
                1,
                "node-1",
                Map.of("node-1", node1, "node-2", node2),
                Map.of("books", new IndexSettings("books", 4, null, Instant.now())),
                List.of(shard("books", 0), shard("books", 1), shard("books", 2), shard("books", 3)),
                Map.of(),
                Instant.now()
        );
        BalancedShardAllocator allocator = new BalancedShardAllocator();

        ClusterState once = allocator.rebalance(state);
        ClusterState twice = allocator.rebalance(once);

        assertEquals(once.routingTable(), twice.routingTable());
    }

    @Test
    public void testNoOpUpdateDoesNotBumpClusterStateVersion() throws Exception {
        InMemoryClusterStateRepository repository = new InMemoryClusterStateRepository("test", dataNode("node-1"));
        ClusterState before = repository.current();

        ClusterState after = repository.update(state -> state);

        assertEquals(before.version(), after.version());
    }

    @Test
    public void testMasterOnlyRepositoryRejectsNonMasterUpdates() {
        InMemoryClusterStateRepository repository = new InMemoryClusterStateRepository("test", dataNode("node-1"));
        MasterOnlyClusterStateRepository guarded = new MasterOnlyClusterStateRepository(repository, nonMasterCoordinator());

        assertThrows(RuntimeException.class, () -> guarded.update(state -> state));
    }

    private ClusterNode dataNode(String id) {
        return new ClusterNode(id, id, "127.0.0.1", 9200, Set.of(NodeRole.DATA), Instant.now());
    }

    private ShardRouting shard(String indexName, int shard) {
        return new ShardRouting(new ShardId(indexName, shard), ShardState.UNASSIGNED, null, 1, 1);
    }

    private String routingKeyForShard(HashShardRouter router, ClusterState state, int shardNumber) {
        for (int i = 0; i < 10_000; i++) {
            String routing = "routing-" + i;
            if (router.route("books", routing, state).shardNumber() == shardNumber) {
                return routing;
            }
        }
        throw new AssertionError("failed to find routing key for shard " + shardNumber);
    }

    private ClusterCoordinator nonMasterCoordinator() {
        return new ClusterCoordinator() {
            @Override
            public void start() {
            }

            @Override
            public boolean isMaster() {
                return false;
            }

            @Override
            public void close() {
            }
        };
    }

    private long shardsOn(ClusterState state, String nodeId) {
        return state.routingTable().stream()
                .filter(routing -> routing.state() == ShardState.STARTED)
                .filter(routing -> nodeId.equals(routing.nodeId()))
                .count();
    }
}
