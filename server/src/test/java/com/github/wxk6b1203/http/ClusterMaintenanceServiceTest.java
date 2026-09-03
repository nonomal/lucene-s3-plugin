package com.github.wxk6b1203.http;

import com.github.wxk6b1203.cluster.*;
import com.github.wxk6b1203.index.IndexDocumentRequest;
import com.github.wxk6b1203.index.IndexDocumentResponse;
import com.github.wxk6b1203.index.LocalShardIndexService;
import com.github.wxk6b1203.metadata.provider.mem.MemMockProvider;
import com.github.wxk6b1203.search.*;
import com.github.wxk6b1203.store.object.LocalFileRemoteObjectStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClusterMaintenanceServiceTest {
    @TempDir
    Path tempDir;

    @Test
    @Timeout(10)
    void indexDeleteWaitsForShardScopedMaintenanceToFinish() throws Exception {
        ClusterNode node = new ClusterNode(
                "node-1",
                "node-1",
                "127.0.0.1",
                9200,
                Set.of(NodeRole.MASTER, NodeRole.DATA),
                Instant.now()
        );
        ClusterState state = new ClusterState(
                "test",
                1,
                node.id(),
                Map.of(node.id(), node),
                Map.of("books", new IndexSettings("books", 1, "delete-now", Instant.now().minusSeconds(1))),
                List.of(new ShardRouting(new ShardId("books", 0), ShardState.STARTED, node.id(), 1, 1)),
                Map.of("delete-now", new IndexLifecyclePolicy("delete-now", Map.of(LifecyclePhase.DELETE, 0L))),
                Instant.now()
        );
        BlockingShardService localShardService = new BlockingShardService();
        AtomicInteger deletes = new AtomicInteger();
        ClusterMaintenanceService service = newService(node, state, localShardService, (index, shards) -> deletes.incrementAndGet());
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> retry = executor.submit(() -> {
                try {
                    service.retryPendingUploadsNow(null);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
            assertTrue(localShardService.retryStarted.await(5, TimeUnit.SECONDS));

            service.run(ClusterMaintenanceService.MaintenanceTask.LIFECYCLE);
            assertEquals(0, deletes.get());

            localShardService.releaseRetry.countDown();
            executor.shutdown();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
            retry.get(5, TimeUnit.SECONDS);

            service.run(ClusterMaintenanceService.MaintenanceTask.LIFECYCLE);
            assertEquals(1, deletes.get());
        } finally {
            localShardService.releaseRetry.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void writeMaintenanceTaskDelegatesToLocalShardService() {
        ClusterNode node = new ClusterNode(
                "node-1",
                "node-1",
                "127.0.0.1",
                9200,
                Set.of(NodeRole.MASTER, NodeRole.DATA),
                Instant.now()
        );
        ClusterState state = new ClusterState(
                "test",
                1,
                node.id(),
                Map.of(node.id(), node),
                Map.of("books", new IndexSettings("books", 1, null, Instant.now())),
                List.of(new ShardRouting(new ShardId("books", 0), ShardState.STARTED, node.id(), 1, 1)),
                Map.of(),
                Instant.now()
        );
        BlockingShardService localShardService = new BlockingShardService();
        ClusterMaintenanceService service = newService(node, state, localShardService, (index, shards) -> {
        });

        service.run(ClusterMaintenanceService.MaintenanceTask.WRITE_MAINTENANCE);

        assertEquals(1, localShardService.writeMaintenanceRuns.get());
    }

    @Test
    void writeMaintenanceSkipsPendingLocalWritesAfterOwnershipMoves() {
        ClusterNode node1 = new ClusterNode(
                "node-1",
                "node-1",
                "127.0.0.1",
                9200,
                Set.of(NodeRole.MASTER, NodeRole.DATA),
                Instant.now()
        );
        ClusterNode node2 = new ClusterNode(
                "node-2",
                "node-2",
                "127.0.0.1",
                9201,
                Set.of(NodeRole.DATA),
                Instant.now()
        );
        ShardId shardId = new ShardId("books", 0);
        ClusterState state = new ClusterState(
                "test",
                1,
                node1.id(),
                Map.of(node1.id(), node1, node2.id(), node2),
                Map.of("books", new IndexSettings("books", 1, null, Instant.now())),
                List.of(new ShardRouting(shardId, ShardState.STARTED, node2.id(), 1, 1)),
                Map.of(),
                Instant.now()
        );
        BlockingShardService localShardService = new BlockingShardService();
        localShardService.pendingWriteShardIds = List.of(shardId);
        ClusterMaintenanceService service = newService(node1, state, localShardService, (index, shards) -> {
        });

        service.run(ClusterMaintenanceService.MaintenanceTask.WRITE_MAINTENANCE);

        // The shard moved to node-2: WRITE_MAINTENANCE must not commit-and-publish the deposed
        // writer (divergent snapshot line); WRITER_RECONCILE drains it upload-only instead.
        assertEquals(Set.of(), Set.copyOf(localShardService.writeMaintenanceShardIds));
    }

    @Test
    void uploadRetrySkipsPendingLocalUploadsAfterOwnershipMoves() {
        ClusterNode node1 = new ClusterNode(
                "node-1",
                "node-1",
                "127.0.0.1",
                9200,
                Set.of(NodeRole.MASTER, NodeRole.DATA),
                Instant.now()
        );
        ClusterNode node2 = new ClusterNode(
                "node-2",
                "node-2",
                "127.0.0.1",
                9201,
                Set.of(NodeRole.DATA),
                Instant.now()
        );
        ShardId shardId = new ShardId("books", 0);
        ClusterState state = new ClusterState(
                "test",
                1,
                node1.id(),
                Map.of(node1.id(), node1, node2.id(), node2),
                Map.of("books", new IndexSettings("books", 1, null, Instant.now())),
                List.of(new ShardRouting(shardId, ShardState.STARTED, node2.id(), 1, 1)),
                Map.of(),
                Instant.now()
        );
        BlockingShardService localShardService = new BlockingShardService();
        localShardService.pendingUploadShardIds = List.of(shardId);
        localShardService.releaseRetry.countDown();
        ClusterMaintenanceService service = newService(node1, state, localShardService, (index, shards) -> {
        });

        service.run(ClusterMaintenanceService.MaintenanceTask.UPLOAD_RETRY);

        // Deposed owners must not publishLocalCommit(); WRITER_RECONCILE drains upload-only.
        assertEquals(Set.of(), Set.copyOf(localShardService.retryShardIds));
    }

    @Test
    void writerReconcileDelegatesToLocalShardServiceWithLocalNodeId() {
        ClusterNode node1 = new ClusterNode(
                "node-1",
                "node-1",
                "127.0.0.1",
                9200,
                Set.of(NodeRole.MASTER, NodeRole.DATA),
                Instant.now()
        );
        ClusterNode node2 = new ClusterNode(
                "node-2",
                "node-2",
                "127.0.0.1",
                9201,
                Set.of(NodeRole.DATA),
                Instant.now()
        );
        ShardId shardId = new ShardId("books", 0);
        ClusterState state = new ClusterState(
                "test",
                1,
                node1.id(),
                Map.of(node1.id(), node1, node2.id(), node2),
                Map.of("books", new IndexSettings("books", 1, null, Instant.now())),
                List.of(new ShardRouting(shardId, ShardState.STARTED, node2.id(), 1, 1)),
                Map.of(),
                Instant.now()
        );
        BlockingShardService localShardService = new BlockingShardService();
        ClusterMaintenanceService service = newService(node1, state, localShardService, (index, shards) -> {
        });

        service.run(ClusterMaintenanceService.MaintenanceTask.WRITER_RECONCILE);

        assertEquals(1, localShardService.reconcileCount.get());
        assertEquals(node1.id(), localShardService.reconciledOwnerNodeId);
    }

    @Test
    void writerReconcileSkippedWhenClusterStateUnavailable() {
        ClusterNode node1 = new ClusterNode(
                "node-1",
                "node-1",
                "127.0.0.1",
                9200,
                Set.of(NodeRole.MASTER, NodeRole.DATA),
                Instant.now()
        );
        ClusterState state = new ClusterState(
                "test",
                1,
                node1.id(),
                Map.of(node1.id(), node1),
                Map.of(),
                List.of(),
                Map.of(),
                Instant.now()
        );
        BlockingShardService localShardService = new BlockingShardService();
        ClusterStateRepository failingRepository = new ClusterStateRepository() {
            @Override
            public ClusterState current() throws IOException {
                throw new IOException("etcd unavailable");
            }

            @Override
            public ClusterState update(ClusterStateUpdate update) throws IOException {
                throw new IOException("etcd unavailable");
            }
        };
        ClusterMaintenanceService service = new ClusterMaintenanceService(
                failingRepository,
                new NoopClusterCoordinator(),
                node1,
                localShardService,
                new MemMockProvider(),
                new LocalFileRemoteObjectStore(tempDir.resolve("remote-objects")),
                1,
                (index, shards) -> {
                },
                null,
                Duration.ofSeconds(1)
        );

        service.run(ClusterMaintenanceService.MaintenanceTask.WRITER_RECONCILE);

        // Must never act on missing evidence.
        assertEquals(0, localShardService.reconcileCount.get());
    }

    @Test
    void uploadRetryContinuesAfterOneShardFails() {
        ClusterNode node = new ClusterNode(
                "node-1",
                "node-1",
                "127.0.0.1",
                9200,
                Set.of(NodeRole.MASTER, NodeRole.DATA),
                Instant.now()
        );
        ShardId shard0 = new ShardId("books", 0);
        ShardId shard1 = new ShardId("books", 1);
        ClusterState state = new ClusterState(
                "test",
                1,
                node.id(),
                Map.of(node.id(), node),
                Map.of("books", new IndexSettings("books", 2, null, Instant.now())),
                List.of(
                        new ShardRouting(shard0, ShardState.STARTED, node.id(), 1, 1),
                        new ShardRouting(shard1, ShardState.STARTED, node.id(), 1, 1)
                ),
                Map.of(),
                Instant.now()
        );
        BlockingShardService localShardService = new BlockingShardService();
        localShardService.releaseRetry.countDown();
        localShardService.failedRetryShardIds = Set.of(shard0);
        ClusterMaintenanceService service = newService(node, state, localShardService, (index, shards) -> {
        });

        assertThrows(IOException.class, () -> service.retryPendingUploadsNow(null));

        assertEquals(List.of(shard0, shard1), localShardService.retryAttempts);
    }

    @Test
    @Timeout(10)
    void indexDeleteWaitsForWriteMaintenanceToFinish() throws Exception {
        ClusterNode node = new ClusterNode(
                "node-1",
                "node-1",
                "127.0.0.1",
                9200,
                Set.of(NodeRole.MASTER, NodeRole.DATA),
                Instant.now()
        );
        ClusterState state = new ClusterState(
                "test",
                1,
                node.id(),
                Map.of(node.id(), node),
                Map.of("books", new IndexSettings("books", 1, "delete-now", Instant.now().minusSeconds(1))),
                List.of(new ShardRouting(new ShardId("books", 0), ShardState.STARTED, node.id(), 1, 1)),
                Map.of("delete-now", new IndexLifecyclePolicy("delete-now", Map.of(LifecyclePhase.DELETE, 0L))),
                Instant.now()
        );
        BlockingShardService localShardService = new BlockingShardService();
        localShardService.blockWriteMaintenance = true;
        AtomicInteger deletes = new AtomicInteger();
        ClusterMaintenanceService service = newService(node, state, localShardService, (index, shards) -> deletes.incrementAndGet());
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> writeMaintenance = executor.submit(() -> service.run(ClusterMaintenanceService.MaintenanceTask.WRITE_MAINTENANCE));
            assertTrue(localShardService.writeMaintenanceStarted.await(5, TimeUnit.SECONDS));

            service.run(ClusterMaintenanceService.MaintenanceTask.LIFECYCLE);
            assertEquals(0, deletes.get());

            localShardService.releaseWriteMaintenance.countDown();
            executor.shutdown();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
            writeMaintenance.get(5, TimeUnit.SECONDS);

            service.run(ClusterMaintenanceService.MaintenanceTask.LIFECYCLE);
            assertEquals(1, deletes.get());
        } finally {
            localShardService.releaseWriteMaintenance.countDown();
            executor.shutdownNow();
        }
    }

    private ClusterMaintenanceService newService(
            ClusterNode node,
            ClusterState state,
            BlockingShardService localShardService,
            ClusterMaintenanceService.IndexDataDeleter deleter
    ) {
        return new ClusterMaintenanceService(
                new FixedClusterStateRepository(state),
                new AlwaysMasterCoordinator(),
                node,
                localShardService,
                new MemMockProvider(),
                new LocalFileRemoteObjectStore(tempDir.resolve("remote")),
                2,
                deleter,
                null,
                Duration.ofSeconds(1)
        );
    }

    private record FixedClusterStateRepository(ClusterState state) implements ClusterStateRepository {
        @Override
        public ClusterState current() {
            return state;
        }

        @Override
        public ClusterState update(ClusterStateUpdate update) {
            return state;
        }
    }

    private static class AlwaysMasterCoordinator implements ClusterCoordinator {
        @Override
        public void start() {
        }

        @Override
        public boolean isMaster() {
            return true;
        }

        @Override
        public void close() {
        }
    }

    private static class BlockingShardService implements LocalShardIndexService {
        private final CountDownLatch retryStarted = new CountDownLatch(1);
        private final CountDownLatch releaseRetry = new CountDownLatch(1);
        private final CountDownLatch writeMaintenanceStarted = new CountDownLatch(1);
        private final CountDownLatch releaseWriteMaintenance = new CountDownLatch(1);
        private final AtomicInteger writeMaintenanceRuns = new AtomicInteger();
        private volatile Collection<ShardId> pendingWriteShardIds = List.of();
        private volatile Collection<ShardId> pendingUploadShardIds = List.of();
        private volatile Collection<ShardId> writeMaintenanceShardIds = List.of();
        private volatile Collection<ShardId> retryShardIds = List.of();
        private volatile Collection<ShardId> failedRetryShardIds = List.of();
        private final List<ShardId> retryAttempts = new CopyOnWriteArrayList<>();
        private volatile boolean blockWriteMaintenance;
        private final AtomicInteger reconcileCount = new AtomicInteger();
        private volatile String reconciledOwnerNodeId;

        @Override
        public void reconcileWithClusterState(ClusterState state, String expectedOwnerNodeId) {
            reconciledOwnerNodeId = expectedOwnerNodeId;
            reconcileCount.incrementAndGet();
        }

        @Override
        public IndexDocumentResponse index(IndexDocumentRequest request) {
            throw new UnsupportedOperationException();
        }

        @Override
        public IndexDocumentResponse delete(IndexDocumentRequest request) {
            throw new UnsupportedOperationException();
        }

        @Override
        public SearchResponse search(ShardId shardId, SearchRequest request) {
            throw new UnsupportedOperationException();
        }

        @Override
        public PointInTimeResponse openPointInTime(
                ShardId shardId,
                String indexName,
                Duration keepAlive,
                String readPreference
        ) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean closePointInTime(String pitId) {
            return false;
        }

        @Override
        public ByQueryResponse updateByQuery(ShardId shardId, ByQueryRequest request) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ByQueryResponse deleteByQuery(ShardId shardId, ByQueryRequest request) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void deleteIndex(String indexName, int numberOfShards) {
        }

        @Override
        public void retryPendingUploads(Collection<ShardId> shardIds) throws IOException {
            retryShardIds = List.copyOf(shardIds);
            retryAttempts.addAll(shardIds);
            retryStarted.countDown();
            try {
                releaseRetry.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(e);
            }
            for (ShardId shardId : shardIds) {
                if (failedRetryShardIds.contains(shardId)) {
                    throw new IOException("retry failed: " + shardId.routeKey());
                }
            }
        }

        @Override
        public Collection<ShardId> shardIdsWithPendingWrites() {
            return pendingWriteShardIds;
        }

        @Override
        public Collection<ShardId> shardIdsWithPendingUploads() {
            return pendingUploadShardIds;
        }

        @Override
        public void runWriteMaintenance(Collection<ShardId> shardIds) throws IOException {
            writeMaintenanceShardIds = List.copyOf(shardIds);
            writeMaintenanceRuns.incrementAndGet();
            writeMaintenanceStarted.countDown();
            if (!blockWriteMaintenance) {
                return;
            }
            try {
                releaseWriteMaintenance.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(e);
            }
        }

        @Override
        public void close() {
        }
    }
}
