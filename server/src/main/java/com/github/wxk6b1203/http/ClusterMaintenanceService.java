package com.github.wxk6b1203.http;

import com.github.wxk6b1203.cluster.*;
import com.github.wxk6b1203.index.LocalShardIndexService;
import com.github.wxk6b1203.metadata.common.IndexCommitSnapshot;
import com.github.wxk6b1203.metadata.common.IndexCommitSnapshotHeader;
import com.github.wxk6b1203.metadata.common.IndexFileMetadata;
import com.github.wxk6b1203.metadata.common.IndexFileStatus;
import com.github.wxk6b1203.metadata.provider.ManifestMetadataManager;
import com.github.wxk6b1203.store.manifest.ManifestManager;
import com.github.wxk6b1203.store.manifest.ManifestOptions;
import com.github.wxk6b1203.store.object.RemoteObjectStore;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
final class ClusterMaintenanceService {
    private static final long STUCK_PENDING_UPLOAD_MILLIS = Duration.ofMinutes(1).toMillis();
    private static final List<IndexFileStatus> ALL_UPLOAD_STATUSES = List.of(
            IndexFileStatus.DIRTY,
            IndexFileStatus.UPLOADING,
            IndexFileStatus.CLEAN,
            IndexFileStatus.PINNED
    );
    private static final List<IndexFileStatus> PENDING_UPLOAD_STATUSES = List.of(
            IndexFileStatus.DIRTY,
            IndexFileStatus.UPLOADING
    );

    private final ClusterStateRepository clusterStateRepository;
    private final ClusterCoordinator clusterCoordinator;
    private final ClusterNode localNode;
    private final LocalShardIndexService localShardIndexService;
    private final ManifestMetadataManager manifestMetadataManager;
    private final RemoteObjectStore remoteObjectStore;
    private final IndexDataDeleter indexDataDeleter;
    private final int snapshotRetainLatest;
    private final LocalCacheManager cacheManager;
    private final Duration cacheCleanupInterval;
    private final Set<String> lifecycleDeletesInProgress = ConcurrentHashMap.newKeySet();
    private final Set<String> lifecycleForceMergesInProgress = ConcurrentHashMap.newKeySet();
    private final Set<String> lifecycleForceMergesCompleted = ConcurrentHashMap.newKeySet();
    private final Object scopeLock = new Object();
    private final Set<String> indexScopes = ConcurrentHashMap.newKeySet();
    private final Set<String> shardScopes = ConcurrentHashMap.newKeySet();
    private volatile Instant lastCacheCleanup = Instant.EPOCH;
    private volatile LocalCacheManager.CleanupStats lastCacheCleanupStats = new LocalCacheManager.CleanupStats(0, 0, 0, 0);

    enum MaintenanceTask {
        WRITER_RECONCILE,
        WRITE_MAINTENANCE,
        UPLOAD_RETRY,
        SNAPSHOT_GC,
        LIFECYCLE,
        LOCAL_CACHE_CLEANUP
    }

    ClusterMaintenanceService(
            ClusterStateRepository clusterStateRepository,
            ClusterCoordinator clusterCoordinator,
            ClusterNode localNode,
            LocalShardIndexService localShardIndexService,
            ManifestMetadataManager manifestMetadataManager,
            RemoteObjectStore remoteObjectStore,
            int snapshotRetainLatest,
            IndexDataDeleter indexDataDeleter,
            LocalCacheManager cacheManager,
            Duration cacheCleanupInterval
    ) {
        this.clusterStateRepository = clusterStateRepository;
        this.clusterCoordinator = clusterCoordinator;
        this.localNode = localNode;
        this.localShardIndexService = localShardIndexService;
        this.manifestMetadataManager = manifestMetadataManager;
        this.remoteObjectStore = remoteObjectStore;
        this.snapshotRetainLatest = snapshotRetainLatest;
        this.indexDataDeleter = indexDataDeleter;
        this.cacheManager = cacheManager;
        this.cacheCleanupInterval = cacheCleanupInterval == null || cacheCleanupInterval.isNegative()
                ? Duration.ofMinutes(1)
                : cacheCleanupInterval;
        this.lastCacheCleanupStats = new LocalCacheManager.CleanupStats(
                0,
                cacheManager == null ? 0 : cacheManager.maxBytes(),
                0,
                0
        );
    }

    void tick() {
        for (MaintenanceTask task : MaintenanceTask.values()) {
            run(task);
        }
    }

    void run(MaintenanceTask task) {
        switch (task) {
            case WRITER_RECONCILE -> reconcileWriters();
            case WRITE_MAINTENANCE -> runWriteMaintenance();
            case UPLOAD_RETRY -> retryOwnedShardUploads();
            case SNAPSHOT_GC -> runSnapshotGarbageCollection();
            case LIFECYCLE -> runLifecyclePolicies();
            case LOCAL_CACHE_CLEANUP -> runLocalCacheCleanup();
        }
    }

    Map<String, Object> uploadStatus(String indexFilter) throws IOException {
        return uploadStatus(clusterStateRepository.current(), indexFilter);
    }

    Map<String, Object> retryPendingUploadsNow(String indexFilter) throws IOException {
        ClusterState state = clusterStateRepository.current();
        retryOwnedShardUploads(state, indexFilter);
        return uploadStatus(clusterStateRepository.current(), indexFilter);
    }

    Map<String, Object> cacheStatus() {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("enabled", cacheManager != null && cacheManager.enabled());
        result.put("last_cleanup_time", lastCacheCleanup.equals(Instant.EPOCH) ? null : lastCacheCleanup.toString());
        result.put("total_bytes", lastCacheCleanupStats.totalBytes());
        result.put("max_bytes", lastCacheCleanupStats.maxBytes());
        result.put("last_deleted_files", lastCacheCleanupStats.deletedFiles());
        result.put("last_deleted_bytes", lastCacheCleanupStats.deletedBytes());
        return result;
    }

    void deleteIndexDataWithScope(String indexName, int numberOfShards) throws IOException {
        if (!tryAcquireIndexScope(indexName)) {
            throw new IllegalStateException("conflict: index maintenance is running: " + indexName);
        }
        try {
            indexDataDeleter.deleteIndexAndData(indexName, numberOfShards);
        } finally {
            releaseIndexScope(indexName);
        }
    }

    void retryOwnedShardUploads() {
        try {
            retryOwnedShardUploads(clusterStateRepository.current(), null);
        } catch (IOException e) {
            log.warn("failed to retry pending shard uploads", e);
        }
    }

    void runWriteMaintenance() {
        ClusterState state;
        try {
            state = clusterStateRepository.current();
        } catch (IOException e) {
            log.warn("failed to load cluster state for write maintenance", e);
            return;
        }
        Set<ShardId> shardIds = state.routingTable().stream()
                .filter(routing -> routing.state() == ShardState.STARTED)
                .filter(routing -> localNode.id().equals(routing.nodeId()))
                .map(ShardRouting::shardId)
                .filter(shardId -> maintenanceEligible(state, shardId))
                .collect(java.util.stream.Collectors.toCollection(LinkedHashSet::new));
        try {
            for (ShardId shardId : localShardIndexService.shardIdsWithPendingWrites()) {
                // Deposed writers are drained (upload-only) by WRITER_RECONCILE; committing and
                // publishing here would interleave a divergent segment line into the new owner's
                // snapshot sequence.
                if (maintenanceEligible(state, shardId) && isOwnedBy(state, shardId)) {
                    shardIds.add(shardId);
                }
            }
        } catch (IOException e) {
            log.warn("failed to list local shards with pending writes", e);
        }
        List<ShardId> acquired = new ArrayList<>(shardIds.size());
        for (ShardId shardId : shardIds) {
            if (tryAcquireShardScope(shardId)) {
                acquired.add(shardId);
            }
        }
        if (acquired.isEmpty()) {
            return;
        }
        try {
            localShardIndexService.runWriteMaintenance(acquired);
        } catch (IOException e) {
            log.warn("failed to run local write maintenance", e);
        } finally {
            acquired.forEach(this::releaseShardScope);
        }
    }

    private boolean maintenanceEligible(ClusterState state, ShardId shardId) {
        var settings = state.indices().get(shardId.indexName());
        return settings != null && !settings.deletePending();
    }

    private boolean isOwnedBy(ClusterState state, ShardId shardId) {
        if (!maintenanceEligible(state, shardId)) {
            return false;
        }
        return state.routingTable().stream()
                .filter(routing -> routing.shardId().equals(shardId))
                .findFirst()
                .map(routing -> routing.state() == ShardState.STARTED
                        && localNode.id().equals(routing.nodeId()))
                .orElse(false);
    }

    private void reconcileWriters() {
        ClusterState state;
        try {
            state = clusterStateRepository.current();
        } catch (IOException e) {
            // Never act on missing evidence: a stale read could quarantine a shard this node owns.
            log.warn("failed to load cluster state for writer reconcile", e);
            return;
        }
        try {
            localShardIndexService.reconcileWithClusterState(state, localNode.id());
        } catch (IOException e) {
            log.warn("failed to reconcile local shard writers", e);
        }
    }

    void runSnapshotGarbageCollection() {
        if (!clusterCoordinator.isMaster()) {
            return;
        }
        ClusterState state;
        try {
            state = clusterStateRepository.current();
        } catch (IOException e) {
            log.warn("failed to load cluster state for snapshot garbage collection", e);
            return;
        }
        try (ManifestManager manifestManager = new ManifestManager(
                new ManifestOptions(""),
                remoteObjectStore,
                manifestMetadataManager
        )) {
            state.indices().forEach((indexName, settings) -> {
                if (settings.deletePending()) {
                    return;
                }
                for (int shard = 0; shard < settings.numberOfShards(); shard++) {
                    String physicalIndexName = physicalIndexName(indexName, shard);
                    ShardId shardId = new ShardId(indexName, shard);
                    if (!tryAcquireShardScope(shardId)) {
                        continue;
                    }
                    try {
                        manifestManager.garbageCollectSnapshots(physicalIndexName, snapshotRetainLatest);
                    } catch (Exception e) {
                        log.warn("failed to garbage collect snapshots for {}", physicalIndexName, e);
                    } finally {
                        releaseShardScope(shardId);
                    }
                }
            });
        }
    }

    private void retryOwnedShardUploads(ClusterState state, String indexFilter) throws IOException {
        Set<ShardId> shardIds = state.routingTable().stream()
                .filter(routing -> routing.state() == ShardState.STARTED)
                .filter(routing -> localNode.id().equals(routing.nodeId()))
                .map(ShardRouting::shardId)
                .filter(shardId -> indexFilter == null || indexFilter.equals(shardId.indexName()))
                .filter(shardId -> maintenanceEligible(state, shardId))
                .collect(java.util.stream.Collectors.toCollection(LinkedHashSet::new));
        for (ShardId shardId : localShardIndexService.shardIdsWithPendingUploads()) {
            if ((indexFilter == null || indexFilter.equals(shardId.indexName()))
                    && maintenanceEligible(state, shardId)
                    && isOwnedBy(state, shardId)) {
                shardIds.add(shardId);
            }
        }
        IOException failure = null;
        for (ShardId shardId : shardIds) {
            if (!tryAcquireShardScope(shardId)) {
                continue;
            }
            try {
                localShardIndexService.retryPendingUploads(List.of(shardId));
            } catch (IOException e) {
                log.warn("failed to retry pending uploads for {}", shardId.routeKey(), e);
                failure = addFailure(failure, e);
            } finally {
                releaseShardScope(shardId);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private static IOException addFailure(IOException failure, IOException e) {
        if (failure == null) {
            return e;
        }
        failure.addSuppressed(e);
        return failure;
    }

    private Map<String, Object> uploadStatus(ClusterState state, String indexFilter) {
        if (indexFilter != null && !state.indices().containsKey(indexFilter)) {
            throw new IllegalArgumentException("index not found: " + indexFilter);
        }
        Instant now = Instant.now();
        List<Map<String, Object>> indices = new ArrayList<>();
        long pendingShards = 0;
        long stuckShards = 0;
        long pendingFiles = 0;
        for (var entry : state.indices().entrySet()) {
            String indexName = entry.getKey();
            if (indexFilter != null && !indexFilter.equals(indexName)) {
                continue;
            }
            List<Map<String, Object>> shards = new ArrayList<>();
            for (int shard = 0; shard < entry.getValue().numberOfShards(); shard++) {
                Map<String, Object> shardStatus = shardUploadStatus(state, indexName, shard, now);
                shards.add(shardStatus);
                boolean pending = Boolean.TRUE.equals(shardStatus.get("pending"));
                boolean stuck = Boolean.TRUE.equals(shardStatus.get("stuck"));
                if (pending) {
                    pendingShards++;
                    pendingFiles += ((Number) shardStatus.get("pending_files")).longValue();
                }
                if (stuck) {
                    stuckShards++;
                }
            }
            indices.add(Map.of(
                    "index", indexName,
                    "shards", shards
            ));
        }
        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("indices", indices.size());
        summary.put("pending_shards", pendingShards);
        summary.put("stuck_shards", stuckShards);
        summary.put("pending_files", pendingFiles);
        summary.put("stuck_after_millis", STUCK_PENDING_UPLOAD_MILLIS);
        return Map.of(
                "summary", summary,
                "indices", indices
        );
    }

    private Map<String, Object> shardUploadStatus(ClusterState state, String indexName, int shard, Instant now) {
        String physicalIndexName = physicalIndexName(indexName, shard);
        List<IndexFileMetadata> files = manifestMetadataManager.listAll(physicalIndexName, ALL_UPLOAD_STATUSES);
        List<IndexFileMetadata> pending = files.stream()
                .filter(file -> PENDING_UPLOAD_STATUSES.contains(file.getStatus()))
                .toList();
        Map<String, Integer> statusCounts = new LinkedHashMap<>();
        for (IndexFileStatus status : IndexFileStatus.values()) {
            statusCounts.put(status.name(), 0);
        }
        for (IndexFileMetadata file : files) {
            statusCounts.computeIfPresent(file.getStatus().name(), (ignored, value) -> value + 1);
        }
        long oldestPendingAgeMillis = pending.stream()
                .mapToLong(file -> Math.max(0, now.toEpochMilli() - file.getModifiedTime()))
                .max()
                .orElse(0);
        IndexCommitSnapshotHeader latestSnapshot = manifestMetadataManager.latestSnapshotHeader(physicalIndexName);
        ShardRouting routing = routing(state, indexName, shard);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("index", indexName);
        result.put("shard", shard);
        result.put("physical_index", physicalIndexName);
        result.put("owner_node", routing == null ? null : routing.nodeId());
        result.put("owner_term", routing == null ? null : routing.ownerTerm());
        result.put("allocation_epoch", routing == null ? null : routing.allocationEpoch());
        result.put("status_counts", statusCounts);
        result.put("pending", !pending.isEmpty());
        result.put("pending_files", pending.size());
        result.put("oldest_pending_age_millis", oldestPendingAgeMillis);
        result.put("stuck", !pending.isEmpty() && oldestPendingAgeMillis >= STUCK_PENDING_UPLOAD_MILLIS);
        result.put("remote_snapshot_ready", pending.isEmpty() && latestSnapshot != null);
        result.put("latest_snapshot_generation", latestSnapshot == null ? null : latestSnapshot.generation());
        result.put("latest_snapshot_segment", latestSnapshot == null ? null : latestSnapshot.segmentFileName());
        result.put("pending_file_details", pending.stream().map(this::pendingFile).toList());
        return result;
    }

    private Map<String, Object> pendingFile(IndexFileMetadata file) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("name", file.getName());
        result.put("status", file.getStatus().name());
        result.put("epoch", file.getEpoch());
        result.put("size", file.getSize());
        result.put("modified_time", file.getModifiedTime());
        result.put("object_key", file.getObjectKey());
        return result;
    }

    private ShardRouting routing(ClusterState state, String indexName, int shard) {
        ShardId shardId = new ShardId(indexName, shard);
        return state.routingTable().stream()
                .filter(routing -> routing.shardId().equals(shardId))
                .findFirst()
                .orElse(null);
    }

    private void runLifecyclePolicies() {
        ClusterState state;
        try {
            state = clusterStateRepository.current();
        } catch (IOException e) {
            log.warn("failed to load cluster state for lifecycle execution", e);
            return;
        }
        Instant now = Instant.now();
        runWarmLifecyclePolicies(state, now);
        if (!clusterCoordinator.isMaster()) {
            return;
        }
        retryPendingIndexDeletes(state);
        runDeleteLifecyclePolicies(state, now);
    }

    private void runWarmLifecyclePolicies(ClusterState state, Instant now) {
        forgetStaleForceMergeCompletions(state);
        state.routingTable().stream()
                .filter(routing -> routing.state() == ShardState.STARTED)
                .filter(routing -> localNode.id().equals(routing.nodeId()))
                .forEach(routing -> runWarmLifecyclePolicy(state, now, routing));
    }

    private void runWarmLifecyclePolicy(ClusterState state, Instant now, ShardRouting routing) {
        var settings = state.indices().get(routing.shardId().indexName());
        if (settings == null) {
            return;
        }
        if (settings.deletePending()) {
            return;
        }
        IndexLifecyclePolicy policy = lifecyclePolicy(state, settings.lifecyclePolicy());
        if (policy == null) {
            return;
        }
        Long warmAgeMillis = policy.minAgeMillisByPhase().get(LifecyclePhase.WARM);
        if (warmAgeMillis == null) {
            return;
        }
        long indexAgeMillis = Math.max(0, Duration.between(settings.createdAt(), now).toMillis());
        if (indexAgeMillis < warmAgeMillis) {
            return;
        }
        String key = routing.shardId().routeKey() + "/" + routing.ownerTerm() + "/" + routing.allocationEpoch();
        if (lifecycleForceMergesCompleted.contains(key) || !lifecycleForceMergesInProgress.add(key)) {
            return;
        }
        if (!tryAcquireShardScope(routing.shardId())) {
            lifecycleForceMergesInProgress.remove(key);
            return;
        }
        try {
            localShardIndexService.forceMerge(routing.shardId(), 1);
            lifecycleForceMergesCompleted.add(key);
            log.info("force-merged shard {} by warm lifecycle policy {}", routing.shardId().routeKey(), policy.name());
        } catch (Exception e) {
            log.warn("failed to force-merge shard {} by warm lifecycle policy {}", routing.shardId().routeKey(), policy.name(), e);
        } finally {
            releaseShardScope(routing.shardId());
            lifecycleForceMergesInProgress.remove(key);
        }
    }

    private void runDeleteLifecyclePolicies(ClusterState state, Instant now) {
        state.indices().forEach((indexName, settings) -> {
            if (settings.deletePending()) {
                return;
            }
            IndexLifecyclePolicy policy = lifecyclePolicy(state, settings.lifecyclePolicy());
            if (policy == null) {
                return;
            }
            Long deleteAgeMillis = policy.minAgeMillisByPhase().get(LifecyclePhase.DELETE);
            if (deleteAgeMillis == null) {
                return;
            }
            long indexAgeMillis = Math.max(0, Duration.between(settings.createdAt(), now).toMillis());
            if (indexAgeMillis < deleteAgeMillis || !lifecycleDeletesInProgress.add(indexName)) {
                return;
            }
            if (!tryAcquireIndexScope(indexName)) {
                lifecycleDeletesInProgress.remove(indexName);
                return;
            }
            try {
                indexDataDeleter.deleteIndexAndData(indexName, settings.numberOfShards());
                log.info("deleted index {} by lifecycle policy {}", indexName, policy.name());
            } catch (Exception e) {
                log.warn("failed to delete index {} by lifecycle policy {}", indexName, policy.name(), e);
            } finally {
                releaseIndexScope(indexName);
                lifecycleDeletesInProgress.remove(indexName);
            }
        });
    }

    private void retryPendingIndexDeletes(ClusterState state) {
        state.indices().forEach((indexName, settings) -> {
            if (!settings.deletePending() || !lifecycleDeletesInProgress.add(indexName)) {
                return;
            }
            if (!tryAcquireIndexScope(indexName)) {
                lifecycleDeletesInProgress.remove(indexName);
                return;
            }
            try {
                indexDataDeleter.deleteIndexAndData(indexName, settings.numberOfShards());
                log.info("completed pending delete for index {}", indexName);
            } catch (Exception e) {
                log.warn("failed to complete pending delete for index {}", indexName, e);
            } finally {
                releaseIndexScope(indexName);
                lifecycleDeletesInProgress.remove(indexName);
            }
        });
    }

    private IndexLifecyclePolicy lifecyclePolicy(ClusterState state, String policyName) {
        if (policyName == null || policyName.isBlank()) {
            return null;
        }
        return state.lifecyclePolicies().get(policyName);
    }

    private void forgetStaleForceMergeCompletions(ClusterState state) {
        if (lifecycleForceMergesCompleted.isEmpty()) {
            return;
        }
        Set<String> ownedKeys = new HashSet<>();
        for (ShardRouting routing : state.routingTable()) {
            if (!localNode.id().equals(routing.nodeId()) || routing.state() != ShardState.STARTED) {
                continue;
            }
            if (state.indices().get(routing.shardId().indexName()) == null) {
                continue;
            }
            ownedKeys.add(routing.shardId().routeKey()
                    + "/" + routing.ownerTerm() + "/" + routing.allocationEpoch());
        }
        lifecycleForceMergesCompleted.retainAll(ownedKeys);
    }

    private void runLocalCacheCleanup() {
        if (cacheManager == null || !cacheManager.enabled()) {
            return;
        }
        Instant now = Instant.now();
        if (Duration.between(lastCacheCleanup, now).compareTo(cacheCleanupInterval) < 0) {
            return;
        }
        lastCacheCleanup = now;
        lastCacheCleanupStats = cacheManager.cleanup();
    }

    private String physicalIndexName(String indexName, int shard) {
        return indexName + "__shard_" + shard;
    }

    private boolean tryAcquireIndexScope(String indexName) {
        synchronized (scopeLock) {
            if (indexScopes.contains(indexName) || hasShardScope(indexName)) {
                return false;
            }
            indexScopes.add(indexName);
            return true;
        }
    }

    private void releaseIndexScope(String indexName) {
        synchronized (scopeLock) {
            indexScopes.remove(indexName);
        }
    }

    private boolean tryAcquireShardScope(ShardId shardId) {
        synchronized (scopeLock) {
            String key = shardId.routeKey();
            if (indexScopes.contains(shardId.indexName()) || shardScopes.contains(key)) {
                return false;
            }
            shardScopes.add(key);
            return true;
        }
    }

    private void releaseShardScope(ShardId shardId) {
        synchronized (scopeLock) {
            shardScopes.remove(shardId.routeKey());
        }
    }

    private boolean hasShardScope(String indexName) {
        String prefix = indexName + "[";
        return shardScopes.stream().anyMatch(scope -> scope.startsWith(prefix));
    }

    interface IndexDataDeleter {
        void deleteIndexAndData(String indexName, int numberOfShards) throws IOException;
    }
}
