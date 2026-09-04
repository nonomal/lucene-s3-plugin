package com.github.wxk6b1203.http;

import com.github.wxk6b1203.cluster.*;
import com.github.wxk6b1203.index.LocalShardIndexService;
import com.github.wxk6b1203.metadata.common.IndexCommitSnapshot;
import com.github.wxk6b1203.metadata.common.IndexCommitSnapshotHeader;
import com.github.wxk6b1203.metadata.common.IndexFileStatus;
import com.github.wxk6b1203.metadata.provider.ManifestMetadataManager;
import com.github.wxk6b1203.store.directory.RemoteCacheStats;
import com.github.wxk6b1203.store.object.S3RemoteObjectStore;
import io.vertx.ext.web.RoutingContext;

import java.io.IOException;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntSupplier;

import static com.github.wxk6b1203.http.HttpApiRequestParsing.longValue;
import static com.github.wxk6b1203.http.HttpApiRequestParsing.mapValue;
import static com.github.wxk6b1203.http.HttpApiResponses.*;

final class ClusterIntrospectionHandlers {
    private final ClusterStateRepository clusterStateRepository;
    private final ClusterMaintenanceService maintenanceService;
    private final ClusterNode localNode;
    private final LocalShardIndexService localShardIndexService;
    private final ManifestMetadataManager manifestMetadataManager;
    private final ServerMetrics serverMetrics;
    private final IntSupplier coordinatingPitCount;

    ClusterIntrospectionHandlers(
            ClusterStateRepository clusterStateRepository,
            ClusterMaintenanceService maintenanceService,
            ClusterNode localNode,
            LocalShardIndexService localShardIndexService,
            ManifestMetadataManager manifestMetadataManager,
            ServerMetrics serverMetrics,
            IntSupplier coordinatingPitCount
    ) {
        this.clusterStateRepository = clusterStateRepository;
        this.maintenanceService = maintenanceService;
        this.localNode = localNode;
        this.localShardIndexService = localShardIndexService;
        this.manifestMetadataManager = manifestMetadataManager;
        this.serverMetrics = serverMetrics;
        this.coordinatingPitCount = coordinatingPitCount;
    }

    void clusterState(RoutingContext context) {
        try {
            json(context, 200, clusterStateRepository.current());
        } catch (IOException e) {
            error(context, 500, e);
        }
    }

    void nodes(RoutingContext context) {
        try {
            json(context, 200, clusterStateRepository.current().nodes());
        } catch (IOException e) {
            error(context, 500, e);
        }
    }

    void clusterHealth(RoutingContext context) {
        try {
            ClusterState state = clusterStateRepository.current();
            Map<String, Object> uploads = maintenanceService.uploadStatus(null);
            Map<String, Object> summary = mapValue(uploads.get("summary"));
            long pendingUploads = longValue(summary.get("pending_shards"), 0);
            long stuckUploads = longValue(summary.get("stuck_shards"), 0);
            long activeShards = state.routingTable().stream()
                    .filter(routing -> routing.state() == ShardState.STARTED)
                    .count();
            long unassignedShards = state.routingTable().stream()
                    .filter(routing -> routing.state() != ShardState.STARTED
                            || !state.nodes().containsKey(routing.nodeId()))
                    .count();
            String status = "green";
            if (state.masterNodeId() == null || unassignedShards > 0) {
                status = "red";
            } else if (pendingUploads > 0 || stuckUploads > 0) {
                status = "yellow";
            }
            Map<String, Object> response = new LinkedHashMap<>();
            response.put("cluster_name", state.clusterName());
            response.put("status", status);
            response.put("timed_out", false);
            response.put("master_node", state.masterNodeId());
            response.put("number_of_nodes", state.nodes().size());
            response.put("active_shards", activeShards);
            response.put("unassigned_shards", unassignedShards);
            response.put("pending_upload_shards", pendingUploads);
            response.put("stuck_upload_shards", stuckUploads);
            response.put("cluster_state_version", state.version());
            serverMetrics.setClusterHealth(activeShards, unassignedShards, pendingUploads, stuckUploads);
            json(context, 200, response);
        } catch (Exception e) {
            error(context, status(e), e);
        }
    }

    void shards(RoutingContext context) {
        try {
            ClusterState state = clusterStateRepository.current();
            List<Map<String, Object>> shards = state.routingTable().stream()
                    .sorted(Comparator
                            .comparing((ShardRouting routing) -> routing.shardId().indexName())
                            .thenComparingInt(routing -> routing.shardId().shardNumber()))
                    .map(routing -> shardStats(state, routing))
                    .toList();
            json(context, 200, Map.of(
                    "cluster_state_version", state.version(),
                    "shards", shards
            ));
        } catch (Exception e) {
            error(context, status(e), e);
        }
    }

    void indices(RoutingContext context) {
        try {
            ClusterState state = clusterStateRepository.current();
            List<Map<String, Object>> indices = state.indices().entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .map(entry -> indexStats(state, entry.getKey(), entry.getValue(), false))
                    .toList();
            json(context, 200, Map.of(
                    "cluster_state_version", state.version(),
                    "indices", indices
            ));
        } catch (Exception e) {
            error(context, status(e), e);
        }
    }

    void snapshotStatus(RoutingContext context) {
        try {
            json(context, 200, maintenanceService.uploadStatus(context.queryParams().get("index")));
        } catch (Exception e) {
            error(context, status(e), e);
        }
    }

    void nodeStats(RoutingContext context) {
        try {
            ClusterState state = clusterStateRepository.current();
            Map<String, Object> localStats = new LinkedHashMap<>();
            localStats.put("name", localNode.name());
            localStats.put("host", localNode.host());
            localStats.put("http_port", localNode.httpPort());
            localStats.put("roles", localNode.roles());
            localStats.put("coordinating_pits", coordinatingPitCount.getAsInt());
            localStats.put("local_pits", localShardIndexService.openPointInTimeCount());
            Map<String, Object> cacheStats = RemoteCacheStats.snapshot();
            Map<String, Object> s3Stats = S3RemoteObjectStore.statsSnapshot();
            localStats.put("cache", cacheStats);
            localStats.put("cache_cleanup", maintenanceService.cacheStatus());
            localStats.put("s3", s3Stats);
            localStats.put("metrics_port", serverMetrics.metricsPort());
            localStats.put("cluster_state_version", state.version());
            serverMetrics.setPitCounts(coordinatingPitCount.getAsInt(), localShardIndexService.openPointInTimeCount());
            serverMetrics.setCacheStats(cacheStats);
            serverMetrics.setS3Stats(s3Stats);
            Map<String, Object> nodes = new LinkedHashMap<>();
            nodes.put(localNode.id(), localStats);
            json(context, 200, Map.of("nodes", nodes));
        } catch (Exception e) {
            error(context, status(e), e);
        }
    }

    void uploadStatus(RoutingContext context) {
        try {
            json(context, 200, maintenanceService.uploadStatus(context.pathParam("index")));
        } catch (Exception e) {
            error(context, status(e), e);
        }
    }

    void retryUploads(RoutingContext context) {
        try {
            json(context, 200, maintenanceService.retryPendingUploadsNow(context.pathParam("index")));
        } catch (Exception e) {
            error(context, status(e), e);
        }
    }

    Map<String, Object> indexStats(
            ClusterState state,
            String indexName,
            IndexSettings settings,
            boolean includeMappings
    ) {
        List<ShardRouting> routings = state.routingTable().stream()
                .filter(routing -> routing.shardId().indexName().equals(indexName))
                .sorted(Comparator.comparingInt(routing -> routing.shardId().shardNumber()))
                .toList();
        long started = 0;
        long ownerUnavailable = 0;
        long remoteSnapshotReady = 0;
        long pendingUploads = 0;
        for (ShardRouting routing : routings) {
            if (routing.state() == ShardState.STARTED) {
                started++;
            }
            if (routing.state() == ShardState.STARTED
                    && (routing.nodeId() == null || !state.nodes().containsKey(routing.nodeId()))) {
                ownerUnavailable++;
            }
            ShardId shardId = routing.shardId();
            IndexCommitSnapshotHeader snapshot = latestRemoteSnapshot(shardId);
            if (remoteSnapshotReady(shardId, snapshot)) {
                remoteSnapshotReady++;
            }
            pendingUploads += pendingUploadCount(physicalIndexName(shardId));
        }
        long missingRouting = Math.max(0, settings.numberOfShards() - routings.size());
        long unassigned = routings.size() - started + missingRouting;
        boolean routingMismatch = routings.size() != settings.numberOfShards();
        String status = "green";
        if (settings.deletePending() || routingMismatch || unassigned > 0 || ownerUnavailable > 0) {
            status = "red";
        } else if (pendingUploads > 0) {
            status = "yellow";
        }

        Map<String, Object> shardSummary = new LinkedHashMap<>();
        shardSummary.put("total", settings.numberOfShards());
        shardSummary.put("routing_entries", routings.size());
        shardSummary.put("routing_mismatch", routingMismatch);
        shardSummary.put("started", started);
        shardSummary.put("unassigned", unassigned);
        shardSummary.put("owner_unavailable", ownerUnavailable);
        shardSummary.put("remote_snapshot_ready", remoteSnapshotReady);
        shardSummary.put("pending_uploads", pendingUploads);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("name", indexName);
        result.put("status", status);
        result.put("number_of_shards", settings.numberOfShards());
        result.put("lifecycle_policy", settings.lifecyclePolicy());
        result.put("created_at", settings.createdAt());
        result.put("delete_pending", settings.deletePending());
        result.put("delete_started_at", settings.deleteStartedAt());
        result.put("mapping_fields", settings.mappings().size());
        result.put("shards", shardSummary);
        if (includeMappings) {
            result.put("mappings", Map.of("properties", settings.mappings()));
        }
        return result;
    }

    private Map<String, Object> shardStats(ClusterState state, ShardRouting routing) {
        ShardId shardId = routing.shardId();
        ClusterNode owner = state.nodes().get(routing.nodeId());
        IndexCommitSnapshotHeader snapshot = latestRemoteSnapshot(shardId);
        String physicalIndexName = physicalIndexName(shardId);
        long pendingUploads = pendingUploadCount(physicalIndexName);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("index", shardId.indexName());
        result.put("shard", shardId.shardNumber());
        result.put("physical_index", physicalIndexName);
        result.put("state", routing.state());
        result.put("owner_node", routing.nodeId());
        result.put("owner_live", owner != null);
        result.put("owner_term", routing.ownerTerm());
        result.put("allocation_epoch", routing.allocationEpoch());
        result.put("latest_snapshot_generation", snapshot == null ? null : snapshot.generation());
        result.put("latest_snapshot_segment", snapshot == null ? null : snapshot.segmentFileName());
        result.put("remote_snapshot_ready", remoteSnapshotReady(shardId, snapshot));
        result.put("pending_uploads", pendingUploads);
        return result;
    }

    private long pendingUploadCount(String physicalIndexName) {
        return manifestMetadataManager.listAll(physicalIndexName, List.of(
                        IndexFileStatus.DIRTY,
                        IndexFileStatus.UPLOADING
                ))
                .stream()
                .filter(metadata -> metadata.getStatus() == IndexFileStatus.DIRTY
                        || metadata.getStatus() == IndexFileStatus.UPLOADING)
                .count();
    }

    private boolean remoteSnapshotReady(ShardId shardId, IndexCommitSnapshotHeader snapshot) {
        if (snapshot == null) {
            return false;
        }
        String physicalIndexName = physicalIndexName(shardId);
        return manifestMetadataManager.listAll(physicalIndexName, List.of(
                        IndexFileStatus.DIRTY,
                        IndexFileStatus.UPLOADING,
                        IndexFileStatus.CLEAN,
                        IndexFileStatus.PINNED
                ))
                .stream()
                .noneMatch(metadata -> metadata.getStatus() == IndexFileStatus.DIRTY
                        || metadata.getStatus() == IndexFileStatus.UPLOADING);
    }

    private IndexCommitSnapshotHeader latestRemoteSnapshot(ShardId shardId) {
        return manifestMetadataManager.latestSnapshotHeader(physicalIndexName(shardId));
    }

    private String physicalIndexName(ShardId shardId) {
        return shardId.indexName() + "__shard_" + shardId.shardNumber();
    }
}
