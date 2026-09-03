package com.github.wxk6b1203.index;

import com.github.wxk6b1203.cluster.ClusterState;
import com.github.wxk6b1203.cluster.ShardId;
import com.github.wxk6b1203.search.*;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public interface LocalShardIndexService extends AutoCloseable {
    IndexDocumentResponse index(IndexDocumentRequest request) throws IOException;

    IndexDocumentResponse delete(IndexDocumentRequest request) throws IOException;

    default List<IndexDocumentOperationResult> bulk(Collection<IndexDocumentOperation> operations) throws IOException {
        List<IndexDocumentOperationResult> results = new ArrayList<>(operations.size());
        for (IndexDocumentOperation operation : operations) {
            try {
                IndexDocumentResponse response = operation.delete()
                        ? delete(operation.request())
                        : index(operation.request());
                results.add(IndexDocumentOperationResult.success(response));
            } catch (Exception e) {
                results.add(IndexDocumentOperationResult.failure(e));
            }
        }
        return results;
    }

    SearchResponse search(ShardId shardId, SearchRequest request) throws IOException;

    default PointInTimeResponse openPointInTime(ShardId shardId, String indexName, Duration keepAlive) throws IOException {
        return openPointInTime(shardId, indexName, keepAlive, "weak");
    }

    PointInTimeResponse openPointInTime(
            ShardId shardId,
            String indexName,
            Duration keepAlive,
            String readPreference
    ) throws IOException;

    boolean closePointInTime(String pitId) throws IOException;

    ByQueryResponse updateByQuery(ShardId shardId, ByQueryRequest request) throws IOException;

    ByQueryResponse deleteByQuery(ShardId shardId, ByQueryRequest request) throws IOException;

    default void forceMerge(ShardId shardId, int maxNumSegments) throws IOException {
    }

    void deleteIndex(String indexName, int numberOfShards) throws IOException;

    void retryPendingUploads(Collection<ShardId> shardIds) throws IOException;

    default Collection<ShardId> shardIdsWithPendingWrites() throws IOException {
        return List.of();
    }

    default Collection<ShardId> shardIdsWithPendingUploads() throws IOException {
        return List.of();
    }

    default void runWriteMaintenance() throws IOException {
    }

    default void runWriteMaintenance(Collection<ShardId> shardIds) throws IOException {
        runWriteMaintenance();
    }

    /**
     * Align local shard state with cluster state: retire writers and drain/quarantine leftover
     * local WAL data for shards no longer owned by {@code expectedOwnerNodeId}. Must be a no-op
     * when the cluster state cannot be read (never act on stale or missing evidence).
     */
    default void reconcileWithClusterState(ClusterState state, String expectedOwnerNodeId) throws IOException {
    }

    /**
     * Stop serving the given shard locally: drain committed-but-unuploaded content (upload only,
     * no snapshot publish), close associated PITs and the writer, then quarantine local WAL files
     * so a same-name-different-content collision cannot corrupt a future reassignment.
     */
    default void retireShardWriter(ShardId shardId) throws IOException {
    }

    default void cleanupIdleResources() throws IOException {
    }

    default int openPointInTimeCount() {
        return 0;
    }

    @Override
    void close() throws IOException;
}
