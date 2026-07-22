package com.github.wxk6b1203.cluster.etcd;

import com.github.wxk6b1203.cluster.ClusterNode;
import com.github.wxk6b1203.cluster.ClusterState;
import com.github.wxk6b1203.cluster.ClusterStateRepository;
import com.github.wxk6b1203.util.JsonUtil;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.op.Cmp;
import io.etcd.jetcd.op.CmpTarget;
import io.etcd.jetcd.op.Op;
import io.etcd.jetcd.options.GetOption;
import lombok.Builder;
import lombok.Data;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class EtcdClusterStateRepository implements ClusterStateRepository {
    private static final int MAX_CAS_RETRIES = 32;

    private final Client client;
    private final String namespace;
    private final String clusterName;
    private final long operationTimeoutSeconds;

    @Data
    @Builder
    public static class Options {
        private String endpoints;
        @Builder.Default
        private String namespace = "lucene-s3/cluster";
        @Builder.Default
        private String clusterName = "lucene-s3";
        @Builder.Default
        private long operationTimeoutSeconds = 10;
    }

    public EtcdClusterStateRepository(Options options) {
        this(options, Client.builder().endpoints(options.endpoints).build());
    }

    public EtcdClusterStateRepository(Options options, Client client) {
        this.client = client;
        this.namespace = normalize(options.namespace);
        this.clusterName = options.clusterName;
        this.operationTimeoutSeconds = Math.max(1, options.operationTimeoutSeconds);
    }

    @Override
    public ClusterState current() throws IOException {
        try {
            KeyValue stateKv = stateKv();
            ClusterState state = stateKv == null ? emptyState() : decodeState(stateKv);
            return mergeLiveNodes(state);
        } catch (Exception e) {
            throw ioException("failed to read cluster state", e);
        }
    }

    @Override
    public ClusterState update(ClusterStateUpdate update) throws IOException {
        for (int attempt = 0; attempt < MAX_CAS_RETRIES; attempt++) {
            try {
                KeyValue currentKv = stateKv();
                ClusterState current = currentKv == null ? emptyState() : decodeState(currentKv);
                current = mergeLiveNodes(current);
                ClusterState updated = update.apply(current);
                if (samePersistentContent(current, updated)) {
                    return current;
                }
                ClusterState next = bump(updated, current.version() + 1);
                ByteSequence stateKey = key("state");
                Cmp cmp = currentKv == null
                        ? new Cmp(stateKey, Cmp.Op.EQUAL, CmpTarget.version(0))
                        : new Cmp(stateKey, Cmp.Op.EQUAL, CmpTarget.modRevision(currentKv.getModRevision()));
                var response = await(client.getKVClient().txn()
                        .If(cmp)
                        .Then(Op.put(stateKey, ByteSequence.from(JsonUtil.writeValueAsBytes(next)), io.etcd.jetcd.options.PutOption.DEFAULT))
                        .commit());
                if (response.isSucceeded()) {
                    return next;
                }
            } catch (RuntimeException e) {
                // Business exceptions (e.g. IllegalArgumentException for "index is deleting" or
                // "index not found") must propagate unwrapped so they map to the same HTTP status
                // as single-node mode. Wrapping them in IOException would turn a 400 into a 500
                // and make error codes depend on whether etcd is configured.
                throw e;
            } catch (Exception e) {
                throw ioException("failed to update cluster state", e);
            }
        }
        throw new IOException("failed to update cluster state after CAS retries");
    }

    private boolean samePersistentContent(ClusterState left, ClusterState right) {
        return Objects.equals(left.clusterName(), right.clusterName())
                && Objects.equals(left.masterNodeId(), right.masterNodeId())
                && Objects.equals(left.indices(), right.indices())
                && Objects.equals(left.routingTable(), right.routingTable())
                && Objects.equals(left.lifecyclePolicies(), right.lifecyclePolicies());
    }

    public void putNode(ClusterNode node, long leaseId) throws IOException {
        try {
            await(client.getKVClient().put(
                    nodeKey(node.id()),
                    ByteSequence.from(JsonUtil.writeValueAsBytes(node)),
                    io.etcd.jetcd.options.PutOption.builder().withLeaseId(leaseId).build()
            ));
        } catch (Exception e) {
            throw ioException("failed to put node heartbeat", e);
        }
    }

    private ClusterState mergeLiveNodes(ClusterState state) throws Exception {
        Map<String, ClusterNode> nodes = new HashMap<>();
        var response = await(client.getKVClient()
                .get(nodesPrefix(), GetOption.builder().isPrefix(true).build()));
        for (KeyValue kv : response.getKvs()) {
            ClusterNode node = JsonUtil.readValue(kv.getValue().getBytes(), ClusterNode.class);
            nodes.put(node.id(), node);
        }
        return new ClusterState(
                state.clusterName(),
                state.version(),
                state.masterNodeId(),
                nodes,
                state.indices(),
                state.routingTable(),
                state.lifecyclePolicies(),
                state.updatedAt()
        );
    }

    private KeyValue stateKv() throws Exception {
        var response = await(client.getKVClient().get(key("state")));
        return response.getKvs().isEmpty() ? null : response.getKvs().getFirst();
    }

    private <T> T await(CompletableFuture<T> future) throws Exception {
        return future.get(operationTimeoutSeconds, TimeUnit.SECONDS);
    }

    private ClusterState decodeState(KeyValue kv) {
        return JsonUtil.readValue(kv.getValue().getBytes(), ClusterState.class);
    }

    private ClusterState emptyState() {
        return new ClusterState(clusterName, 0, null, Map.of(), Map.of(), List.of(), Map.of(), Instant.now());
    }

    private ClusterState bump(ClusterState state, long version) {
        return new ClusterState(
                state.clusterName(),
                version,
                state.masterNodeId(),
                state.nodes(),
                state.indices(),
                state.routingTable(),
                state.lifecyclePolicies(),
                Instant.now()
        );
    }

    private ByteSequence nodesPrefix() {
        return key("nodes/");
    }

    private ByteSequence nodeKey(String nodeId) {
        return key("nodes/" + nodeId);
    }

    private ByteSequence key(String suffix) {
        return ByteSequence.from((namespace + "/" + suffix).getBytes(StandardCharsets.UTF_8));
    }

    private String normalize(String value) {
        String normalized = value == null || value.isBlank() ? "lucene-s3/cluster" : value;
        while (normalized.startsWith("/")) {
            normalized = normalized.substring(1);
        }
        while (normalized.endsWith("/")) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        return "/" + normalized;
    }

    private IOException ioException(String message, Exception cause) {
        String causeMessage = cause.getMessage();
        IOException exception = new IOException(causeMessage == null ? message : message + ": " + causeMessage);
        exception.initCause(cause);
        return exception;
    }
}
