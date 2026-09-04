package com.github.wxk6b1203.cluster.etcd;

import com.github.wxk6b1203.cluster.ClusterNode;
import com.github.wxk6b1203.cluster.ClusterState;
import com.github.wxk6b1203.cluster.ClusterStateRepository;
import com.github.wxk6b1203.cluster.IndexLifecyclePolicy;
import com.github.wxk6b1203.cluster.IndexSettings;
import com.github.wxk6b1203.cluster.ShardRouting;
import com.github.wxk6b1203.util.JsonUtil;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.kv.TxnResponse;
import io.etcd.jetcd.op.Cmp;
import io.etcd.jetcd.op.CmpTarget;
import io.etcd.jetcd.op.Op;
import io.etcd.jetcd.options.GetOption;
import lombok.Builder;
import lombok.Data;
import tools.jackson.databind.JsonNode;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Cluster state on etcd as SPLIT KEYS: a small head key ({@code state}) plus one key per index
 * ({@code indices/{name}}), one key per index's routing list ({@code routing/{indexName}}) and
 * one key per lifecycle policy ({@code lifecycle/{name}}). The legacy layout serialized the
 * whole state (all indices + mappings + routing) into the single {@code state} key, which hit
 * etcd's 2MB grpc / 1.5MB request limits at ~1000 small indices and rewrote every byte of the
 * state on every update. Writes here diff old vs new and touch only changed keys; every chunk
 * transaction is guarded by the head revision so concurrent updaters serialize cleanly.
 *
 * <p>Consistency bound: content prefixes are read with paged range gets (separate RPCs), so a
 * concurrent update can interleave between pages — the result may mix content from two adjacent
 * state versions. This is fail-safe downstream: writes re-validate ownerTerm/allocationEpoch at
 * the shard owner and CAS the head version, and search plans are re-derived per request.
 */
public class EtcdClusterStateRepository implements ClusterStateRepository {
    private static final int MAX_CAS_RETRIES = 32;
    /** Content keys per guarded chunk transaction (etcd max-txn-ops is 128). */
    private static final int WRITE_CHUNK_OPS = 64;
    /** Keys per paged range read; keeps each response well under the grpc message limit. */
    private static final int READ_PAGE_LIMIT = 250;

    private final Client client;
    private final String namespace;
    private final String clusterName;
    private final long operationTimeoutSeconds;

    /** Small head record persisted under the {@code state} key. */
    public record ClusterStateHead(
            String clusterName,
            long version,
            String masterNodeId,
            Instant updatedAt
    ) {
    }

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
        this.operationTimeoutSeconds = Math.max(1, options == null ? 10 : options.operationTimeoutSeconds);
    }

    @Override
    public ClusterState current() throws IOException {
        try {
            return readSnapshot().state();
        } catch (Exception e) {
            throw ioException("failed to read cluster state", e);
        }
    }

    @Override
    public ClusterState update(ClusterStateUpdate update) throws IOException {
        for (int attempt = 0; attempt < MAX_CAS_RETRIES; attempt++) {
            try {
                StateSnapshot snapshot = readSnapshot();
                KeyValue currentKv = snapshot.stateKv();
                ClusterState current = snapshot.state();
                ClusterState updated = update.apply(current);
                if (samePersistentContent(current, updated)) {
                    return current;
                }
                ClusterState next = bump(updated, current.version() + 1);
                // When migrating from the legacy single-key layout the content diff must treat
                // every existing index/routing/policy as ADDED: the legacy content lives only
                // inside the state value, so an unchanged entry would otherwise never land in
                // its split key and would silently vanish after the head overwrites the value.
                byte[] stateRaw = currentKv == null ? null : currentKv.getValue().getBytes();
                boolean legacyLayout = stateRaw != null
                        && JsonUtil.readValue(stateRaw, JsonNode.class).has("indices");
                ClusterState diffBase = legacyLayout
                        ? new ClusterState(current.clusterName(), current.version(), current.masterNodeId(),
                                Map.of(), Map.of(), List.of(), Map.of(), current.updatedAt())
                        : current;
                if (writeContentAndHead(currentKv, diffBase, next)) {
                    return next;
                }
                // Head moved underneath us: re-read and retry.
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

    /**
     * Diff old vs new content and write it in chunk transactions, each guarded by the head
     * revision read up front; the last chunk also CAS-writes the bumped head. A failed guard
     * means a concurrent update committed in between — nothing of ours was applied and the
     * caller retries against the fresh state.
     */
    private boolean writeContentAndHead(KeyValue currentKv, ClusterState current, ClusterState next) throws Exception {
        ByteSequence headKey = key("state");
        List<Op> ops = new ArrayList<>();
        for (var entry : next.indices().entrySet()) {
            if (!Objects.equals(current.indices().get(entry.getKey()), entry.getValue())) {
                ops.add(Op.put(key("indices/" + entry.getKey()),
                        ByteSequence.from(JsonUtil.writeValueAsBytes(entry.getValue())),
                        io.etcd.jetcd.options.PutOption.DEFAULT));
            }
        }
        for (String name : current.indices().keySet()) {
            if (!next.indices().containsKey(name)) {
                ops.add(Op.delete(key("indices/" + name), io.etcd.jetcd.options.DeleteOption.DEFAULT));
            }
        }
        Map<String, List<ShardRouting>> oldRouting = groupRouting(current.routingTable());
        Map<String, List<ShardRouting>> newRouting = groupRouting(next.routingTable());
        for (var entry : newRouting.entrySet()) {
            if (!Objects.equals(oldRouting.get(entry.getKey()), entry.getValue())) {
                ops.add(Op.put(key("routing/" + entry.getKey()),
                        ByteSequence.from(JsonUtil.writeValueAsBytes(entry.getValue())),
                        io.etcd.jetcd.options.PutOption.DEFAULT));
            }
        }
        for (String name : oldRouting.keySet()) {
            if (!newRouting.containsKey(name)) {
                ops.add(Op.delete(key("routing/" + name), io.etcd.jetcd.options.DeleteOption.DEFAULT));
            }
        }
        for (var entry : next.lifecyclePolicies().entrySet()) {
            if (!Objects.equals(current.lifecyclePolicies().get(entry.getKey()), entry.getValue())) {
                ops.add(Op.put(key("lifecycle/" + entry.getKey()),
                        ByteSequence.from(JsonUtil.writeValueAsBytes(entry.getValue())),
                        io.etcd.jetcd.options.PutOption.DEFAULT));
            }
        }
        for (String name : current.lifecyclePolicies().keySet()) {
            if (!next.lifecyclePolicies().containsKey(name)) {
                ops.add(Op.delete(key("lifecycle/" + name), io.etcd.jetcd.options.DeleteOption.DEFAULT));
            }
        }

        Cmp headGuard = currentKv == null
                ? new Cmp(headKey, Cmp.Op.EQUAL, CmpTarget.version(0))
                : new Cmp(headKey, Cmp.Op.EQUAL, CmpTarget.modRevision(currentKv.getModRevision()));
        Op headPut = Op.put(headKey,
                ByteSequence.from(JsonUtil.writeValueAsBytes(new ClusterStateHead(
                        next.clusterName(), next.version(), next.masterNodeId(), next.updatedAt()))),
                io.etcd.jetcd.options.PutOption.DEFAULT);

        for (int from = 0; from <= ops.size(); from += WRITE_CHUNK_OPS) {
            boolean last = from + WRITE_CHUNK_OPS >= ops.size();
            List<Op> chunk = new ArrayList<>(ops.subList(from, Math.min(from + WRITE_CHUNK_OPS, ops.size())));
            if (last) {
                chunk.add(headPut);
            }
            TxnResponse response = await(client.getKVClient()
                    .txn()
                    .If(headGuard)
                    .Then(chunk.toArray(Op[]::new))
                    .commit());
            if (!response.isSucceeded()) {
                return false;
            }
            if (last) {
                return true;
            }
        }
        return true;
    }

    private Map<String, List<ShardRouting>> groupRouting(List<ShardRouting> routingTable) {
        Map<String, List<ShardRouting>> result = new LinkedHashMap<>();
        for (ShardRouting routing : routingTable) {
            result.computeIfAbsent(routing.shardId().indexName(), ignored -> new ArrayList<>()).add(routing);
        }
        for (List<ShardRouting> routings : result.values()) {
            routings.sort(Comparator.comparingInt(r -> r.shardId().shardNumber()));
        }
        return result;
    }

    /**
     * Read the head key plus the content prefixes. The head is read FIRST; content pages follow,
     * so an interleaved update can only make the returned content NEWER than the returned head.
     */
    private StateSnapshot readSnapshot() throws Exception {
        KeyValue stateKv = fileKv(key("state"));
        ClusterState state;
        if (stateKv == null) {
            state = emptyState();
        } else {
            byte[] raw = stateKv.getValue().getBytes();
            JsonNode node = JsonUtil.readValue(raw, JsonNode.class);
            if (node.has("indices")) {
                // Legacy layout: full state JSON in the state key. Migrates to split keys on the
                // next update, whose content diff writes every key and the head overwrites this.
                state = JsonUtil.readValue(raw, ClusterState.class);
            } else {
                ClusterStateHead head = JsonUtil.readValue(raw, ClusterStateHead.class);
                Map<String, IndexSettings> indices = readPrefixMap("indices/", IndexSettings.class);
                Map<String, List<ShardRouting>> routingByIndex = readRoutingByIndex();
                Map<String, IndexLifecyclePolicy> policies = readPrefixMap("lifecycle/", IndexLifecyclePolicy.class);
                List<ShardRouting> routing = routingByIndex.values().stream()
                        .flatMap(List::stream)
                        .sorted(Comparator.comparing((ShardRouting r) -> r.shardId().indexName())
                                .thenComparingInt(r -> r.shardId().shardNumber()))
                        .toList();
                state = new ClusterState(
                        head.clusterName(),
                        head.version(),
                        head.masterNodeId(),
                        Map.of(),
                        indices,
                        routing,
                        policies,
                        head.updatedAt()
                );
            }
        }
        Map<String, ClusterNode> nodes = new HashMap<>();
        for (KeyValue kv : await(client.getKVClient()
                .get(nodesPrefix(), GetOption.builder().isPrefix(true).build())).getKvs()) {
            ClusterNode node = JsonUtil.readValue(kv.getValue().getBytes(), ClusterNode.class);
            nodes.put(node.id(), node);
        }
        ClusterState merged = new ClusterState(
                state.clusterName(),
                state.version(),
                state.masterNodeId(),
                nodes,
                state.indices(),
                state.routingTable(),
                state.lifecyclePolicies(),
                state.updatedAt()
        );
        return new StateSnapshot(stateKv, merged);
    }

    private Map<String, List<ShardRouting>> readRoutingByIndex() throws Exception {
        Map<String, List<ShardRouting>> result = new HashMap<>();
        for (KeyValue kv : readPrefix("routing/")) {
            String indexName = suffixOf(kv.getKey(), "routing/");
            List<ShardRouting> routings = new ArrayList<>(List.of(
                    JsonUtil.readValue(kv.getValue().getBytes(), ShardRouting[].class)));
            routings.sort(Comparator.comparingInt(r -> r.shardId().shardNumber()));
            result.put(indexName, routings);
        }
        return result;
    }

    private <T> Map<String, T> readPrefixMap(String prefix, Class<T> type) throws Exception {
        Map<String, T> result = new HashMap<>();
        for (KeyValue kv : readPrefix(prefix)) {
            result.put(suffixOf(kv.getKey(), prefix), JsonUtil.readValue(kv.getValue().getBytes(), type));
        }
        return result;
    }

    /** Paged ascending range read of all keys under a prefix. */
    private List<KeyValue> readPrefix(String prefix) throws Exception {
        List<KeyValue> all = new ArrayList<>();
        ByteSequence start = key(prefix);
        ByteSequence end = prefixEnd(start);
        while (true) {
            // withRange(end) sets the exclusive range end; the start key goes to get().
            GetOption options = GetOption.builder()
                    .withRange(end)
                    .withLimit(READ_PAGE_LIMIT)
                    .withSortField(GetOption.SortTarget.KEY)
                    .withSortOrder(GetOption.SortOrder.ASCEND)
                    .build();
            var response = await(client.getKVClient().get(start, options));
            all.addAll(response.getKvs());
            if (!response.isMore() || response.getKvs().isEmpty()) {
                return all;
            }
            ByteSequence lastKey = response.getKvs().getLast().getKey();
            byte[] next = Arrays.copyOf(lastKey.getBytes(), lastKey.size() + 1);
            start = ByteSequence.from(next);
        }
    }

    /** Exclusive range end covering exactly one prefix (last byte incremented with carry). */
    private static ByteSequence prefixEnd(ByteSequence prefix) {
        byte[] bytes = prefix.getBytes();
        for (int i = bytes.length - 1; i >= 0; i--) {
            if (bytes[i] != (byte) 0xff) {
                byte[] end = Arrays.copyOf(bytes, i + 1);
                end[i]++;
                return ByteSequence.from(end);
            }
        }
        return ByteSequence.from(new byte[0]);
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

    private <T> T await(CompletableFuture<T> future) throws Exception {
        return future.get(operationTimeoutSeconds, TimeUnit.SECONDS);
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

    private String suffixOf(ByteSequence fullKey, String prefix) {
        String full = fullKey.toString(StandardCharsets.UTF_8);
        String marker = namespace + "/" + prefix;
        return full.substring(marker.length());
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
        IOException exception = new IOException(
                causeMessage == null ? message : message + ": " + causeMessage
        );
        exception.initCause(cause);
        return exception;
    }

    private KeyValue fileKv(ByteSequence key) throws Exception {
        var response = await(client.getKVClient().get(key));
        return response.getKvs().isEmpty() ? null : response.getKvs().getFirst();
    }

    private record StateSnapshot(KeyValue stateKv, ClusterState state) {
    }
}
