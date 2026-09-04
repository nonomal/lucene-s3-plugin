package com.github.wxk6b1203.metadata.provider.etcd;

import com.github.wxk6b1203.errors.StorageException;
import com.github.wxk6b1203.metadata.common.IndexFile;
import com.github.wxk6b1203.metadata.common.IndexCommitSnapshot;
import com.github.wxk6b1203.metadata.common.IndexCommitSnapshotHeader;
import com.github.wxk6b1203.metadata.common.IndexCommitSnapshotPin;
import com.github.wxk6b1203.metadata.common.IndexFileMetadata;
import com.github.wxk6b1203.metadata.common.IndexFileStatus;
import com.github.wxk6b1203.metadata.common.ShardSummary;
import com.github.wxk6b1203.metadata.provider.ManifestMetadataManager;
import com.github.wxk6b1203.util.JsonUtil;
import io.etcd.jetcd.ByteSequence;
import tools.jackson.databind.JsonNode;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.kv.TxnResponse;
import io.etcd.jetcd.op.Cmp;
import io.etcd.jetcd.op.CmpTarget;
import io.etcd.jetcd.op.Op;
import io.etcd.jetcd.options.DeleteOption;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.PutOption;
import lombok.Builder;
import lombok.Data;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class EtcdManifestMetadataManager extends ManifestMetadataManager {
    private static final int MAX_CAS_RETRIES = 32;
    // etcd's default --max-txn-ops is 128; a transaction with more ops is rejected with
    // INVALID_ARGUMENT "too many operations in txn request". Each CAS file costs 2 ops
    // (one Cmp + one Put), each read-only Op.get costs 1 op, so chunk under these limits.
    private static final int MAX_TXN_OPS = 128;
    private static final int CAS_CHUNK_SIZE = MAX_TXN_OPS / 2;
    // Snapshot name chunks: 128 names ~ 3.5 KiB per value; 32 chunk puts (~112 KiB) per txn
    // stays far under both the max-txn-ops and max-request-bytes limits.
    private static final int NAMES_PER_CHUNK = 128;
    private static final int CHUNKS_PER_TXN = 32;

    private final Client client;
    private final String namespace;
    private final long operationTimeoutSeconds;

    @Data
    @Builder
    public static class Options {
        @Builder.Default
        private String namespace = "lucene-s3/cluster/manifest";
        @Builder.Default
        private long operationTimeoutSeconds = 10;
    }

    public EtcdManifestMetadataManager(Options options, Client client) {
        this.client = Objects.requireNonNull(client, "client");
        this.namespace = normalize(options == null ? null : options.namespace);
        this.operationTimeoutSeconds = Math.max(1, options == null ? 10 : options.operationTimeoutSeconds);
    }

    private <T> T await(java.util.concurrent.CompletableFuture<T> future) throws Exception {
        try {
            return future.get(operationTimeoutSeconds, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            future.cancel(true);
            throw e;
        }
    }

    @Override
    public IndexFileMetadata commitFile(IndexFile file) {
        ByteSequence key = fileKey(file.indexName(), file.name());
        for (int attempt = 0; attempt < MAX_CAS_RETRIES; attempt++) {
            try {
                KeyValue currentKv = fileKv(key);
                IndexFileMetadata current = currentKv == null ? null : decodeFileMetadata(currentKv);
                long epoch = current == null ? 1 : current.getEpoch() + 1;
                IndexFileMetadata metadata = new IndexFileMetadata(
                        file.indexName(),
                        file.name(),
                        file.dataDirectory(),
                        file.objectKey(),
                        epoch,
                        file.size(),
                        file.checksum(),
                        file.modifiedTime(),
                        IndexFileStatus.DIRTY
                );
                // The summary increment rides in the same transaction as the file write so the
                // pending count cannot drift from the entries it counts.
                boolean alreadyPending = current != null
                        && (current.getStatus() == IndexFileStatus.DIRTY
                                || current.getStatus() == IndexFileStatus.UPLOADING);
                SummaryVersion summary = summaryVersion(file.indexName());
                ShardSummary base = summary.summary() == null ? ShardSummary.empty() : summary.summary();
                ShardSummary updatedSummary = new ShardSummary(
                        base.pendingCount() + (alreadyPending ? 0 : 1),
                        base.latestGeneration(),
                        System.currentTimeMillis());
                ByteSequence summaryKey = shardStateKey(file.indexName());
                TxnResponse response = await(client.getKVClient().txn()
                        .If(
                                currentKv == null
                                        ? new Cmp(key, Cmp.Op.EQUAL, CmpTarget.version(0))
                                        : new Cmp(key, Cmp.Op.EQUAL, CmpTarget.modRevision(currentKv.getModRevision())),
                                summaryGuard(summaryKey, summary)
                        )
                        .Then(
                                Op.put(key, ByteSequence.from(JsonUtil.writeValueAsBytes(metadata)), PutOption.DEFAULT),
                                Op.put(summaryKey, ByteSequence.from(JsonUtil.writeValueAsBytes(updatedSummary)), PutOption.DEFAULT)
                        )
                        .commit());
                if (response.isSucceeded()) {
                    return metadata;
                }
            } catch (Exception e) {
                throw storageException("Failed to commit file metadata: " + file.indexName() + "/" + file.name(), e);
            }
        }
        throw new StorageException("Failed to commit file metadata after CAS retries: "
                + file.indexName() + "/" + file.name());
    }

    @Override
    public List<IndexFileMetadata> commitFiles(List<IndexFile> files) {
        if (files.isEmpty()) {
            return List.of();
        }
        String indexName = files.getFirst().indexName();
        for (IndexFile file : files) {
            if (!indexName.equals(file.indexName())) {
                throw new IllegalArgumentException("commitFiles must target a single physical index");
            }
        }
        for (int attempt = 0; attempt < MAX_CAS_RETRIES; attempt++) {
            try {
                // Read only the files in this commit (one batched read-only txn) instead of a
                // full prefix range get. CLEAN file metadata accumulates unbounded across the
                // shard's lifetime (reclaimed only on whole-index delete), so a prefix read would
                // decode the entire history on every write.
                List<String> fileNames = new ArrayList<>(files.size());
                for (IndexFile file : files) {
                    fileNames.add(file.name());
                }
                Map<String, KeyValue> current = fileKvsByNames(indexName, fileNames);
                SummaryVersion summary = summaryVersion(indexName);
                List<IndexFileMetadata> result = new ArrayList<>(files.size());
                List<BatchCommitEntry> toCommit = new ArrayList<>();
                for (IndexFile file : files) {
                    KeyValue currentKv = current.get(file.name());
                    IndexFileMetadata existing = currentKv == null ? null : decodeFileMetadata(currentKv);
                    if (existing != null && sameUpload(existing, file)) {
                        // Same name AND same content: skip only when already durable remotely; otherwise
                        // reuse the existing metadata so the upload state machine continues.
                        if (existing.getStatus() == IndexFileStatus.CLEAN
                                || existing.getStatus() == IndexFileStatus.PINNED) {
                            result.add(null);
                        } else {
                            result.add(existing);
                        }
                        continue;
                    }
                    // Same name but different content (divergent owner histories colliding on one segment
                    // name after failover) must re-commit with a bumped epoch. A CLEAN-status skip alone
                    // would keep the stale objectKey published and corrupt the new history line.
                    long epoch = existing == null ? 1 : existing.getEpoch() + 1;
                    IndexFileMetadata metadata = new IndexFileMetadata(
                            file.indexName(),
                            file.name(),
                            file.dataDirectory(),
                            file.objectKey(),
                            epoch,
                            file.size(),
                            file.checksum(),
                            file.modifiedTime(),
                            IndexFileStatus.DIRTY
                    );
                    toCommit.add(new BatchCommitEntry(file, currentKv, metadata));
                    result.add(metadata);
                }
                if (toCommit.isEmpty()) {
                    return result;
                }
                // Chunk the CAS transaction under etcd's max-txn-ops. Chunks are not mutually
                // atomic: if a later chunk's CAS fails (an upload flipped a status), the whole
                // loop re-reads and retries — already-committed files re-resolve to sameUpload
                // skips, so the retry converges to a consistent state. This matches the prior
                // per-file CAS path, which was never cross-file atomic either.
                boolean allApplied = true;
                for (int from = 0; from < toCommit.size(); from += CAS_CHUNK_SIZE) {
                    int to = Math.min(from + CAS_CHUNK_SIZE, toCommit.size());
                    boolean lastChunk = to == toCommit.size();
                    // The pending-count increment rides on the last chunk's transaction so the
                    // shard summary moves atomically with the files it counts.
                    if (!putAllIfCurrent(toCommit.subList(from, to),
                            lastChunk ? summary : null, toCommit.size())) {
                        allApplied = false;
                        break;
                    }
                }
                if (allApplied) {
                    return result;
                }
                // CAS failed: re-read and retry.
            } catch (Exception e) {
                throw storageException("Failed to commit file metadata batch: " + indexName, e);
            }
        }
        throw new StorageException("Failed to commit file metadata batch after CAS retries: " + indexName);
    }

    @Override
    public ShardSummary shardSummary(String indexName) {
        try {
            return summaryVersion(indexName).summary();
        } catch (Exception e) {
            throw storageException("Failed to read shard summary: " + indexName, e);
        }
    }

    @Override
    public void putShardSummary(String indexName, ShardSummary summary) {
        try {
            await(client.getKVClient().put(shardStateKey(indexName),
                    ByteSequence.from(JsonUtil.writeValueAsBytes(summary))));
        } catch (Exception e) {
            throw storageException("Failed to write shard summary: " + indexName, e);
        }
    }

    @Override
    public Map<String, ShardSummary> shardSummaries() {
        try {
            var response = await(client.getKVClient()
                    .get(shardStatePrefix(), GetOption.builder().isPrefix(true).build()));
            Map<String, ShardSummary> result = new HashMap<>();
            for (KeyValue kv : response.getKvs()) {
                String key = kv.getKey().toString(StandardCharsets.UTF_8);
                String marker = namespace + "/shard_state/";
                if (!key.startsWith(marker)) {
                    continue;
                }
                result.put(key.substring(marker.length()), decodeSummary(kv));
            }
            return result;
        } catch (Exception e) {
            throw storageException("Failed to list shard summaries", e);
        }
    }

    private ShardSummary decodeSummary(KeyValue kv) {
        return JsonUtil.readValue(kv.getValue().getBytes(), ShardSummary.class);
    }

    private SummaryVersion summaryVersion(String indexName) throws Exception {
        KeyValue kv = fileKv(shardStateKey(indexName));
        return kv == null
                ? new SummaryVersion(null, 0)
                : new SummaryVersion(decodeSummary(kv), kv.getModRevision());
    }

    /** Stored shard summary together with its revision for CAS guards (revision 0 = absent). */
    private record SummaryVersion(ShardSummary summary, long modRevision) {
    }

    @Override
    public Map<String, FileVersion> fileVersionsByName(String indexName, Collection<String> names) {
        try {
            Map<String, KeyValue> kvs = fileKvsByNames(indexName, new ArrayList<>(names));
            Map<String, FileVersion> result = new HashMap<>(kvs.size());
            kvs.forEach((name, kv) -> result.put(name, new FileVersion(name, decodeFileMetadata(kv), kv.getModRevision())));
            return result;
        } catch (Exception e) {
            throw storageException("Failed to batch-read file versions: " + indexName, e);
        }
    }

    @Override
    public StatusFlipOutcome compareAndSetStatus(String indexName, FileVersion expected, IndexFileStatus to) {
        ByteSequence key = fileKey(indexName, expected.fileName());
        FileVersion current = expected;
        for (int attempt = 0; attempt < MAX_CAS_RETRIES; attempt++) {
            try {
                if (current.modRevision() < 0) {
                    // No revision available (e.g. caller read through the default base-class path):
                    // fetch it first so the CAS below stays a single round trip.
                    KeyValue kv = fileKv(key);
                    if (kv == null) {
                        return new StatusFlipOutcome(false, null);
                    }
                    current = new FileVersion(expected.fileName(), decodeFileMetadata(kv), kv.getModRevision());
                    if (current.metadata().getEpoch() != expected.metadata().getEpoch()) {
                        return new StatusFlipOutcome(false, current);
                    }
                }
                IndexFileMetadata updated = copyWithStatus(current.metadata(), to);
                TxnResponse response = flipTxn(indexName, key, current.modRevision(),
                        ByteSequence.from(JsonUtil.writeValueAsBytes(updated)), to == IndexFileStatus.CLEAN);
                KeyValue afterKv = extractFlipGet(response);
                if (response.isSucceeded()) {
                    FileVersion updatedVersion = afterKv == null
                            ? null
                            : new FileVersion(expected.fileName(), decodeFileMetadata(afterKv), afterKv.getModRevision());
                    return new StatusFlipOutcome(true, updatedVersion);
                }
                if (afterKv == null) {
                    return new StatusFlipOutcome(false, null);
                }
                IndexFileMetadata stored = decodeFileMetadata(afterKv);
                if (stored.getEpoch() != expected.metadata().getEpoch()) {
                    return new StatusFlipOutcome(false, new FileVersion(expected.fileName(), stored, afterKv.getModRevision()));
                }
                if (stored.getStatus() == to) {
                    return new StatusFlipOutcome(true, new FileVersion(expected.fileName(), stored, afterKv.getModRevision()));
                }
                // Concurrent write with the same epoch: retry the CAS against the fresh revision.
                current = new FileVersion(expected.fileName(), stored, afterKv.getModRevision());
            } catch (Exception e) {
                throw storageException("Failed to flip file status: " + indexName + "/" + expected.fileName(), e);
            }
        }
        throw new StorageException("Failed to flip file status after CAS retries: "
                + indexName + "/" + expected.fileName());
    }

    @Override
    public List<StatusFlipOutcome> compareAndSetStatuses(String indexName, List<FileVersion> expected, IndexFileStatus to) {
        if (expected.isEmpty()) {
            return List.of();
        }
        List<StatusFlipOutcome> results = new ArrayList<>(expected.size());
        // 3 ops per entry (Cmp + Put + Get); 2 ops are the minimum for a guarded write, so 42
        // entries fill etcd's 128-op transaction budget.
        int chunkSize = MAX_TXN_OPS / 3;
        for (int from = 0; from < expected.size(); from += chunkSize) {
            List<FileVersion> chunk = expected.subList(from, Math.min(from + chunkSize, expected.size()));
            List<StatusFlipOutcome> chunkResults = flipChunk(indexName, chunk, to);
            results.addAll(chunkResults);
            if (chunkResults.stream().anyMatch(outcome -> !outcome.flipped())) {
                // The chunk txn is all-or-nothing: one stale entry rejects the whole chunk and
                // nothing was applied. Fall back to per-entry CAS (which self-maintains the shard
                // summary) so each outcome is exact for its own entry.
                for (int i = 0; i < chunk.size(); i++) {
                    if (!chunkResults.get(i).flipped()) {
                        results.set(from + i, compareAndSetStatus(indexName, chunk.get(i), to));
                    }
                }
            }
        }
        return results;
    }

    /** All-or-nothing chunk: one txn guards every entry and updates the shard summary once. */
    private List<StatusFlipOutcome> flipChunk(String indexName, List<FileVersion> chunk, IndexFileStatus to) {
        try {
            SummaryVersion summary = summaryVersion(indexName);
            ShardSummary updatedSummary = summaryAfterFlip(summary.summary(), to, chunk.size());

            ByteSequence summaryKey = shardStateKey(indexName);
            List<Cmp> cmps = new ArrayList<>(chunk.size() + 1);
            List<Op> ops = new ArrayList<>(chunk.size() * 2 + 1);
            cmps.add(summaryGuard(summaryKey, summary));
            for (FileVersion version : chunk) {
                ByteSequence key = fileKey(indexName, version.fileName());
                cmps.add(new Cmp(key, Cmp.Op.EQUAL, CmpTarget.modRevision(version.modRevision())));
                ops.add(Op.put(key, ByteSequence.from(JsonUtil.writeValueAsBytes(copyWithStatus(version.metadata(), to))), PutOption.DEFAULT));
                ops.add(Op.get(key, GetOption.DEFAULT));
            }
            ops.add(Op.put(summaryKey, ByteSequence.from(JsonUtil.writeValueAsBytes(updatedSummary)), PutOption.DEFAULT));

            TxnResponse response = await(client.getKVClient().txn().If(cmps.toArray(Cmp[]::new)).Then(ops.toArray(Op[]::new)).commit());
            List<StatusFlipOutcome> outcomes = new ArrayList<>(chunk.size());
            // getGetResponses holds one response per Op.get in order, each carrying the post-write
            // value and its fresh revision for chained flips.
            List<GetResponse> gets = response.getGetResponses();
            boolean succeeded = response.isSucceeded() && gets.size() == chunk.size();
            for (int i = 0; i < chunk.size(); i++) {
                if (!succeeded) {
                    outcomes.add(new StatusFlipOutcome(false, null));
                    continue;
                }
                List<KeyValue> kvs = gets.get(i).getKvs();
                KeyValue afterKv = kvs.isEmpty() ? null : kvs.getFirst();
                outcomes.add(new StatusFlipOutcome(true, afterKv == null
                        ? null
                        : new FileVersion(chunk.get(i).fileName(), decodeFileMetadata(afterKv), afterKv.getModRevision())));
            }
            return outcomes;
        } catch (Exception e) {
            throw storageException("Failed to flip file statuses: " + indexName, e);
        }
    }

    /**
     * Guarded write of one file status plus the shard summary delta (one fewer pending file when
     * the transition lands CLEAN), both in a single transaction.
     */
    private TxnResponse flipTxn(String indexName,
                                ByteSequence key,
                                long expectedModRevision,
                                ByteSequence updatedValue,
                                boolean decrementPending) throws Exception {
        SummaryVersion summary = summaryVersion(indexName);
        ShardSummary updatedSummary = summaryAfterFlip(summary.summary(),
                decrementPending ? IndexFileStatus.CLEAN : IndexFileStatus.UPLOADING, 1);
        ByteSequence summaryKey = shardStateKey(indexName);
        return await(client.getKVClient().txn()
                .If(
                        new Cmp(key, Cmp.Op.EQUAL, CmpTarget.modRevision(expectedModRevision)),
                        summaryGuard(summaryKey, summary)
                )
                .Then(
                        Op.put(key, updatedValue, PutOption.DEFAULT),
                        Op.put(summaryKey, ByteSequence.from(JsonUtil.writeValueAsBytes(updatedSummary)), PutOption.DEFAULT),
                        Op.get(key, GetOption.DEFAULT)
                )
                .Else(Op.get(key, GetOption.DEFAULT))
                .commit());
    }

    /** Summary CAS guard: mod-revision match for existing keys, version-0 (absent) otherwise. */
    private Cmp summaryGuard(ByteSequence summaryKey, SummaryVersion summary) {
        return summary.modRevision() > 0
                ? new Cmp(summaryKey, Cmp.Op.EQUAL, CmpTarget.modRevision(summary.modRevision()))
                : new Cmp(summaryKey, Cmp.Op.EQUAL, CmpTarget.version(0));
    }

    private ShardSummary summaryAfterFlip(ShardSummary summary, IndexFileStatus to, int count) {
        ShardSummary base = summary == null ? ShardSummary.empty() : summary;
        int pending = base.pendingCount();
        if (to == IndexFileStatus.CLEAN) {
            // DIRTY -> UPLOADING keeps the count: files were already counted pending when
            // committed. Only landing CLEAN retires them from the pending total.
            pending = Math.max(0, pending - count);
        }
        return new ShardSummary(pending, base.latestGeneration(), System.currentTimeMillis());
    }

    private KeyValue extractFlipGet(TxnResponse response) {
        // jetcd's TxnResponse.getGetResponses() only contains responses for get ops (puts are
        // omitted), so the single Op.get sits at index 0 in both branches.
        List<GetResponse> getResponses = response.getGetResponses();
        List<KeyValue> afterKvs = getResponses.getFirst().getKvs();
        return afterKvs.isEmpty() ? null : afterKvs.getFirst();
    }

    private IndexFileMetadata copyWithStatus(IndexFileMetadata metadata, IndexFileStatus status) {
        return new IndexFileMetadata(
                metadata.getIndexName(),
                metadata.getName(),
                metadata.getDataDirectory(),
                metadata.getObjectKey(),
                metadata.getEpoch(),
                metadata.getSize(),
                metadata.getChecksum(),
                metadata.getModifiedTime(),
                status
        );
    }

    @Override
    public void deleteFile(String indexName, String name) {
        try {
            KeyValue kv = fileKv(fileKey(indexName, name));
            if (kv != null) {
                deleteKvsIfUnchanged(List.of(kv));
            }
        } catch (Exception e) {
            throw storageException("Failed to delete file metadata: " + indexName + "/" + name, e);
        }
    }

    @Override
    public void deleteFiles(String indexName, Collection<String> names) {
        try {
            Map<String, KeyValue> kvs = fileKvsByNames(indexName, new ArrayList<>(names));
            List<KeyValue> entries = new ArrayList<>(kvs.values());
            for (int from = 0; from < entries.size(); from += CAS_CHUNK_SIZE) {
                deleteKvsIfUnchanged(entries.subList(from, Math.min(from + CAS_CHUNK_SIZE, entries.size())));
            }
        } catch (Exception e) {
            throw storageException("Failed to batch-delete file metadata: " + indexName, e);
        }
    }

    /** Revision-guarded batched delete: one transaction per chunk, skipping entries changed in between. */
    private void deleteKvsIfUnchanged(List<KeyValue> entries) throws Exception {
        if (entries.isEmpty()) {
            return;
        }
        Cmp[] cmps = new Cmp[entries.size()];
        Op[] ops = new Op[entries.size()];
        for (int i = 0; i < entries.size(); i++) {
            KeyValue kv = entries.get(i);
            cmps[i] = new Cmp(kv.getKey(), Cmp.Op.EQUAL, CmpTarget.modRevision(kv.getModRevision()));
            ops[i] = Op.delete(kv.getKey(), DeleteOption.DEFAULT);
        }
        await(client.getKVClient().txn().If(cmps).Then(ops).commit());
    }

    private Map<String, KeyValue> fileKvsByNames(String indexName, List<String> names) throws Exception {
        if (names.isEmpty()) {
            return Map.of();
        }
        // Read-only transaction (no If, only Op.get in Then) executes as a single linearizable
        // snapshot read of the named keys in one round-trip; etcd supports read-only txns.
        // Chunk under max-txn-ops: each Op.get counts as one op. Each chunk is its own snapshot
        // read; for the commit/publish callers this is acceptable because a fresh re-read is
        // already required (async uploads flip statuses), and the CAS re-validates modRevision.
        Map<String, KeyValue> map = new HashMap<>(names.size());
        for (int from = 0; from < names.size(); from += MAX_TXN_OPS) {
            int to = Math.min(from + MAX_TXN_OPS, names.size());
            List<String> chunk = names.subList(from, to);
            Op[] getOps = new Op[chunk.size()];
            for (int i = 0; i < chunk.size(); i++) {
                getOps[i] = Op.get(fileKey(indexName, chunk.get(i)), GetOption.DEFAULT);
            }
            TxnResponse txnResponse = await(client.getKVClient().txn().Then(getOps).commit());
            List<GetResponse> getResponses = txnResponse.getGetResponses();
            for (int i = 0; i < chunk.size(); i++) {
                List<KeyValue> kvs = getResponses.get(i).getKvs();
                if (!kvs.isEmpty()) {
                    map.put(chunk.get(i), kvs.getFirst());
                }
            }
        }
        return map;
    }

    @Override
    public Map<String, IndexFileMetadata> filesByName(String indexName, Collection<String> names) {
        if (names.isEmpty()) {
            return Map.of();
        }
        try {
            List<String> ordered = new ArrayList<>(names);
            Map<String, KeyValue> kvs = fileKvsByNames(indexName, ordered);
            Map<String, IndexFileMetadata> result = new HashMap<>(kvs.size());
            for (Map.Entry<String, KeyValue> entry : kvs.entrySet()) {
                result.put(entry.getKey(), decodeFileMetadata(entry.getValue()));
            }
            return result;
        } catch (Exception e) {
            throw storageException("Failed to batch-read file metadata: " + indexName, e);
        }
    }

    private boolean putAllIfCurrent(List<BatchCommitEntry> entries, SummaryVersion summary, int pendingDelta) throws Exception {
        ByteSequence summaryKey = shardStateKey(entries.getFirst().file.indexName());
        Cmp[] cmps = new Cmp[entries.size() + (summary == null ? 0 : 1)];
        List<Op> ops = new ArrayList<>(entries.size() + 1);
        for (int i = 0; i < entries.size(); i++) {
            BatchCommitEntry entry = entries.get(i);
            ByteSequence key = fileKey(entry.file.indexName(), entry.file.name());
            cmps[i] = entry.currentKv == null
                    ? new Cmp(key, Cmp.Op.EQUAL, CmpTarget.version(0))
                    : new Cmp(key, Cmp.Op.EQUAL, CmpTarget.modRevision(entry.currentKv.getModRevision()));
            ops.add(Op.put(key, ByteSequence.from(JsonUtil.writeValueAsBytes(entry.metadata)), PutOption.DEFAULT));
        }
        if (summary != null) {
            ShardSummary base = summary.summary() == null ? ShardSummary.empty() : summary.summary();
            ShardSummary updated = new ShardSummary(
                    base.pendingCount() + pendingDelta,
                    base.latestGeneration(),
                    System.currentTimeMillis());
            cmps[entries.size()] = summaryGuard(summaryKey, summary);
            ops.add(Op.put(summaryKey, ByteSequence.from(JsonUtil.writeValueAsBytes(updated)), PutOption.DEFAULT));
        }
        return await(client.getKVClient().txn().If(cmps).Then(ops.toArray(Op[]::new)).commit()).isSucceeded();
    }

    private boolean sameUpload(IndexFileMetadata metadata, IndexFile file) {
        return metadata.getSize() == file.size()
                && metadata.getChecksum() == file.checksum()
                && metadata.getModifiedTime() == file.modifiedTime()
                && Objects.equals(metadata.getObjectKey(), file.objectKey());
    }

    private record BatchCommitEntry(IndexFile file, KeyValue currentKv, IndexFileMetadata metadata) {
    }

    @Override
    public void updateFileStatus(String indexName, String fileName, long epoch, IndexFileStatus status) {
        ByteSequence key = fileKey(indexName, fileName);
        for (int attempt = 0; attempt < MAX_CAS_RETRIES; attempt++) {
            try {
                KeyValue currentKv = fileKv(key);
                if (currentKv == null) {
                    return;
                }
                IndexFileMetadata metadata = decodeFileMetadata(currentKv);
                if (metadata.getEpoch() != epoch || !IndexFileStatus.validTransition(metadata.getStatus(), status)) {
                    return;
                }
                metadata.setStatus(status);
                if (putIfCurrent(key, currentKv, metadata)) {
                    return;
                }
            } catch (Exception e) {
                throw storageException("Failed to update file metadata status: " + indexName + "/" + fileName, e);
            }
        }
        throw new StorageException("Failed to update file metadata status after CAS retries: "
                + indexName + "/" + fileName);
    }

    @Override
    public List<IndexFileMetadata> listAll(String indexName, List<IndexFileStatus> status) {
        try {
            var response = await(client.getKVClient()
                    .get(filePrefix(indexName), GetOption.builder().isPrefix(true).build()));
            List<IndexFileMetadata> result = new ArrayList<>();
            for (KeyValue item : response.getKvs()) {
                IndexFileMetadata metadata = decodeFileMetadata(item);
                if (status.contains(metadata.getStatus())) {
                    result.add(metadata);
                }
            }
            return result;
        } catch (Exception e) {
            throw storageException("Failed to list file metadata: " + indexName, e);
        }
    }

    @Override
    public IndexFileMetadata fileMetadata(String indexName, String name) {
        try {
            KeyValue kv = fileKv(fileKey(indexName, name));
            return kv == null ? null : decodeFileMetadata(kv);
        } catch (Exception e) {
            throw storageException("Failed to get file metadata: " + indexName + "/" + name, e);
        }
    }

    @Override
    public long publishSnapshot(String indexName, String segmentFileName, List<IndexFileMetadata> files) {
        for (int attempt = 0; attempt < MAX_CAS_RETRIES; attempt++) {
            try {
                // Snapshot keys are zero-padded (%020d), so lexicographic descending order equals
                // numeric descending order; a limit-1 descending range get returns the highest
                // generation without decoding every snapshot.
                long generation = latestGeneration(indexName) + 1;
                // The file list is stored as NAME CHUNKS under a separate prefix, not embedded in
                // the header value: a full snapshot serialized into one key hit etcd's 1.5MB
                // request limit at ~6000 files (266 B/file) and permanently broke all commits for
                // the shard. Chunk values stay ~3.5 KiB; the file metadata itself is reassembled
                // from the index_file/ keys at read time (single source of truth).
                List<String> names = files.stream().map(IndexFileMetadata::getName).toList();
                List<List<String>> nameChunks = partition(names, NAMES_PER_CHUNK);
                for (int from = 0; from < nameChunks.size(); from += CHUNKS_PER_TXN) {
                    int to = Math.min(from + CHUNKS_PER_TXN, nameChunks.size());
                    Op[] puts = new Op[to - from];
                    for (int i = from; i < to; i++) {
                        puts[i - from] = Op.put(namesChunkKey(indexName, generation, i),
                                ByteSequence.from(JsonUtil.writeValueAsBytes(nameChunks.get(i))), PutOption.DEFAULT);
                    }
                    await(client.getKVClient().txn().Then(puts).commit());
                }
                IndexCommitSnapshotHeader header = new IndexCommitSnapshotHeader(
                        indexName,
                        generation,
                        segmentFileName,
                        names.size(),
                        nameChunks.size(),
                        System.currentTimeMillis()
                );
                ByteSequence headerKey = snapshotKey(indexName, generation);
                SummaryVersion summary = summaryVersion(indexName);
                ShardSummary updatedSummary = new ShardSummary(
                        summary.summary() == null ? 0 : summary.summary().pendingCount(),
                        generation,
                        System.currentTimeMillis()
                );
                ByteSequence summaryKey = shardStateKey(indexName);
                // Header LAST: the snapshot becomes visible atomically with the summary update;
                // a crash before this leaves unreferenced name chunks that the GC sweep removes.
                boolean created = await(client.getKVClient()
                        .txn()
                        .If(
                                new Cmp(headerKey, Cmp.Op.EQUAL, CmpTarget.version(0)),
                                summaryGuard(summaryKey, summary)
                        )
                        .Then(
                                Op.put(headerKey, ByteSequence.from(JsonUtil.writeValueAsBytes(header)), PutOption.DEFAULT),
                                Op.put(summaryKey, ByteSequence.from(JsonUtil.writeValueAsBytes(updatedSummary)), PutOption.DEFAULT)
                        )
                        .commit())
                        .isSucceeded();
                if (created) {
                    return generation;
                }
            } catch (Exception e) {
                throw storageException("Failed to publish commit snapshot: " + indexName, e);
            }
        }
        throw new StorageException("Failed to publish commit snapshot after CAS retries: " + indexName);
    }

    @Override
    public IndexCommitSnapshotHeader latestSnapshotHeader(String indexName) {
        try {
            KeyValue kv = latestSnapshotKv(indexName);
            return kv == null ? null : decodeHeader(kv);
        } catch (Exception e) {
            throw storageException("Failed to get commit snapshot header: " + indexName, e);
        }
    }

    @Override
    public IndexCommitSnapshotHeader snapshotHeader(String indexName, long generation) {
        try {
            KeyValue kv = fileKv(snapshotKey(indexName, generation));
            return kv == null ? null : decodeHeader(kv);
        } catch (Exception e) {
            throw storageException("Failed to get commit snapshot header: " + indexName + "/" + generation, e);
        }
    }

    @Override
    public IndexCommitSnapshot latestSnapshot(String indexName) {
        try {
            KeyValue kv = latestSnapshotKv(indexName);
            return kv == null ? null : assembleSnapshot(kv);
        } catch (Exception e) {
            throw storageException("Failed to get latest commit snapshot: " + indexName, e);
        }
    }

    private long latestGeneration(String indexName) throws Exception {
        IndexCommitSnapshotHeader header = latestSnapshotHeader(indexName);
        return header == null ? 0 : header.generation();
    }

    private KeyValue latestSnapshotKv(String indexName) throws Exception {
        var response = await(client.getKVClient()
                .get(snapshotPrefix(indexName), GetOption.builder()
                        .isPrefix(true)
                        .withLimit(1)
                        .withSortOrder(GetOption.SortOrder.DESCEND)
                        .withSortField(GetOption.SortTarget.KEY)
                        .build()));
        return response.getKvs().isEmpty() ? null : response.getKvs().getFirst();
    }

    @Override
    public IndexCommitSnapshot snapshot(String indexName, long generation) {
        try {
            KeyValue kv = fileKv(snapshotKey(indexName, generation));
            return kv == null ? null : assembleSnapshot(kv);
        } catch (Exception e) {
            throw storageException("Failed to get commit snapshot: " + indexName + "/" + generation, e);
        }
    }

    @Override
    public List<IndexCommitSnapshot> listSnapshots(String indexName) {
        try {
            var response = await(client.getKVClient()
                    .get(snapshotPrefix(indexName), GetOption.builder().isPrefix(true).build()));
            List<IndexCommitSnapshot> result = new ArrayList<>();
            for (KeyValue item : response.getKvs()) {
                result.add(assembleSnapshot(item));
            }
            return result.stream()
                    .sorted(Comparator.comparingLong(IndexCommitSnapshot::getGeneration))
                    .toList();
        } catch (Exception e) {
            throw storageException("Failed to list commit snapshots: " + indexName, e);
        }
    }

    @Override
    public void deleteSnapshot(String indexName, long generation) {
        try {
            await(client.getKVClient()
                    .txn()
                    .Then(
                            Op.delete(snapshotKey(indexName, generation), DeleteOption.DEFAULT),
                            Op.delete(namesPrefix(indexName, generation),
                                    DeleteOption.builder().isPrefix(true).build())
                    )
                    .commit());
        } catch (Exception e) {
            throw storageException("Failed to delete commit snapshot: " + indexName + "/" + generation, e);
        }
    }

    @Override
    public void deleteOrphanSnapshotFiles(String indexName) {
        try {
            String headerMarker = (namespace + "/snapshot/" + indexName + "/");
            String namesMarker = (namespace + "/snapshot_files/" + indexName + "/");
            Set<Long> liveGenerations = new HashSet<>();
            var headers = await(client.getKVClient()
                    .get(snapshotPrefix(indexName), GetOption.builder()
                            .isPrefix(true)
                            .withKeysOnly(true)
                            .build()));
            for (KeyValue kv : headers.getKvs()) {
                // Header key suffix is "{generation}"; names key suffix is "{generation}/{chunk}".
                liveGenerations.add(generationAfter(kv.getKey().toString(StandardCharsets.UTF_8), headerMarker));
            }
            // Keys only: the sweep runs every GC tick and must not transfer chunk values.
            var names = await(client.getKVClient()
                    .get(namesRootPrefix(indexName), GetOption.builder()
                            .isPrefix(true)
                            .withKeysOnly(true)
                            .build()));
            Set<Long> orphanGenerations = new HashSet<>();
            for (KeyValue kv : names.getKvs()) {
                long generation = generationAfter(kv.getKey().toString(StandardCharsets.UTF_8), namesMarker);
                if (!liveGenerations.contains(generation)) {
                    orphanGenerations.add(generation);
                }
            }
            for (Long generation : orphanGenerations) {
                await(client.getKVClient().delete(namesPrefix(indexName, generation),
                        DeleteOption.builder().isPrefix(true).build()));
            }
        } catch (Exception e) {
            throw storageException("Failed to sweep orphan snapshot files: " + indexName, e);
        }
    }

    /** First path segment after the marker: the generation (chunk key or header key alike). */
    private static long generationAfter(String fullKey, String marker) {
        String relative = fullKey.substring(marker.length());
        int slash = relative.indexOf('/');
        return Long.parseLong(slash < 0 ? relative : relative.substring(0, slash));
    }

    @Override
    public void pinSnapshot(String indexName, long generation, String pinId, long expiresAtMillis) {
        try {
            IndexCommitSnapshotPin pin = new IndexCommitSnapshotPin(indexName, generation, pinId, expiresAtMillis);
            await(client.getKVClient()
                    .put(pinKey(indexName, pinId), ByteSequence.from(JsonUtil.writeValueAsBytes(pin))));
        } catch (Exception e) {
            throw storageException("Failed to pin commit snapshot: " + indexName + "/" + generation, e);
        }
    }

    @Override
    public void releaseSnapshotPin(String indexName, String pinId) {
        try {
            await(client.getKVClient().delete(pinKey(indexName, pinId)));
        } catch (Exception e) {
            throw storageException("Failed to release commit snapshot pin: " + indexName + "/" + pinId, e);
        }
    }

    @Override
    public List<IndexCommitSnapshotPin> snapshotPins(String indexName) {
        try {
            var response = await(client.getKVClient()
                    .get(pinPrefix(indexName), GetOption.builder().isPrefix(true).build()));
            List<IndexCommitSnapshotPin> result = new ArrayList<>();
            for (KeyValue item : response.getKvs()) {
                result.add(decodeSnapshotPin(item));
            }
            return result;
        } catch (Exception e) {
            throw storageException("Failed to list commit snapshot pins: " + indexName, e);
        }
    }

    @Override
    public void deleteExpiredSnapshotPins(long nowMillis) {
        try {
            var response = await(client.getKVClient()
                    .get(key("snapshot_pin/"), GetOption.builder().isPrefix(true).build()));
            List<KeyValue> expired = new ArrayList<>();
            for (KeyValue item : response.getKvs()) {
                IndexCommitSnapshotPin pin = decodeSnapshotPin(item);
                if (pin.getExpiresAtMillis() <= nowMillis) {
                    expired.add(item);
                }
            }
            for (int from = 0; from < expired.size(); from += CAS_CHUNK_SIZE) {
                deleteKvsIfUnchanged(expired.subList(from, Math.min(from + CAS_CHUNK_SIZE, expired.size())));
            }
        } catch (Exception e) {
            throw storageException("Failed to delete expired commit snapshot pins", e);
        }
    }

    @Override
    public void deleteByStatus(String indexName, List<IndexFileStatus> statuses) {
        try {
            var response = await(client.getKVClient()
                    .get(filePrefix(indexName), GetOption.builder().isPrefix(true).build()));
            List<KeyValue> matching = new ArrayList<>();
            for (KeyValue item : response.getKvs()) {
                IndexFileMetadata metadata = decodeFileMetadata(item);
                if (statuses.contains(metadata.getStatus())) {
                    matching.add(item);
                }
            }
            for (int from = 0; from < matching.size(); from += CAS_CHUNK_SIZE) {
                deleteKvsIfUnchanged(matching.subList(from, Math.min(from + CAS_CHUNK_SIZE, matching.size())));
            }
            if (matching.isEmpty()) {
                return;
            }
            boolean removedPending = statuses.contains(IndexFileStatus.DIRTY)
                    || statuses.contains(IndexFileStatus.UPLOADING);
            if (removedPending) {
                // discardPendingUploads removed every pending entry; record an exact zero so the
                // summary cannot keep readiness pinned to owner-only reads.
                SummaryVersion summary = summaryVersion(indexName);
                ShardSummary base = summary.summary() == null ? ShardSummary.empty() : summary.summary();
                await(client.getKVClient().txn()
                        .If(summaryGuard(shardStateKey(indexName), summary))
                        .Then(Op.put(shardStateKey(indexName), ByteSequence.from(JsonUtil.writeValueAsBytes(
                                new ShardSummary(0, base.latestGeneration(), System.currentTimeMillis()))), PutOption.DEFAULT))
                        .commit());
            }
        } catch (Exception e) {
            throw storageException("Failed to delete file metadata by status: " + indexName, e);
        }
    }

    @Override
    public void deleteAll(String indexName) {
        try {
            await(client.getKVClient()
                    .delete(filePrefix(indexName), DeleteOption.builder().isPrefix(true).build()));
            await(client.getKVClient()
                    .delete(snapshotPrefix(indexName), DeleteOption.builder().isPrefix(true).build()));
            await(client.getKVClient()
                    .delete(namesRootPrefix(indexName), DeleteOption.builder().isPrefix(true).build()));
            await(client.getKVClient()
                    .delete(pinPrefix(indexName), DeleteOption.builder().isPrefix(true).build()));
            await(client.getKVClient()
                    .delete(shardStateKey(indexName)));
        } catch (Exception e) {
            throw storageException("Failed to delete file metadata: " + indexName, e);
        }
    }

    private KeyValue fileKv(ByteSequence key) throws Exception {
        var response = await(client.getKVClient().get(key));
        return response.getKvs().isEmpty() ? null : response.getKvs().getFirst();
    }

    private boolean putIfCurrent(ByteSequence key, KeyValue currentKv, IndexFileMetadata metadata) throws Exception {
        Cmp cmp = currentKv == null
                ? new Cmp(key, Cmp.Op.EQUAL, CmpTarget.version(0))
                : new Cmp(key, Cmp.Op.EQUAL, CmpTarget.modRevision(currentKv.getModRevision()));
        return await(client.getKVClient()
                .txn()
                .If(cmp)
                .Then(Op.put(key, ByteSequence.from(JsonUtil.writeValueAsBytes(metadata)), PutOption.DEFAULT))
                .commit())
                .isSucceeded();
    }

    private boolean deleteIfCurrent(KeyValue currentKv) throws Exception {
        Cmp cmp = new Cmp(currentKv.getKey(), Cmp.Op.EQUAL, CmpTarget.modRevision(currentKv.getModRevision()));
        return await(client.getKVClient()
                .txn()
                .If(cmp)
                .Then(Op.delete(currentKv.getKey(), DeleteOption.DEFAULT))
                .commit())
                .isSucceeded();
    }

    /**
     * Rebuild the full snapshot from its header + name chunks + live index_file/ entries. A
     * header without a "fileCount" field is a legacy full-snapshot value (pre-chunking format)
     * and is returned as-is.
     */
    private IndexCommitSnapshot assembleSnapshot(KeyValue headerKv) throws Exception {
        byte[] raw = headerKv.getValue().getBytes();
        JsonNode node = JsonUtil.readValue(raw, JsonNode.class);
        if (node.has("files")) {
            return JsonUtil.readValue(raw, IndexCommitSnapshot.class);
        }
        IndexCommitSnapshotHeader header = JsonUtil.readValue(raw, IndexCommitSnapshotHeader.class);
        List<String> names = new ArrayList<>(header.fileCount());
        var response = await(client.getKVClient()
                .get(namesPrefix(header.indexName(), header.generation()),
                        GetOption.builder()
                                .isPrefix(true)
                                .withSortField(GetOption.SortTarget.KEY)
                                .withSortOrder(GetOption.SortOrder.ASCEND)
                                .build()));
        for (KeyValue kv : response.getKvs()) {
            names.addAll(List.of(JsonUtil.readValue(kv.getValue().getBytes(), String[].class)));
        }
        Map<String, IndexFileMetadata> byName = filesByName(header.indexName(), names);
        List<IndexFileMetadata> files = new ArrayList<>(names.size());
        for (String name : names) {
            IndexFileMetadata metadata = byName.get(name);
            if (metadata == null) {
                throw new StorageException("Snapshot " + header.indexName() + "/" + header.generation()
                        + " references missing file metadata: " + name);
            }
            files.add(copyFileMetadata(metadata));
        }
        return new IndexCommitSnapshot(header.indexName(), header.generation(), header.segmentFileName(), files, header.createdAtMillis());
    }

    private IndexCommitSnapshotHeader decodeHeader(KeyValue kv) {
        byte[] raw = kv.getValue().getBytes();
        JsonNode node = JsonUtil.readValue(raw, JsonNode.class);
        if (node.has("files")) {
            return IndexCommitSnapshotHeader.of(JsonUtil.readValue(raw, IndexCommitSnapshot.class));
        }
        return JsonUtil.readValue(raw, IndexCommitSnapshotHeader.class);
    }

    private static <T> List<List<T>> partition(List<T> list, int size) {
        List<List<T>> chunks = new ArrayList<>();
        for (int from = 0; from < list.size(); from += size) {
            chunks.add(list.subList(from, Math.min(from + size, list.size())));
        }
        return chunks;
    }

    private IndexFileMetadata decodeFileMetadata(KeyValue kv) {
        return JsonUtil.readValue(kv.getValue().getBytes(), IndexFileMetadata.class);
    }

    private IndexCommitSnapshot decodeSnapshot(KeyValue kv) {
        return JsonUtil.readValue(kv.getValue().getBytes(), IndexCommitSnapshot.class);
    }

    private IndexCommitSnapshotPin decodeSnapshotPin(KeyValue kv) {
        return JsonUtil.readValue(kv.getValue().getBytes(), IndexCommitSnapshotPin.class);
    }

    private IndexFileMetadata copyFileMetadata(IndexFileMetadata file) {
        return new IndexFileMetadata(
                file.getIndexName(),
                file.getName(),
                file.getDataDirectory(),
                file.getObjectKey(),
                file.getEpoch(),
                file.getSize(),
                file.getChecksum(),
                file.getModifiedTime(),
                file.getStatus()
        );
    }

    private ByteSequence filePrefix(String indexName) {
        return key("index_file/" + indexName + "/");
    }

    private ByteSequence fileKey(String indexName, String fileName) {
        return key("index_file/" + indexName + "/" + fileName);
    }

    private ByteSequence snapshotPrefix(String indexName) {
        return key("snapshot/" + indexName + "/");
    }

    private ByteSequence snapshotKey(String indexName, long generation) {
        return key("snapshot/" + indexName + "/" + String.format("%020d", generation));
    }

    private ByteSequence namesRootPrefix(String indexName) {
        return key("snapshot_files/" + indexName + "/");
    }

    private ByteSequence namesPrefix(String indexName, long generation) {
        return key("snapshot_files/" + indexName + "/" + String.format("%020d", generation) + "/");
    }

    private ByteSequence namesChunkKey(String indexName, long generation, int chunk) {
        return key("snapshot_files/" + indexName + "/" + String.format("%020d", generation)
                + "/" + String.format("%04d", chunk));
    }

    private ByteSequence pinPrefix(String indexName) {
        return key("snapshot_pin/" + indexName + "/");
    }

    private ByteSequence pinKey(String indexName, String pinId) {
        return key("snapshot_pin/" + indexName + "/" + pinId);
    }

    private ByteSequence shardStateKey(String indexName) {
        return key("shard_state/" + indexName);
    }

    private ByteSequence shardStatePrefix() {
        return key("shard_state/");
    }

    private ByteSequence key(String suffix) {
        return ByteSequence.from((namespace + "/" + suffix).getBytes(StandardCharsets.UTF_8));
    }

    private String normalize(String value) {
        String normalized = value == null || value.isBlank() ? "lucene-s3/cluster/manifest" : value;
        while (normalized.startsWith("/")) {
            normalized = normalized.substring(1);
        }
        while (normalized.endsWith("/")) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        return "/" + normalized;
    }

    private StorageException storageException(String message, Exception cause) {
        String causeMessage = cause.getMessage();
        StorageException exception = new StorageException(
                causeMessage == null ? message : message + ": " + causeMessage
        );
        exception.initCause(cause);
        return exception;
    }
}
