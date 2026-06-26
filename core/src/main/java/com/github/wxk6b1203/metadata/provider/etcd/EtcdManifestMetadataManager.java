package com.github.wxk6b1203.metadata.provider.etcd;

import com.github.wxk6b1203.errors.StorageException;
import com.github.wxk6b1203.metadata.common.IndexFile;
import com.github.wxk6b1203.metadata.common.IndexCommitSnapshot;
import com.github.wxk6b1203.metadata.common.IndexCommitSnapshotPin;
import com.github.wxk6b1203.metadata.common.IndexFileMetadata;
import com.github.wxk6b1203.metadata.common.IndexFileStatus;
import com.github.wxk6b1203.metadata.provider.ManifestMetadataManager;
import com.github.wxk6b1203.util.JsonUtil;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KeyValue;
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
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class EtcdManifestMetadataManager extends ManifestMetadataManager {
    private static final int MAX_CAS_RETRIES = 32;

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
    public int commitFile(IndexFile file) {
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
                if (putIfCurrent(key, currentKv, metadata)) {
                    return Math.toIntExact(epoch);
                }
            } catch (Exception e) {
                throw storageException("Failed to commit file metadata: " + file.indexName() + "/" + file.name(), e);
            }
        }
        throw new StorageException("Failed to commit file metadata after CAS retries: "
                + file.indexName() + "/" + file.name());
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
                long generation = listSnapshots(indexName).stream()
                        .mapToLong(IndexCommitSnapshot::getGeneration)
                        .max()
                        .orElse(0) + 1;
                IndexCommitSnapshot snapshot = new IndexCommitSnapshot(
                        indexName,
                        generation,
                        segmentFileName,
                        files.stream().map(this::copyFileMetadata).toList(),
                        System.currentTimeMillis()
                );
                ByteSequence key = snapshotKey(indexName, generation);
                boolean created = await(client.getKVClient()
                        .txn()
                        .If(new Cmp(key, Cmp.Op.EQUAL, CmpTarget.version(0)))
                        .Then(Op.put(key, ByteSequence.from(JsonUtil.writeValueAsBytes(snapshot)), PutOption.DEFAULT))
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
    public IndexCommitSnapshot latestSnapshot(String indexName) {
        return listSnapshots(indexName).stream()
                .max(Comparator.comparingLong(IndexCommitSnapshot::getGeneration))
                .orElse(null);
    }

    @Override
    public IndexCommitSnapshot snapshot(String indexName, long generation) {
        try {
            var response = await(client.getKVClient().get(snapshotKey(indexName, generation)));
            return response.getKvs().isEmpty() ? null : decodeSnapshot(response.getKvs().getFirst());
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
                result.add(decodeSnapshot(item));
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
            await(client.getKVClient().delete(snapshotKey(indexName, generation)));
        } catch (Exception e) {
            throw storageException("Failed to delete commit snapshot: " + indexName + "/" + generation, e);
        }
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
            for (KeyValue item : response.getKvs()) {
                IndexCommitSnapshotPin pin = decodeSnapshotPin(item);
                if (pin.getExpiresAtMillis() <= nowMillis) {
                    deleteIfCurrent(item);
                }
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
            for (KeyValue item : response.getKvs()) {
                IndexFileMetadata metadata = decodeFileMetadata(item);
                if (statuses.contains(metadata.getStatus())) {
                    deleteIfCurrent(item);
                }
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
                    .delete(pinPrefix(indexName), DeleteOption.builder().isPrefix(true).build()));
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

    private ByteSequence pinPrefix(String indexName) {
        return key("snapshot_pin/" + indexName + "/");
    }

    private ByteSequence pinKey(String indexName, String pinId) {
        return key("snapshot_pin/" + indexName + "/" + pinId);
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
