package com.github.wxk6b1203.store.object;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class S3RemoteObjectStore implements RemoteObjectStore {
    private static final int DELETE_OBJECTS_BATCH_SIZE = 1_000;
    // S3 requires every part except the last to be at least 5 MiB.
    private static final long MIN_PART_SIZE_BYTES = 5L * 1024 * 1024;
    private static final long MAX_PART_COUNT = 10_000;
    private static final long DEFAULT_MULTIPART_MIN_FILE_SIZE = 64L * 1024 * 1024;
    private static final long DEFAULT_MULTIPART_PART_SIZE = 8L * 1024 * 1024;

    private final String bucket;
    private final S3Client s3Client;
    private final long multipartMinFileSizeBytes;
    private final long multipartPartSizeBytes;

    public S3RemoteObjectStore(String bucket, S3Client s3Client) {
        this(bucket, s3Client, DEFAULT_MULTIPART_MIN_FILE_SIZE, DEFAULT_MULTIPART_PART_SIZE);
    }

    public S3RemoteObjectStore(String bucket, S3Client s3Client, long multipartMinFileSizeBytes, long multipartPartSizeBytes) {
        this.bucket = bucket;
        this.s3Client = s3Client;
        this.multipartMinFileSizeBytes = Math.max(0, multipartMinFileSizeBytes);
        this.multipartPartSizeBytes = Math.max(MIN_PART_SIZE_BYTES, multipartPartSizeBytes);
    }

    @Override
    public void put(String key, Path source) throws IOException {
        long started = System.nanoTime();
        try {
            long size = Files.size(source);
            if (isMultipart(size)) {
                putMultipart(key, source, size);
            } else {
                PutObjectRequest request = PutObjectRequest.builder()
                        .bucket(bucket)
                        .key(key)
                        .build();
                s3Client.putObject(request, RequestBody.fromFile(source));
            }
            PUTS.incrementAndGet();
        } catch (RuntimeException e) {
            PUT_ERRORS.incrementAndGet();
            throw e;
        } catch (IOException e) {
            PUT_ERRORS.incrementAndGet();
            throw e;
        } finally {
            PUT_DURATION_NANOS.addAndGet(System.nanoTime() - started);
        }
    }

    private boolean isMultipart(long size) {
        return multipartMinFileSizeBytes > 0 && size >= multipartMinFileSizeBytes;
    }

    /**
     * Multipart upload for large segment files: single PUTs cap out at 5 GiB, offer no
     * parallelism, and restart from byte zero on failure. Parts are retransferrable
     * individually (SDK-level retries), and a failed upload is aborted so no orphan parts
     * are left billing storage.
     */
    private void putMultipart(String key, Path source, long size) throws IOException {
        CreateMultipartUploadResponse create = s3Client.createMultipartUpload(
                CreateMultipartUploadRequest.builder().bucket(bucket).key(key).build());
        String uploadId = create.uploadId();
        boolean completed = false;
        try {
            List<CompletedPart> parts = new ArrayList<>();
            long partSize = effectivePartSize(size);
            int partNumber = 1;
            for (long offset = 0; offset < size; offset += partSize) {
                long length = Math.min(partSize, size - offset);
                // Repeatable body (byte[]): with chunked encoding disabled the SDK must re-read
                // the payload to compute the signature hash, which a bare InputStream cannot do.
                byte[] chunk = new byte[(int) length];
                try (InputStream in = partStream(source, offset, length)) {
                    int read = 0;
                    while (read < length) {
                        int n = in.read(chunk, read, (int) length - read);
                        if (n == -1) {
                            throw new IOException("short read from " + source + " at " + (offset + read));
                        }
                        read += n;
                    }
                }
                UploadPartResponse part = s3Client.uploadPart(
                        UploadPartRequest.builder()
                                .bucket(bucket)
                                .key(key)
                                .uploadId(uploadId)
                                .partNumber(partNumber)
                                .build(),
                        RequestBody.fromBytes(chunk));
                parts.add(CompletedPart.builder().partNumber(partNumber).eTag(part.eTag()).build());
                partNumber++;
            }
            s3Client.completeMultipartUpload(CompleteMultipartUploadRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .uploadId(uploadId)
                    .multipartUpload(CompletedMultipartUpload.builder().parts(parts).build())
                    .build());
            completed = true;
        } finally {
            if (!completed) {
                try {
                    s3Client.abortMultipartUpload(AbortMultipartUploadRequest.builder()
                            .bucket(bucket)
                            .key(key)
                            .uploadId(uploadId)
                            .build());
                } catch (RuntimeException e) {
                    // Best effort: a failing abort must not mask the original upload error.
                }
            }
        }
    }

    private long effectivePartSize(long size) {
        long partSize = multipartPartSizeBytes;
        while (ceilDiv(size, partSize) > MAX_PART_COUNT) {
            partSize *= 2;
        }
        return partSize;
    }

    private static long ceilDiv(long value, long divisor) {
        return (value + divisor - 1) / divisor;
    }

    private static InputStream partStream(Path file, long offset, long length) throws IOException {
        FileChannel channel = FileChannel.open(file, StandardOpenOption.READ);
        channel.position(offset);
        return new InputStream() {
            private long remaining = length;
            private final ByteBuffer buffer = ByteBuffer.allocate(64 * 1024);

            @Override
            public int read() throws IOException {
                byte[] one = new byte[1];
                int read = read(one, 0, 1);
                return read == -1 ? -1 : one[0] & 0xFF;
            }

            @Override
            public int read(byte[] out, int outOffset, int outLength) throws IOException {
                if (remaining <= 0) {
                    return -1;
                }
                buffer.clear();
                buffer.limit((int) Math.min(Math.min(outLength, remaining), buffer.capacity()));
                int read = channel.read(buffer);
                if (read == -1) {
                    return -1;
                }
                buffer.flip();
                buffer.get(out, outOffset, read);
                remaining -= read;
                return read;
            }

            @Override
            public void close() throws IOException {
                channel.close();
            }
        };
    }

    @Override
    public void get(String key, Path target) throws IOException {
        long started = System.nanoTime();
        try {
            Files.createDirectories(target.getParent());
            GetObjectRequest request = GetObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .build();
            s3Client.getObject(request, target);
            GETS.incrementAndGet();
        } catch (IOException | RuntimeException e) {
            GET_ERRORS.incrementAndGet();
            throw e;
        } finally {
            GET_DURATION_NANOS.addAndGet(System.nanoTime() - started);
        }
    }

    @Override
    public void delete(Collection<String> keys) throws IOException {
        if (keys.isEmpty()) {
            return;
        }
        long started = System.nanoTime();
        try {
            List<String> objectKeys = new ArrayList<>(keys);
            int deleted = 0;
            for (int offset = 0; offset < objectKeys.size(); offset += DELETE_OBJECTS_BATCH_SIZE) {
                List<String> batch = objectKeys.subList(
                        offset,
                        Math.min(offset + DELETE_OBJECTS_BATCH_SIZE, objectKeys.size())
                );
                DeleteObjectsResponse response = deleteBatch(batch);
                if (response.hasErrors()) {
                    DELETE_ERRORS.incrementAndGet();
                    String errors = response.errors().stream()
                            .limit(5)
                            .map(error -> error.key() + "=" + error.code() + ":" + error.message())
                            .collect(Collectors.joining(", "));
                    throw new IOException("Failed to delete S3 objects from bucket " + bucket + ": " + errors);
                }
                deleted += batch.size();
            }
            DELETES.addAndGet(deleted);
        } catch (IOException | RuntimeException e) {
            if (!(e instanceof IOException)) {
                DELETE_ERRORS.incrementAndGet();
            }
            throw e;
        } finally {
            DELETE_DURATION_NANOS.addAndGet(System.nanoTime() - started);
        }
    }

    private DeleteObjectsResponse deleteBatch(List<String> keys) {
        DeleteObjectsRequest request = DeleteObjectsRequest.builder()
                .bucket(bucket)
                .delete(delete -> delete.objects(keys.stream()
                        .map(key -> ObjectIdentifier.builder().key(key).build())
                        .toList()))
                .build();
        return s3Client.deleteObjects(request);
    }

    public static Map<String, Object> statsSnapshot() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("puts", PUTS.get());
        stats.put("gets", GETS.get());
        stats.put("deletes", DELETES.get());
        stats.put("put_errors", PUT_ERRORS.get());
        stats.put("get_errors", GET_ERRORS.get());
        stats.put("delete_errors", DELETE_ERRORS.get());
        stats.put("put_duration_seconds_sum", PUT_DURATION_NANOS.get() / 1_000_000_000.0);
        stats.put("get_duration_seconds_sum", GET_DURATION_NANOS.get() / 1_000_000_000.0);
        stats.put("delete_duration_seconds_sum", DELETE_DURATION_NANOS.get() / 1_000_000_000.0);
        return stats;
    }

    private static final AtomicLong PUTS = new AtomicLong();
    private static final AtomicLong GETS = new AtomicLong();
    private static final AtomicLong DELETES = new AtomicLong();
    private static final AtomicLong PUT_ERRORS = new AtomicLong();
    private static final AtomicLong GET_ERRORS = new AtomicLong();
    private static final AtomicLong DELETE_ERRORS = new AtomicLong();
    private static final AtomicLong PUT_DURATION_NANOS = new AtomicLong();
    private static final AtomicLong GET_DURATION_NANOS = new AtomicLong();
    private static final AtomicLong DELETE_DURATION_NANOS = new AtomicLong();
}
