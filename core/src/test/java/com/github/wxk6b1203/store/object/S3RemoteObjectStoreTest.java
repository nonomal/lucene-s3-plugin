package com.github.wxk6b1203.store.object;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;

import java.io.InputStream;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class S3RemoteObjectStoreTest {
    @TempDir
    Path tempDir;

    @Test
    void deleteSplitsMultiObjectDeleteIntoS3SizedBatches() throws Exception {
        List<Integer> batchSizes = new ArrayList<>();
        S3Client s3Client = s3Client(batchSizes);
        S3RemoteObjectStore store = new S3RemoteObjectStore("bucket", s3Client);
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < 2_501; i++) {
            keys.add("index/_data/file-" + i);
        }

        store.delete(keys);

        assertEquals(List.of(1_000, 1_000, 501), batchSizes);
    }

    @Test
    void smallFilesUseSinglePutInsteadOfMultipart() throws Exception {
        RecordingS3Client s3 = new RecordingS3Client();
        // Threshold 1 byte would force multipart for everything; use 64 MiB default-like config.
        S3RemoteObjectStore store = new S3RemoteObjectStore("bucket", s3.proxy(), 64L * 1024 * 1024, 8L * 1024 * 1024);
        Path file = tempDir.resolve("small.bin");
        Files.write(file, new byte[1024]);

        store.put("index/_data/small", file);

        assertEquals(1, s3.putObjectCalls);
        assertEquals(0, s3.createCalls.size());
    }

    @Test
    void largeFilesUploadMultipartPartsWithExactBoundaries() throws Exception {
        RecordingS3Client s3 = new RecordingS3Client();
        long partSize = 5L * 1024 * 1024;
        S3RemoteObjectStore store = new S3RemoteObjectStore("bucket", s3.proxy(), 1, partSize);
        Path file = tempDir.resolve("large.bin");
        byte[] content = new byte[12 * 1024 * 1024];
        for (int i = 0; i < content.length; i++) {
            content[i] = (byte) (i * 31);
        }
        Files.write(file, content);

        store.put("index/_data/large", file);

        assertEquals("bucket", s3.createCalls.getFirst().bucket());
        assertEquals(3, s3.parts.size());
        assertPart(s3, 0, 1, content, 0, 5 * 1024 * 1024);
        assertPart(s3, 1, 2, content, 5L * 1024 * 1024, 5 * 1024 * 1024);
        assertPart(s3, 2, 3, content, 10L * 1024 * 1024, 2 * 1024 * 1024);
        assertEquals(1, s3.completedCalls.size());
        assertEquals("upload-1", s3.completedCalls.getFirst().uploadId());
        assertEquals(3, s3.completedCalls.getFirst().multipartUpload().parts().size());
        assertEquals(0, s3.abortCalls.size());
    }

    private void assertPart(RecordingS3Client s3, int index, int partNumber,
                            byte[] content, long offset, int length) throws Exception {
        UploadPartRequest request = s3.parts.get(index).request();
        assertEquals(partNumber, request.partNumber());
        assertEquals("upload-1", request.uploadId());
        byte[] uploaded = s3.parts.get(index).body();
        assertEquals(length, uploaded.length);
        for (int i = 0; i < length; i++) {
            if (uploaded[i] != content[(int) (offset + i)]) {
                throw new AssertionError("part " + partNumber + " byte " + i + " mismatch");
            }
        }
    }

    @Test
    void failedMultipartUploadIsAbortedWithoutComplete() throws Exception {
        RecordingS3Client s3 = new RecordingS3Client();
        s3.failPartNumber(2);
        S3RemoteObjectStore store = new S3RemoteObjectStore("bucket", s3.proxy(), 1, 5L * 1024 * 1024);
        Path file = tempDir.resolve("large.bin");
        Files.write(file, new byte[12 * 1024 * 1024]);

        try {
            store.put("index/_data/large", file);
            org.junit.jupiter.api.Assertions.fail("upload should have failed");
        } catch (RuntimeException expected) {
            // expected: injected part failure
        }

        assertEquals(1, s3.abortCalls.size());
        assertEquals("upload-1", s3.abortCalls.getFirst().uploadId());
        assertEquals(0, s3.completedCalls.size());
    }

    private S3Client s3Client(List<Integer> batchSizes) {
        InvocationHandler handler = (Object proxy, Method method, Object[] args) -> {
            if (method.getName().equals("deleteObjects")) {
                DeleteObjectsRequest request = (DeleteObjectsRequest) args[0];
                batchSizes.add(request.delete().objects().size());
                return DeleteObjectsResponse.builder().build();
            }
            if (method.getName().equals("close")) {
                return null;
            }
            if (method.getDeclaringClass() == Object.class) {
                return method.invoke(this, args);
            }
            throw new UnsupportedOperationException("unexpected S3Client method: " + method.getName());
        };
        return (S3Client) Proxy.newProxyInstance(
                S3Client.class.getClassLoader(),
                new Class<?>[]{S3Client.class},
                handler
        );
    }

    /** Records multipart-related S3Client calls; everything else throws. */
    private static final class RecordingS3Client {
        final List<CreateMultipartUploadRequest> createCalls = new ArrayList<>();
        final List<PartCall> parts = new ArrayList<>();
        final List<CompleteMultipartUploadRequest> completedCalls = new ArrayList<>();
        final List<AbortMultipartUploadRequest> abortCalls = new ArrayList<>();
        int putObjectCalls;
        private volatile int failPartNumber = -1;

        void failPartNumber(int partNumber) {
            this.failPartNumber = partNumber;
        }

        S3Client proxy() {
            InvocationHandler handler = (Object proxy, Method method, Object[] args) -> {
                switch (method.getName()) {
                    case "putObject" -> putObjectCalls++;
                    case "createMultipartUpload" -> {
                        CreateMultipartUploadRequest request = (CreateMultipartUploadRequest) args[0];
                        createCalls.add(request);
                        return CreateMultipartUploadResponse.builder().uploadId("upload-1").build();
                    }
                    case "uploadPart" -> {
                        UploadPartRequest request = (UploadPartRequest) args[0];
                        if (request.partNumber() == failPartNumber) {
                            throw new RuntimeException("injected part failure");
                        }
                        byte[] body;
                        try (InputStream in = ((RequestBody) args[1]).contentStreamProvider().newStream()) {
                            body = in.readAllBytes();
                        }
                        parts.add(new PartCall(request, body));
                        return UploadPartResponse.builder().eTag("etag-" + request.partNumber()).build();
                    }
                    case "completeMultipartUpload" -> {
                        completedCalls.add((CompleteMultipartUploadRequest) args[0]);
                        return null;
                    }
                    case "abortMultipartUpload" -> abortCalls.add((AbortMultipartUploadRequest) args[0]);
                    case "close" -> {
                        return null;
                    }
                    default -> throw new UnsupportedOperationException("unexpected S3Client method: " + method.getName());
                }
                return null;
            };
            return (S3Client) Proxy.newProxyInstance(
                    S3Client.class.getClassLoader(),
                    new Class<?>[]{S3Client.class},
                    handler
            );
        }
    }

    private record PartCall(UploadPartRequest request, byte[] body) {
    }
}
