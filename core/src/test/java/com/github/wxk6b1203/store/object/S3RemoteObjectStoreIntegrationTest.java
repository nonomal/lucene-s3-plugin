package com.github.wxk6b1203.store.object;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Runs against a real S3-compatible service (developed against Aliyun OSS). Enable with:
 * <pre>
 *   S3_TEST_BUCKET / S3_TEST_ENDPOINT (region endpoint WITHOUT the bucket prefix)
 *   S3_TEST_REGION / S3_TEST_ACCESS_KEY / S3_TEST_SECRET_KEY
 * </pre>
 */
class S3RemoteObjectStoreIntegrationTest {
    @TempDir
    Path tempDir;

    private S3Client client() {
        return S3Client.builder()
                .region(Region.of(System.getenv("S3_TEST_REGION")))
                .endpointOverride(java.net.URI.create(System.getenv("S3_TEST_ENDPOINT")))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(
                        System.getenv("S3_TEST_ACCESS_KEY"),
                        System.getenv("S3_TEST_SECRET_KEY"))))
                .serviceConfiguration(S3Configuration.builder()
                        .pathStyleAccessEnabled(false)
                        .chunkedEncodingEnabled(false)
                        .build())
                .overrideConfiguration(override -> override.apiCallAttemptTimeout(Duration.ofSeconds(60)))
                .addPlugin(software.amazon.awssdk.services.s3.LegacyMd5Plugin.create())
                .build();
    }

    private String bucket() {
        return System.getenv("S3_TEST_BUCKET");
    }

    @Test
    @EnabledIfEnvironmentVariable(named = "S3_TEST_BUCKET", matches = ".+")
    void multipartRoundTripsThroughRealObjectStore() throws Exception {
        String bucket = bucket();
        S3Client real = client();
        try {
            // Force multipart for everything; 5 MiB parts over a 12 MiB body => 3 parts.
            S3RemoteObjectStore store = new S3RemoteObjectStore(bucket, real, 1, 5L * 1024 * 1024);
            String key = "it/" + UUID.randomUUID() + "/large.bin";
            Path source = tempDir.resolve("large.bin");
            byte[] content = new byte[12 * 1024 * 1024];
            for (int i = 0; i < content.length; i++) {
                content[i] = (byte) (i * 89);
            }
            Files.write(source, content);

            store.put(key, source);

            Path downloaded = tempDir.resolve("downloaded.bin");
            store.get(key, downloaded);
            assertArrayEquals(content, Files.readAllBytes(downloaded));

            store.delete(List.of(key));
            assertThrows(NoSuchKeyException.class, () -> real.headObject(
                    HeadObjectRequest.builder().bucket(bucket).key(key).build()));
        } finally {
            real.close();
        }
    }

    @Test
    @EnabledIfEnvironmentVariable(named = "S3_TEST_BUCKET", matches = ".+")
    void smallFileUsesSinglePutRoundTrip() throws Exception {
        String bucket = bucket();
        S3Client real = client();
        try {
            S3RemoteObjectStore store = new S3RemoteObjectStore(bucket, real, 64L * 1024 * 1024, 8L * 1024 * 1024);
            String key = "it/" + UUID.randomUUID() + "/small.bin";
            Path source = tempDir.resolve("small.bin");
            byte[] content = "lucene-s3 integration".getBytes(java.nio.charset.StandardCharsets.UTF_8);
            Files.write(source, content);

            store.put(key, source);

            Path downloaded = tempDir.resolve("downloaded.bin");
            store.get(key, downloaded);
            assertArrayEquals(content, Files.readAllBytes(downloaded));

            store.delete(List.of(key));
            assertThrows(NoSuchKeyException.class, () -> real.headObject(
                    HeadObjectRequest.builder().bucket(bucket).key(key).build()));
        } finally {
            real.close();
        }
    }

    @Test
    @EnabledIfEnvironmentVariable(named = "S3_TEST_BUCKET", matches = ".+")
    void failedMultipartAbortLeavesNoObjectBehind() throws Exception {
        String bucket = bucket();
        S3Client real = client();
        try {
            // Fail the second part while delegating create/abort/complete to the real store.
            S3Client failing = (S3Client) Proxy.newProxyInstance(
                    S3Client.class.getClassLoader(),
                    new Class<?>[]{S3Client.class},
                    (Object proxy, Method method, Object[] args) -> {
                        if (method.getName().equals("uploadPart")) {
                            UploadPartRequest request = (UploadPartRequest) args[0];
                            if (request.partNumber() == 2) {
                                throw new RuntimeException("injected part failure");
                            }
                            return real.uploadPart(request, (RequestBody) args[1]);
                        }
                        if (method.getName().equals("close")) {
                            return null;
                        }
                        try {
                            return method.invoke(real, args);
                        } catch (java.lang.reflect.InvocationTargetException e) {
                            throw e.getCause() instanceof RuntimeException runtimeException
                                    ? runtimeException
                                    : new RuntimeException(e.getCause());
                        }
                    });

            S3RemoteObjectStore store = new S3RemoteObjectStore(bucket, failing, 1, 5L * 1024 * 1024);
            String key = "it/" + UUID.randomUUID() + "/aborted.bin";
            Path source = tempDir.resolve("aborted.bin");
            Files.write(source, new byte[12 * 1024 * 1024]);

            try {
                store.put(key, source);
                fail("upload should have failed");
            } catch (RuntimeException expected) {
                // expected: injected part failure
            }

            // The abort must have removed the in-progress upload: no object may exist.
            assertThrows(NoSuchKeyException.class, () -> real.headObject(
                    HeadObjectRequest.builder().bucket(bucket).key(key).build()));
        } finally {
            real.close();
        }
    }
}
