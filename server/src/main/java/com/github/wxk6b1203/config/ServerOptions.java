package com.github.wxk6b1203.config;

import com.github.wxk6b1203.cluster.NodeRole;

import java.util.Set;

public record ServerOptions(
        int httpPort,
        String clusterName,
        String nodeId,
        String nodeName,
        String host,
        Set<NodeRole> roles,
        String etcdEndpoints,
        String etcdNamespace,
        String dataPath,
        String s3Bucket,
        String s3Region,
        String s3Protocol,
        String s3Endpoint,
        boolean s3ChunkedEncoding,
        boolean s3ContentMd5,
        String s3AccessKey,
        String s3SecretKey,
        int snapshotRetainLatest,
        int etcdTimeoutSeconds,
        int httpForwardTimeoutSeconds,
        String uploadWaitStrategy,
        int uploadWaitTimeoutSeconds,
        boolean commitEveryRequest,
        int commitIntervalMillis,
        int commitAfterDocs,
        String refreshPolicy,
        int refreshIntervalMillis,
        String analyzerPluginPath,
        long cacheMaxBytes,
        int cacheCleanupIntervalSeconds,
        int metricsPort,
        int maxWriteRequests,
        int maxBulkItems,
        long maxBulkBytes,
        long maxHttpBodyBytes
) {
    public ServerOptions {
        etcdTimeoutSeconds = Math.max(1, etcdTimeoutSeconds);
        httpForwardTimeoutSeconds = Math.max(1, httpForwardTimeoutSeconds);
        uploadWaitStrategy = uploadWaitStrategy == null || uploadWaitStrategy.isBlank()
                ? "async"
                : uploadWaitStrategy.trim();
        uploadWaitTimeoutSeconds = Math.max(1, uploadWaitTimeoutSeconds);
        commitIntervalMillis = Math.max(0, commitIntervalMillis);
        commitAfterDocs = Math.max(0, commitAfterDocs);
        refreshPolicy = refreshPolicy == null || refreshPolicy.isBlank()
                ? "immediate"
                : refreshPolicy.trim();
        refreshIntervalMillis = Math.max(1, refreshIntervalMillis);
        analyzerPluginPath = analyzerPluginPath == null || analyzerPluginPath.isBlank()
                ? null
                : analyzerPluginPath.trim();
        cacheMaxBytes = Math.max(0, cacheMaxBytes);
        cacheCleanupIntervalSeconds = Math.max(1, cacheCleanupIntervalSeconds);
        metricsPort = Math.max(0, metricsPort);
        maxWriteRequests = Math.max(0, maxWriteRequests);
        maxBulkItems = Math.max(0, maxBulkItems);
        maxBulkBytes = Math.max(0, maxBulkBytes);
        maxHttpBodyBytes = Math.max(0, maxHttpBodyBytes);
    }

    public ServerOptions(
            int httpPort,
            String clusterName,
            String nodeId,
            String nodeName,
            String host,
            Set<NodeRole> roles,
            String etcdEndpoints,
            String etcdNamespace,
            String dataPath,
            String s3Bucket,
            String s3Region,
            String s3Protocol,
            String s3Endpoint,
            boolean s3ChunkedEncoding,
            String s3AccessKey,
            String s3SecretKey,
            int snapshotRetainLatest
    ) {
        this(
                httpPort,
                clusterName,
                nodeId,
                nodeName,
                host,
                roles,
                etcdEndpoints,
                etcdNamespace,
                dataPath,
                s3Bucket,
                s3Region,
                s3Protocol,
                s3Endpoint,
                s3ChunkedEncoding,
                false,
                s3AccessKey,
                s3SecretKey,
                snapshotRetainLatest,
                10,
                10,
                "async",
                30,
                true,
                0,
                0,
                "immediate",
                1000,
                null,
                0,
                60,
                0,
                0,
                0,
                0,
                0
        );
    }

    public ServerOptions(
            int httpPort,
            String clusterName,
            String nodeId,
            String nodeName,
            String host,
            Set<NodeRole> roles,
            String etcdEndpoints,
            String etcdNamespace,
            String dataPath,
            String s3Bucket,
            String s3Region,
            String s3Protocol,
            String s3Endpoint,
            boolean s3ChunkedEncoding,
            String s3AccessKey,
            String s3SecretKey,
            int snapshotRetainLatest,
            int etcdTimeoutSeconds
    ) {
        this(
                httpPort,
                clusterName,
                nodeId,
                nodeName,
                host,
                roles,
                etcdEndpoints,
                etcdNamespace,
                dataPath,
                s3Bucket,
                s3Region,
                s3Protocol,
                s3Endpoint,
                s3ChunkedEncoding,
                false,
                s3AccessKey,
                s3SecretKey,
                snapshotRetainLatest,
                etcdTimeoutSeconds,
                10,
                "async",
                30,
                true,
                0,
                0,
                "immediate",
                1000,
                null,
                0,
                60,
                0,
                0,
                0,
                0,
                0
        );
    }

    public ServerOptions(
            int httpPort,
            String clusterName,
            String nodeId,
            String nodeName,
            String host,
            Set<NodeRole> roles,
            String etcdEndpoints,
            String etcdNamespace,
            String dataPath,
            String s3Bucket,
            String s3Region,
            String s3Protocol,
            String s3Endpoint,
            boolean s3ChunkedEncoding,
            String s3AccessKey,
            String s3SecretKey,
            int snapshotRetainLatest,
            int etcdTimeoutSeconds,
            long cacheMaxBytes,
            int cacheCleanupIntervalSeconds
    ) {
        this(
                httpPort,
                clusterName,
                nodeId,
                nodeName,
                host,
                roles,
                etcdEndpoints,
                etcdNamespace,
                dataPath,
                s3Bucket,
                s3Region,
                s3Protocol,
                s3Endpoint,
                s3ChunkedEncoding,
                false,
                s3AccessKey,
                s3SecretKey,
                snapshotRetainLatest,
                etcdTimeoutSeconds,
                10,
                "async",
                30,
                true,
                0,
                0,
                "immediate",
                1000,
                null,
                cacheMaxBytes,
                cacheCleanupIntervalSeconds,
                0,
                0,
                0,
                0,
                0
        );
    }

    public ServerOptions(
            int httpPort,
            String clusterName,
            String nodeId,
            String nodeName,
            String host,
            Set<NodeRole> roles,
            String etcdEndpoints,
            String etcdNamespace,
            String dataPath,
            String s3Bucket,
            String s3Region,
            String s3Protocol,
            String s3Endpoint,
            boolean s3ChunkedEncoding,
            String s3AccessKey,
            String s3SecretKey,
            int snapshotRetainLatest,
            int etcdTimeoutSeconds,
            long cacheMaxBytes,
            int cacheCleanupIntervalSeconds,
            int metricsPort
    ) {
        this(
                httpPort,
                clusterName,
                nodeId,
                nodeName,
                host,
                roles,
                etcdEndpoints,
                etcdNamespace,
                dataPath,
                s3Bucket,
                s3Region,
                s3Protocol,
                s3Endpoint,
                s3ChunkedEncoding,
                false,
                s3AccessKey,
                s3SecretKey,
                snapshotRetainLatest,
                etcdTimeoutSeconds,
                10,
                "async",
                30,
                true,
                0,
                0,
                "immediate",
                1000,
                null,
                cacheMaxBytes,
                cacheCleanupIntervalSeconds,
                metricsPort,
                0,
                0,
                0,
                0
        );
    }

    public boolean etcdEnabled() {
        return etcdEndpoints != null && !etcdEndpoints.isBlank();
    }

    public boolean s3Enabled() {
        return s3Bucket != null && !s3Bucket.isBlank();
    }
}
