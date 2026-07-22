package com.github.wxk6b1203.cli;

import com.github.wxk6b1203.cluster.NodeRole;
import com.github.wxk6b1203.config.ServerConfigFile;
import com.github.wxk6b1203.config.ServerOptions;
import com.github.wxk6b1203.http.HttpApiServer;
import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine;
import picocli.CommandLine.Model.CommandSpec;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

@Slf4j
@CommandLine.Command(name = "server", mixinStandardHelpOptions = true, versionProvider = Version.class, description = "Lucene S3 Server")
public class Server implements Callable<Integer> {
    @CommandLine.Spec
    private CommandSpec spec;

    @CommandLine.Option(names = {"-c", "--conf"}, description = "Path to configuration file")
    private String configFile;

    @CommandLine.Option(names = {"-p", "--http-port"}, description = "HTTP API port")
    private int httpPort = 9200;

    @CommandLine.Option(names = {"--cluster-name"}, description = "Cluster name")
    private String clusterName = "lucene-s3";

    @CommandLine.Option(names = {"--node-id"}, description = "Stable node id")
    private String nodeId = UUID.randomUUID().toString();

    @CommandLine.Option(names = {"--node-name"}, description = "Node display name")
    private String nodeName = "local-node";

    @CommandLine.Option(names = {"--host"}, description = "Advertised host")
    private String host = "127.0.0.1";

    @CommandLine.Option(names = {"--roles"}, description = "Comma separated node roles: MASTER,DATA,INGEST,COORDINATING")
    private String roles = "MASTER,DATA,COORDINATING";

    @CommandLine.Option(names = {"--etcd-endpoints"}, description = "ETCD endpoints. When set, enables distributed cluster state.")
    private String etcdEndpoints;

    @CommandLine.Option(names = {"--etcd-namespace"}, description = "ETCD namespace for cluster state")
    private String etcdNamespace = "lucene-s3/cluster";

    @CommandLine.Option(names = {"--etcd-timeout"}, description = "ETCD operation timeout in seconds for startup and metadata requests")
    private int etcdTimeoutSeconds = 10;

    @CommandLine.Option(names = {"--http-forward-timeout"}, description = "Internal HTTP forwarding timeout in seconds")
    private int httpForwardTimeoutSeconds = 10;

    @CommandLine.Option(names = {"--data-path"}, description = "Local data/cache path")
    private String dataPath = "data";

    @CommandLine.Option(names = {"--cache-max-bytes"}, description = "Maximum bytes for local remote cache. 0 disables capacity eviction.")
    private long cacheMaxBytes = 0;

    @CommandLine.Option(names = {"--cache-cleanup-interval"}, description = "Local remote cache cleanup interval in seconds")
    private int cacheCleanupIntervalSeconds = 60;

    @CommandLine.Option(names = {"--metrics-port"}, description = "Prometheus metrics HTTP port. 0 disables the standalone metrics exporter.")
    private int metricsPort = 0;

    @CommandLine.Option(names = {"--s3-bucket"}, description = "S3 bucket for committed Lucene files")
    private String s3Bucket;

    @CommandLine.Option(names = {"--s3-region"}, description = "S3 region")
    private String s3Region;

    @CommandLine.Option(names = {"--s3-protocol"}, description = "S3 endpoint protocol when endpoint has no scheme: http or https")
    private String s3Protocol = "https";

    @CommandLine.Option(names = {"--s3-endpoint"}, description = "S3-compatible endpoint")
    private String s3Endpoint;

    @CommandLine.Option(names = {"--s3-chunked-encoding"}, negatable = true, description = "Enable AWS SDK S3 chunked encoding. Disabled by default for S3-compatible services.")
    private boolean s3ChunkedEncoding = false;

    @CommandLine.Option(names = {"--s3-content-md5"}, negatable = true, description = "Enable legacy Content-MD5 headers for S3 requests that require MD5, such as DeleteObjects on some S3-compatible services.")
    private boolean s3ContentMd5 = false;

    @CommandLine.Option(names = {"--s3-access-key"}, description = "S3 access key")
    private String s3AccessKey;

    @CommandLine.Option(names = {"--s3-secret-key"}, description = "S3 secret key")
    private String s3SecretKey;

    @CommandLine.Option(names = {"--snapshot-retain-latest"}, description = "Number of latest commit snapshot generations retained per shard")
    private int snapshotRetainLatest = 2;

    @CommandLine.Option(names = {"--upload-wait-strategy"}, description = "Write upload wait strategy: async or wait_for_upload")
    private String uploadWaitStrategy = "async";

    @CommandLine.Option(names = {"--upload-wait-timeout"}, description = "Timeout in seconds when upload wait strategy waits for committed files")
    private int uploadWaitTimeoutSeconds = 30;

    @CommandLine.Option(names = {"--commit-every-request"}, description = "Commit and publish each successful write request: true or false. Enabled by default.")
    private boolean commitEveryRequest = true;

    @CommandLine.Option(names = {"--commit-interval"}, description = "Deferred commit interval in milliseconds when commit-every-request is disabled")
    private int commitIntervalMillis = 1000;

    @CommandLine.Option(names = {"--commit-after-docs"}, description = "Deferred commit threshold in changed documents. 0 disables the threshold.")
    private int commitAfterDocs = 0;

    @CommandLine.Option(names = {"--refresh-policy"}, description = "Refresh policy for local search visibility: immediate or interval")
    private String refreshPolicy = "immediate";

    @CommandLine.Option(names = {"--refresh-interval"}, description = "Refresh interval in milliseconds when refresh-policy=interval")
    private int refreshIntervalMillis = 1000;

    @CommandLine.Option(names = {"--max-write-requests"}, description = "Maximum concurrent write requests. 0 disables write request backpressure.")
    private int maxWriteRequests = 0;

    @CommandLine.Option(names = {"--max-bulk-items"}, description = "Maximum items accepted in a single bulk request. 0 disables the limit.")
    private int maxBulkItems = 0;

    @CommandLine.Option(names = {"--max-bulk-bytes"}, description = "Maximum request body bytes accepted by the bulk API. 0 disables the limit.")
    private long maxBulkBytes = 0;

    @CommandLine.Option(names = {"--max-http-body-bytes"}, description = "Maximum request body bytes for non-bulk HTTP routes (e.g. single-doc, search). 0 disables the limit.")
    private long maxHttpBodyBytes = 100L * 1024 * 1024;

    @CommandLine.Option(names = {"--analyzer-plugin-path"}, description = "Directory or jar file containing third-party Lucene Analyzer plugins")
    private String analyzerPluginPath;

    @Override
    public Integer call() {
        log.info("Starting Lucene S3 Server with config file: {}", configFile);
        CountDownLatch countDownLatch = new CountDownLatch(1);
        try (HttpApiServer apiServer = new HttpApiServer(options())) {
            apiServer.start().toCompletionStage().toCompletableFuture().join();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("Shutting down Lucene S3 Server...");
                countDownLatch.countDown();
            }));
            countDownLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return 1;
        } catch (Exception e) {
            log.error("Lucene S3 Server failed", e);
            return 1;
        }
        return CommandLine.ExitCode.OK;
    }

    ServerOptions options() throws IOException {
        ServerConfigFile config = ServerConfigFile.load(configFile);
        Object resolvedRoles = optionValue("--roles", roles, () -> config.value(
                "roles", "node.roles", "server.roles"
        ));
        Set<NodeRole> parsedRoles = parseRoles(resolvedRoles);
        return new ServerOptions(
                optionValue("--http-port", httpPort, () -> config.intValue(httpPort,
                        "httpPort", "http.port", "server.httpPort", "server.http.port")),
                optionValue("--cluster-name", clusterName, () -> config.stringValue(clusterName,
                        "clusterName", "cluster.name", "server.clusterName", "server.cluster.name")),
                optionValue("--node-id", nodeId, () -> config.stringValue(nodeId,
                        "nodeId", "node.id", "server.nodeId", "server.node.id")),
                optionValue("--node-name", nodeName, () -> config.stringValue(nodeName,
                        "nodeName", "node.name", "server.nodeName", "server.node.name")),
                optionValue("--host", host, () -> config.stringValue(host,
                        "host", "node.host", "server.host", "server.node.host")),
                parsedRoles,
                optionValue("--etcd-endpoints", etcdEndpoints, () -> config.stringValue(etcdEndpoints,
                        "etcdEndpoints", "etcd.endpoints", "server.etcdEndpoints", "server.etcd.endpoints")),
                optionValue("--etcd-namespace", etcdNamespace, () -> config.stringValue(etcdNamespace,
                        "etcdNamespace", "etcd.namespace", "server.etcdNamespace", "server.etcd.namespace")),
                optionValue("--data-path", dataPath, () -> config.stringValue(dataPath,
                        "dataPath", "data.path", "server.dataPath", "server.data.path")),
                optionValue("--s3-bucket", s3Bucket, () -> config.stringValue(s3Bucket,
                        "s3Bucket", "s3.bucket", "server.s3Bucket", "server.s3.bucket")),
                optionValue("--s3-region", s3Region, () -> config.stringValue(s3Region,
                        "s3Region", "s3.region", "server.s3Region", "server.s3.region")),
                optionValue("--s3-protocol", s3Protocol, () -> config.stringValue(s3Protocol,
                        "s3Protocol", "s3.protocol", "server.s3Protocol", "server.s3.protocol")),
                optionValue("--s3-endpoint", s3Endpoint, () -> config.stringValue(s3Endpoint,
                        "s3Endpoint", "s3.endpoint", "server.s3Endpoint", "server.s3.endpoint")),
                optionValue("--s3-chunked-encoding", s3ChunkedEncoding, () -> config.booleanValue(s3ChunkedEncoding,
                        "s3ChunkedEncoding", "s3.chunkedEncoding", "s3.chunked.encoding",
                        "server.s3ChunkedEncoding", "server.s3.chunkedEncoding", "server.s3.chunked.encoding")),
                optionValue("--s3-content-md5", s3ContentMd5, () -> config.booleanValue(s3ContentMd5,
                        "s3ContentMd5", "s3.contentMd5", "s3.content.md5",
                        "server.s3ContentMd5", "server.s3.contentMd5", "server.s3.content.md5")),
                optionValue("--s3-access-key", s3AccessKey, () -> config.stringValue(s3AccessKey,
                        "s3AccessKey", "s3.accessKey", "s3.access.key", "server.s3AccessKey", "server.s3.accessKey")),
                optionValue("--s3-secret-key", s3SecretKey, () -> config.stringValue(s3SecretKey,
                        "s3SecretKey", "s3.secretKey", "s3.secret.key", "server.s3SecretKey", "server.s3.secretKey")),
                optionValue("--snapshot-retain-latest", snapshotRetainLatest, () -> config.intValue(snapshotRetainLatest,
                        "snapshotRetainLatest", "snapshot.retainLatest", "snapshot.retain.latest",
                        "server.snapshotRetainLatest", "server.snapshot.retainLatest", "server.snapshot.retain.latest")),
                optionValue("--etcd-timeout", etcdTimeoutSeconds, () -> config.intValue(etcdTimeoutSeconds,
                        "etcdTimeoutSeconds", "etcd.timeoutSeconds", "etcd.timeout.seconds",
                        "server.etcdTimeoutSeconds", "server.etcd.timeoutSeconds", "server.etcd.timeout.seconds")),
                optionValue("--http-forward-timeout", httpForwardTimeoutSeconds, () -> config.intValue(httpForwardTimeoutSeconds,
                        "httpForwardTimeoutSeconds", "http.forwardTimeoutSeconds", "http.forward.timeout.seconds",
                        "server.httpForwardTimeoutSeconds", "server.http.forwardTimeoutSeconds",
                        "server.http.forward.timeout.seconds")),
                optionValue("--upload-wait-strategy", uploadWaitStrategy, () -> config.stringValue(uploadWaitStrategy,
                        "uploadWaitStrategy", "upload.waitStrategy", "upload.wait.strategy",
                        "server.uploadWaitStrategy", "server.upload.waitStrategy", "server.upload.wait.strategy")),
                optionValue("--upload-wait-timeout", uploadWaitTimeoutSeconds, () -> config.intValue(uploadWaitTimeoutSeconds,
                        "uploadWaitTimeoutSeconds", "upload.waitTimeoutSeconds", "upload.wait.timeout.seconds",
                        "server.uploadWaitTimeoutSeconds", "server.upload.waitTimeoutSeconds",
                        "server.upload.wait.timeout.seconds")),
                optionValue("--commit-every-request", commitEveryRequest, () -> config.booleanValue(commitEveryRequest,
                        "commitEveryRequest", "commit.everyRequest", "commit.every.request",
                        "server.commitEveryRequest", "server.commit.everyRequest", "server.commit.every.request",
                        "server.index.commitEveryRequest", "server.index.commit.everyRequest",
                        "server.index.commit.every.request")),
                optionValue("--commit-interval", commitIntervalMillis, () -> config.intValue(commitIntervalMillis,
                        "commitIntervalMillis", "commit.intervalMillis", "commit.interval.millis",
                        "server.commitIntervalMillis", "server.commit.intervalMillis", "server.commit.interval.millis",
                        "server.index.commitIntervalMillis", "server.index.commit.intervalMillis",
                        "server.index.commit.interval.millis")),
                optionValue("--commit-after-docs", commitAfterDocs, () -> config.intValue(commitAfterDocs,
                        "commitAfterDocs", "commit.afterDocs", "commit.after.docs",
                        "server.commitAfterDocs", "server.commit.afterDocs", "server.commit.after.docs",
                        "server.index.commitAfterDocs", "server.index.commit.afterDocs",
                        "server.index.commit.after.docs")),
                optionValue("--refresh-policy", refreshPolicy, () -> config.stringValue(refreshPolicy,
                        "refreshPolicy", "refresh.policy",
                        "server.refreshPolicy", "server.refresh.policy",
                        "server.index.refreshPolicy", "server.index.refresh.policy")),
                optionValue("--refresh-interval", refreshIntervalMillis, () -> config.intValue(refreshIntervalMillis,
                        "refreshIntervalMillis", "refresh.intervalMillis", "refresh.interval.millis",
                        "server.refreshIntervalMillis", "server.refresh.intervalMillis", "server.refresh.interval.millis",
                        "server.index.refreshIntervalMillis", "server.index.refresh.intervalMillis",
                        "server.index.refresh.interval.millis")),
                optionValue("--analyzer-plugin-path", analyzerPluginPath, () -> config.stringValue(analyzerPluginPath,
                        "analyzerPluginPath", "analyzer.pluginPath", "analyzer.plugin.path",
                        "server.analyzerPluginPath", "server.analyzer.pluginPath", "server.analyzer.plugin.path")),
                optionValue("--cache-max-bytes", cacheMaxBytes, () -> config.longValue(cacheMaxBytes,
                        "cacheMaxBytes", "cache.maxBytes", "cache.max.bytes",
                        "server.cacheMaxBytes", "server.cache.maxBytes", "server.cache.max.bytes")),
                optionValue("--cache-cleanup-interval", cacheCleanupIntervalSeconds, () -> config.intValue(cacheCleanupIntervalSeconds,
                        "cacheCleanupIntervalSeconds", "cache.cleanupIntervalSeconds", "cache.cleanup.interval.seconds",
                        "server.cacheCleanupIntervalSeconds", "server.cache.cleanupIntervalSeconds",
                        "server.cache.cleanup.interval.seconds")),
                optionValue("--metrics-port", metricsPort, () -> config.intValue(metricsPort,
                        "metricsPort", "metrics.port", "server.metricsPort", "server.metrics.port")),
                optionValue("--max-write-requests", maxWriteRequests, () -> config.intValue(maxWriteRequests,
                        "maxWriteRequests", "write.maxRequests", "write.max.requests",
                        "server.maxWriteRequests", "server.write.maxRequests", "server.write.max.requests")),
                optionValue("--max-bulk-items", maxBulkItems, () -> config.intValue(maxBulkItems,
                        "maxBulkItems", "bulk.maxItems", "bulk.max.items",
                        "server.maxBulkItems", "server.bulk.maxItems", "server.bulk.max.items")),
                optionValue("--max-bulk-bytes", maxBulkBytes, () -> config.longValue(maxBulkBytes,
                        "maxBulkBytes", "bulk.maxBytes", "bulk.max.bytes",
                        "server.maxBulkBytes", "server.bulk.maxBytes", "server.bulk.max.bytes")),
                optionValue("--max-http-body-bytes", maxHttpBodyBytes, () -> config.longValue(maxHttpBodyBytes,
                        "maxHttpBodyBytes", "http.maxBodyBytes", "http.max.body.bytes",
                        "server.maxHttpBodyBytes", "server.http.maxBodyBytes", "server.http.max.body.bytes"))
        );
    }

    private Set<NodeRole> parseRoles(Object value) {
        if (value instanceof Collection<?> collection) {
            return collection.stream()
                    .map(Object::toString)
                    .map(String::trim)
                    .filter(role -> !role.isBlank())
                    .map(String::toUpperCase)
                    .map(NodeRole::valueOf)
                    .collect(Collectors.toSet());
        }
        return Arrays.stream(value.toString().split(","))
                .map(String::trim)
                .filter(role -> !role.isBlank())
                .map(String::toUpperCase)
                .map(NodeRole::valueOf)
                .collect(Collectors.toSet());
    }

    private <T> T optionValue(String optionName, T cliValue, ValueSupplier<T> configValue) {
        if (isCliMatched(optionName)) {
            return cliValue;
        }
        T value = configValue.get();
        return value == null ? cliValue : value;
    }

    private boolean isCliMatched(String optionName) {
        if (spec == null || spec.commandLine() == null || spec.commandLine().getParseResult() == null) {
            return false;
        }
        CommandLine.Model.OptionSpec option = spec.findOption(optionName);
        if (option == null) {
            return false;
        }
        if (spec.commandLine().getParseResult().hasMatchedOption(option)) {
            return true;
        }
        for (String name : option.names()) {
            if (spec.commandLine().getParseResult().hasMatchedOption(name)) {
                return true;
            }
            if (name.startsWith("--")) {
                String negatedName = "--no-" + name.substring(2);
                if (spec.commandLine().getParseResult().hasMatchedOption(negatedName)
                        || spec.commandLine().getParseResult().originalArgs().contains(negatedName)) {
                    return true;
                }
            }
        }
        return false;
    }

    private interface ValueSupplier<T> {
        T get();
    }
}
