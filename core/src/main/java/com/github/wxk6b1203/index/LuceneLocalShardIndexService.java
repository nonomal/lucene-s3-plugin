package com.github.wxk6b1203.index;

import com.github.wxk6b1203.cluster.ClusterState;
import com.github.wxk6b1203.cluster.FieldMapping;
import com.github.wxk6b1203.cluster.IndexSettings;
import com.github.wxk6b1203.cluster.ShardId;
import com.github.wxk6b1203.cluster.ShardState;
import com.github.wxk6b1203.metadata.common.IndexCommitSnapshot;
import com.github.wxk6b1203.metadata.common.IndexFileStatus;
import com.github.wxk6b1203.metadata.common.ShardSummary;
import com.github.wxk6b1203.metadata.provider.ManifestMetadataManager;
import com.github.wxk6b1203.search.*;
import com.github.wxk6b1203.store.common.PathUtil;
import com.github.wxk6b1203.store.directory.Hierarchy;
import com.github.wxk6b1203.store.directory.S3CachingDirectory;
import com.github.wxk6b1203.store.directory.S3DirectoryOptions;
import com.github.wxk6b1203.store.directory.S3LockFactory;
import com.github.wxk6b1203.store.manifest.ManifestManager;
import com.github.wxk6b1203.store.manifest.ManifestOptions;
import com.github.wxk6b1203.store.object.RemoteObjectStore;
import com.github.wxk6b1203.util.JsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.*;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class LuceneLocalShardIndexService implements LocalShardIndexService {
    private static final int UPDATE_BY_QUERY_BATCH_SIZE = 512;
    private static final Duration REMOTE_SEARCHER_CACHE_TTL = Duration.ofMinutes(5);
    private static final long SLOW_COMMIT_WARN_NANOS = 1_000_000_000L;
    private static final String FIELD_NAMES_FIELD = "_field_names";

    private final Path basePath;
    private final String bucket;
    private final ManifestOptions manifestOptions;
    private final IndexWriteOptions writeOptions;
    private final ManifestMetadataManager metadataManager;
    private final RemoteObjectStore remoteObjectStore;
    private final ExecutorService uploadWorkerPool;
    private final AnalyzerRegistry analyzerRegistry;
    private final LuceneAggregationExecutor aggregationExecutor = new LuceneAggregationExecutor();
    private final ConcurrentMap<ShardId, Map<String, FieldMapping>> mappingsByShard = new ConcurrentHashMap<>();
    private final Map<ShardId, ShardWriter> writers = new ConcurrentHashMap<>();
    private final Set<ShardId> retiringShards = ConcurrentHashMap.newKeySet();
    private final Map<RemoteSearcherKey, CachedRemoteSearcher> remoteSearchers = new ConcurrentHashMap<>();
    private final Map<String, PitContext> pits = new ConcurrentHashMap<>();

    public LuceneLocalShardIndexService(
            Path basePath,
            String bucket,
            ManifestMetadataManager metadataManager,
            RemoteObjectStore remoteObjectStore
    ) {
        this(basePath, bucket, metadataManager, remoteObjectStore, new ManifestOptions(bucket));
    }

    public LuceneLocalShardIndexService(
            Path basePath,
            String bucket,
            ManifestMetadataManager metadataManager,
            RemoteObjectStore remoteObjectStore,
            ManifestOptions manifestOptions
    ) {
        this(basePath, bucket, metadataManager, remoteObjectStore, manifestOptions, null);
    }

    public LuceneLocalShardIndexService(
            Path basePath,
            String bucket,
            ManifestMetadataManager metadataManager,
            RemoteObjectStore remoteObjectStore,
            ManifestOptions manifestOptions,
            Path analyzerPluginPath
    ) {
        this(basePath, bucket, metadataManager, remoteObjectStore, manifestOptions, analyzerPluginPath, IndexWriteOptions.defaults());
    }

    public LuceneLocalShardIndexService(
            Path basePath,
            String bucket,
            ManifestMetadataManager metadataManager,
            RemoteObjectStore remoteObjectStore,
            ManifestOptions manifestOptions,
            Path analyzerPluginPath,
            IndexWriteOptions writeOptions
    ) {
        this.basePath = basePath;
        this.bucket = bucket;
        this.manifestOptions = manifestOptions == null ? new ManifestOptions(bucket) : manifestOptions;
        this.writeOptions = writeOptions == null ? IndexWriteOptions.defaults() : writeOptions;
        this.metadataManager = metadataManager;
        this.remoteObjectStore = remoteObjectStore;
        this.analyzerRegistry = new AnalyzerRegistry(analyzerPluginPath);
        this.uploadWorkerPool = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("upload-worker-", 0).factory());
    }

    @Override
    public IndexDocumentResponse index(IndexDocumentRequest request) throws IOException {
        String id = request.id() == null || request.id().isBlank() ? UUID.randomUUID().toString() : request.id();
        rememberMappings(request.shardId(), request.mappings());
        ShardWriter shardWriter = writers.computeIfAbsent(request.shardId(), this::openShardWriter);
        synchronized (shardWriter) {
            Document document = document(request.indexName(), request.shardId(), id, request.source(), request.mappings());
            if (request.createOnly()) {
                try (DirectoryReader reader = DirectoryReader.open(shardWriter.writer)) {
                    if (new IndexSearcher(reader).count(new TermQuery(new Term("_id", id))) > 0) {
                        throw new IllegalArgumentException("document already exists: " + id);
                    }
                }
                shardWriter.writer.addDocument(document);
            } else {
                shardWriter.writer.updateDocument(new Term("_id", id), document);
            }
            finishWrite(shardWriter, 1);
        }
        return new IndexDocumentResponse(
                request.indexName(),
                request.shardId(),
                id,
                request.createOnly() ? "created" : "indexed",
                true
        );
    }

    @Override
    public IndexDocumentResponse delete(IndexDocumentRequest request) throws IOException {
        if (request.id() == null || request.id().isBlank()) {
            throw new IllegalArgumentException("delete requires document id");
        }
        ShardWriter shardWriter = writers.computeIfAbsent(request.shardId(), this::openShardWriter);
        synchronized (shardWriter) {
            shardWriter.writer.deleteDocuments(new Term("_id", request.id()));
            finishWrite(shardWriter, 1);
        }
        return new IndexDocumentResponse(request.indexName(), request.shardId(), request.id(), "deleted", true);
    }

    @Override
    public List<IndexDocumentOperationResult> bulk(Collection<IndexDocumentOperation> operations) throws IOException {
        List<IndexDocumentOperation> operationList = List.copyOf(operations);
        if (operationList.isEmpty()) {
            return List.of();
        }
        ShardId shardId = null;
        Map<String, FieldMapping> mappings = Map.of();
        for (IndexDocumentOperation operation : operationList) {
            if (operation.request() == null) {
                throw new IllegalArgumentException("bulk operation request is required");
            }
            if (shardId == null) {
                shardId = operation.request().shardId();
            } else if (!shardId.equals(operation.request().shardId())) {
                throw new IllegalArgumentException("bulk batch must target a single shard");
            }
            if (!operation.request().mappings().isEmpty()) {
                mappings = operation.request().mappings();
            }
        }
        rememberMappings(shardId, mappings);
        ShardWriter shardWriter = writers.computeIfAbsent(shardId, this::openShardWriter);
        synchronized (shardWriter) {
            List<IndexDocumentOperationResult> results = new ArrayList<>(operationList.size());
            List<Integer> successfulPositions = new ArrayList<>();
            Set<String> visibleCreateIds = visibleCreateIds(shardWriter.writer, operationList);
            for (int i = 0; i < operationList.size(); i++) {
                IndexDocumentOperation operation = operationList.get(i);
                IndexDocumentRequest request = operation.request();
                String id = request.id() == null || request.id().isBlank() ? UUID.randomUUID().toString() : request.id();
                try {
                    IndexDocumentResponse response;
                    if (operation.delete()) {
                        shardWriter.writer.deleteDocuments(new Term("_id", id));
                        visibleCreateIds.remove(id);
                        response = new IndexDocumentResponse(request.indexName(), request.shardId(), id, "deleted", true);
                    } else {
                        Document document = document(request.indexName(), request.shardId(), id, request.source(), request.mappings());
                        if (request.createOnly()) {
                            if (visibleCreateIds.contains(id)) {
                                throw new IllegalArgumentException("document already exists: " + id);
                            }
                            shardWriter.writer.addDocument(document);
                            visibleCreateIds.add(id);
                        } else {
                            shardWriter.writer.updateDocument(new Term("_id", id), document);
                            visibleCreateIds.add(id);
                        }
                        response = new IndexDocumentResponse(
                                request.indexName(),
                                request.shardId(),
                                id,
                                request.createOnly() ? "created" : "indexed",
                                true
                        );
                    }
                    successfulPositions.add(i);
                    results.add(IndexDocumentOperationResult.success(response));
                } catch (Exception e) {
                    results.add(IndexDocumentOperationResult.failure(e));
                }
            }
            if (!successfulPositions.isEmpty()) {
                try {
                    finishWrite(shardWriter, successfulPositions.size());
                } catch (Exception e) {
                    for (Integer position : successfulPositions) {
                        results.set(position, IndexDocumentOperationResult.failure(e));
                    }
                }
            }
            return results;
        }
    }

    private Set<String> visibleCreateIds(IndexWriter writer, List<IndexDocumentOperation> operations) throws IOException {
        Set<String> createIds = new HashSet<>();
        for (IndexDocumentOperation operation : operations) {
            IndexDocumentRequest request = operation.request();
            if (request.createOnly() && request.id() != null && !request.id().isBlank()) {
                createIds.add(request.id());
            }
        }
        if (createIds.isEmpty()) {
            return new HashSet<>();
        }
        Set<String> visibleIds = new HashSet<>();
        try (DirectoryReader reader = DirectoryReader.open(writer)) {
            IndexSearcher searcher = new IndexSearcher(reader);
            for (String id : createIds) {
                if (searcher.count(new TermQuery(new Term("_id", id))) > 0) {
                    visibleIds.add(id);
                }
            }
        }
        return visibleIds;
    }

    @Override
    public SearchResponse search(ShardId shardId, SearchRequest request) throws IOException {
        long started = System.nanoTime();
        cleanupIdleResources();
        PitContext pit = pit(request.pitId(), shardId);
        if (pit != null) {
            if (pit.reader() == null) {
                return emptySearchResponse(started);
            }
            return searchSearcher(new IndexSearcher(pit.reader()), request, started);
        }
        boolean remoteOnly = "remote".equalsIgnoreCase(request.readPreference())
                || "strong".equalsIgnoreCase(request.readPreference());
        ShardWriter shardWriter = remoteOnly ? null : writers.get(shardId);
        if (shardWriter != null) {
            IndexSearcher searcher = shardWriter.searcherManager.acquire();
            try {
                return searchSearcher(searcher, request, started);
            } finally {
                shardWriter.searcherManager.release(searcher);
            }
        }
        return searchRemoteSnapshot(shardId, request, started);
    }

    private SearchResponse searchRemoteSnapshot(ShardId shardId, SearchRequest request, long started) throws IOException {
        Long generation = request.remoteSnapshotGeneration();
        if (generation == null) {
            IndexCommitSnapshot snapshot = metadataManager.latestSnapshot(physicalIndexName(shardId));
            if (snapshot == null) {
                return emptySearchResponse(started);
            }
            generation = snapshot.getGeneration();
        }
        if (generation < 0) {
            return emptySearchResponse(started);
        }
        CachedRemoteSearcher cached;
        try {
            cached = acquireRemoteSearcher(shardId, generation);
        } catch (IndexNotFoundException e) {
            return emptySearchResponse(started);
        }
        try {
            return searchSearcher(cached.searcher(), request, started);
        } finally {
            cached.release();
        }
    }

    private CachedRemoteSearcher acquireRemoteSearcher(ShardId shardId, long generation) throws IOException {
        RemoteSearcherKey key = new RemoteSearcherKey(shardId, generation);
        while (true) {
            CachedRemoteSearcher cached = remoteSearchers.get(key);
            if (cached != null && cached.acquire()) {
                return cached;
            }
            CachedRemoteSearcher created = openRemoteSearcher(shardId, generation);
            CachedRemoteSearcher existing = remoteSearchers.putIfAbsent(key, created);
            if (existing == null) {
                created.acquire();
                return created;
            }
            created.close();
        }
    }

    private CachedRemoteSearcher openRemoteSearcher(ShardId shardId, long generation) throws IOException {
        S3CachingDirectory directory = openShardDirectory(
                shardId,
                remoteSnapshotStatuses(),
                false,
                generation
        );
        try {
            DirectoryReader reader = DirectoryReader.open(directory);
            return new CachedRemoteSearcher(directory, reader, new IndexSearcher(reader));
        } catch (IOException | RuntimeException e) {
            directory.close();
            throw e;
        }
    }

    private void cleanupExpiredRemoteSearchers() {
        long now = System.nanoTime();
        remoteSearchers.forEach((key, searcher) -> {
            if (searcher.expired(now) && remoteSearchers.remove(key, searcher)) {
                try {
                    searcher.close();
                } catch (IOException ignored) {
                }
            }
        });
    }

    @Override
    public void cleanupIdleResources() {
        cleanupExpiredRemoteSearchers();
    }

    private void closeRemoteSearchers(ShardId shardId) {
        remoteSearchers.forEach((key, searcher) -> {
            if (key.shardId().equals(shardId) && remoteSearchers.remove(key, searcher)) {
                try {
                    searcher.close();
                } catch (IOException ignored) {
                }
            }
        });
    }

    private void closeRemoteSearchers() {
        remoteSearchers.forEach((key, searcher) -> {
            if (remoteSearchers.remove(key, searcher)) {
                try {
                    searcher.close();
                } catch (IOException ignored) {
                }
            }
        });
    }

    private SearchResponse emptySearchResponse(long started) {
        return new SearchResponse(
                elapsedMillis(started),
                1,
                1,
                0,
                List.of(),
                Map.of(),
                List.of()
        );
    }

    private SearchResponse searchSearcher(IndexSearcher searcher, SearchRequest request, long started) throws IOException {
        IndexReader reader = searcher.getIndexReader();
        int from = Math.max(0, request.from());
        int size = Math.max(0, request.size());
        Query query = searchQuery(request);
        Sort luceneSort = luceneSort(request.sort(), request.mappings());
        if (!request.searchAfter().isEmpty() && luceneSort == null) {
            throw new IllegalArgumentException("search_after requires sort");
        }
        int topN = Math.max(1, request.searchAfter().isEmpty() ? from + size : size);
        ScoreDoc after = searchAfterDoc(request, reader.maxDoc());
        TopDocs topDocs = luceneSort == null
                ? searcher.search(query, topN)
                : searcher.searchAfter(after, query, topN, luceneSort, true);
        List<SearchHit> hits = new ArrayList<>();
        var storedFields = searcher.storedFields();
        ScoreDoc[] scoreDocs = topDocs.scoreDocs;
        for (ScoreDoc scoreDoc : scoreDocs) {
            hits.add(hit(
                    searcher,
                    scoreDoc.doc,
                    storedFields.document(scoreDoc.doc),
                    scoreDoc.score,
                    nativeSortValues(topDocs, scoreDoc),
                    request.sort(),
                    request.mappings()
            ));
        }
        if (request.vector() != null && request.vector().minScore() != null) {
            hits = hits.stream()
                    .filter(hit -> hit.score() >= request.vector().minScore())
                    .toList();
        }
        if (luceneSort == null && !request.sort().isEmpty()) {
            hits = hits.stream()
                    .sorted((left, right) -> compareHits(left, right, request.sort()))
                    .toList();
        }
        int hitFrom = Math.min(request.searchAfter().isEmpty() ? from : 0, hits.size());
        int hitTo = Math.min(hitFrom + size, hits.size());
        List<SearchHit> page = hits.subList(hitFrom, hitTo);
        Map<String, Object> aggregations = aggregationExecutor.aggregate(searcher, query, request.aggregations(), request.mappings());
        return new SearchResponse(
                elapsedMillis(started),
                1,
                1,
                0,
                page,
                aggregations,
                List.of()
        );
    }

    private long elapsedMillis(long started) {
        long elapsedNanos = Math.max(0, System.nanoTime() - started);
        return elapsedNanos == 0 ? 0 : Math.max(1, (elapsedNanos + 999_999) / 1_000_000);
    }

    @Override
    public PointInTimeResponse openPointInTime(
            ShardId shardId,
            String indexName,
            Duration keepAlive,
            String readPreference
    ) throws IOException {
        cleanupExpiredPits();
        Duration ttl = keepAlive == null || keepAlive.isNegative() || keepAlive.isZero() ? Duration.ofMinutes(1) : keepAlive;
        if ("remote".equalsIgnoreCase(readPreference) || "strong".equalsIgnoreCase(readPreference)) {
            return openRemotePointInTime(shardId, indexName, ttl);
        }
        ShardWriter shardWriter = writers.computeIfAbsent(shardId, this::openShardWriter);
        synchronized (shardWriter) {
            DirectoryReader reader = DirectoryReader.open(shardWriter.writer);
            String id = UUID.randomUUID().toString();
            pits.put(id, new PitContext(shardId, indexName, reader, null, Instant.now().plus(ttl)));
            return new PointInTimeResponse(id);
        }
    }

    private PointInTimeResponse openRemotePointInTime(ShardId shardId, String indexName, Duration ttl) throws IOException {
        S3CachingDirectory directory = openShardDirectory(shardId);
        String id = UUID.randomUUID().toString();
        directory.pinRemoteSnapshot(id, Instant.now().plus(ttl));
        try {
            DirectoryReader reader = DirectoryReader.open(directory);
            pits.put(id, new PitContext(shardId, indexName, reader, directory, Instant.now().plus(ttl)));
        } catch (IndexNotFoundException e) {
            directory.close();
            pits.put(id, new PitContext(shardId, indexName, null, null, Instant.now().plus(ttl)));
        } catch (IOException | RuntimeException e) {
            directory.close();
            throw e;
        }
        return new PointInTimeResponse(id);
    }

    @Override
    public boolean closePointInTime(String pitId) throws IOException {
        if (pitId == null || pitId.isBlank()) {
            return false;
        }
        PitContext pit = pits.remove(pitId);
        if (pit == null) {
            return false;
        }
        closePit(pit);
        return true;
    }

    private PitContext pit(String pitId, ShardId shardId) throws IOException {
        if (pitId == null || pitId.isBlank()) {
            return null;
        }
        PitContext pit = pits.get(pitId);
        if (pit == null) {
            throw new IllegalArgumentException("point in time not found: " + pitId);
        }
        if (Instant.now().isAfter(pit.expiresAt())) {
            closePointInTime(pitId);
            throw new IllegalArgumentException("point in time expired: " + pitId);
        }
        if (!pit.shardId().equals(shardId)) {
            throw new IllegalArgumentException("point in time does not belong to shard: " + shardId.routeKey());
        }
        return pit;
    }

    private void cleanupExpiredPits() {
        Instant now = Instant.now();
        pits.entrySet().removeIf(entry -> {
            if (now.isBefore(entry.getValue().expiresAt())) {
                return false;
            }
            try {
                closePit(entry.getValue());
            } catch (IOException ignored) {
            }
            return true;
        });
    }

    private void closePit(PitContext pit) throws IOException {
        IOException failure = null;
        if (pit.reader() != null) {
            try {
                pit.reader().close();
            } catch (IOException e) {
                failure = e;
            }
        }
        if (pit.directory() != null) {
            try {
                pit.directory().close();
            } catch (IOException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private SearchHit hit(
            IndexSearcher searcher,
            int docId,
            Document document,
            float score,
            List<Object> nativeSortValues,
            List<Map<String, Object>> sort,
            Map<String, FieldMapping> mappings
    ) throws IOException {
        Map<String, Object> source = source(document);
        return new SearchHit(
                document.get("_index"),
                document.get("_id"),
                score,
                source,
                nativeSortValues == null
                        ? sortValues(searcher, docId, document.get("_id"), score, source, sort, mappings)
                        : nativeSortValues
        );
    }

    private List<Object> nativeSortValues(TopDocs topDocs, ScoreDoc scoreDoc) {
        if (!(topDocs instanceof TopFieldDocs) || !(scoreDoc instanceof FieldDoc fieldDoc)) {
            return null;
        }
        return Arrays.stream(fieldDoc.fields)
                .map(value -> value instanceof BytesRef bytesRef ? bytesRef.utf8ToString() : value)
                .toList();
    }

    private ScoreDoc searchAfterDoc(SearchRequest request, int maxDoc) {
        if (request.searchAfter().isEmpty()) {
            return null;
        }
        Object[] fields = coerceSearchAfterValues(request.sort(), request.mappings(), request.searchAfter());
        return new FieldDoc(Math.max(0, maxDoc - 1), Float.NaN, fields);
    }

    private Object[] coerceSearchAfterValues(
            List<Map<String, Object>> sort,
            Map<String, FieldMapping> mappings,
            List<Object> values
    ) {
        if (values.size() != sort.size()) {
            throw new IllegalArgumentException("search_after value count must match sort field count");
        }
        Object[] result = new Object[values.size()];
        for (int i = 0; i < values.size(); i++) {
            result[i] = coerceSearchAfterValue(sortField(sort.get(i)), mappings, values.get(i));
        }
        return result;
    }

    private Object coerceSearchAfterValue(String field, Map<String, FieldMapping> mappings, Object value) {
        if (value == null) {
            return null;
        }
        if ("_score".equals(field)) {
            return value instanceof Number number ? number.floatValue() : Float.valueOf(String.valueOf(value));
        }
        if ("_id".equals(field)) {
            return new BytesRef(String.valueOf(value));
        }
        FieldMapping mapping = mappings.get(field);
        if (mapping == null) {
            throw new IllegalArgumentException("search_after sort field is not mapped: " + field);
        }
        if (mapping.longNumber()) {
            return requiredLong(value, field);
        }
        if (mapping.date()) {
            return requiredDate(value, field, mapping);
        }
        if (mapping.doubleNumber()) {
            return requiredDouble(value, field);
        }
        return new BytesRef(String.valueOf(value));
    }

    private List<Object> sortValues(
            IndexSearcher searcher,
            int docId,
            String id,
            float score,
            Map<String, Object> source,
            List<Map<String, Object>> sort,
            Map<String, FieldMapping> mappings
    ) throws IOException {
        if (sort == null || sort.isEmpty()) {
            return List.of();
        }
        List<Object> values = new ArrayList<>(sort.size());
        for (Map<String, Object> spec : sort) {
            String field = sortField(spec);
            Object value = switch (field) {
                case "_id" -> id;
                case "_score" -> score;
                default -> sortFieldValue(searcher, docId, field, mappings.get(field), source);
            };
            values.add(value);
        }
        return values;
    }

    private Object sortFieldValue(
            IndexSearcher searcher,
            int docId,
            String field,
            FieldMapping mapping,
            Map<String, Object> source
    ) throws IOException {
        if (mapping != null && Boolean.TRUE.equals(mapping.docValues())) {
            Object value = docValue(searcher, docId, field, mapping);
            if (value != null) {
                return value;
            }
        }
        return source.get(field);
    }

    private int compareHits(SearchHit left, SearchHit right, List<Map<String, Object>> sort) {
        for (int i = 0; i < sort.size(); i++) {
            int compared = compareValues(left.sortValues().get(i), right.sortValues().get(i));
            if (compared != 0) {
                return sortDescending(sort.get(i)) ? -compared : compared;
            }
        }
        int byScore = Float.compare(right.score(), left.score());
        return byScore != 0 ? byScore : left.id().compareTo(right.id());
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private int compareValues(Object left, Object right) {
        if (left == null && right == null) {
            return 0;
        }
        if (left == null) {
            return 1;
        }
        if (right == null) {
            return -1;
        }
        if (left instanceof Number leftNumber && right instanceof Number rightNumber) {
            return Double.compare(leftNumber.doubleValue(), rightNumber.doubleValue());
        }
        if (left instanceof Comparable comparable && left.getClass().isInstance(right)) {
            return comparable.compareTo(right);
        }
        return String.valueOf(left).compareTo(String.valueOf(right));
    }

    private String sortField(Map<String, Object> spec) {
        return spec.keySet().stream().findFirst().orElse("_score");
    }

    private boolean sortDescending(Map<String, Object> spec) {
        String field = sortField(spec);
        Object value = spec.get(field);
        if (value instanceof Map<?, ?> map) {
            Object order = map.get("order");
            return order == null ? "_score".equals(field) : "desc".equalsIgnoreCase(String.valueOf(order));
        }
        return value == null ? "_score".equals(field) : "desc".equalsIgnoreCase(String.valueOf(value));
    }

    private Sort luceneSort(List<Map<String, Object>> sort, Map<String, FieldMapping> mappings) {
        if (sort == null || sort.isEmpty()) {
            return null;
        }
        List<SortField> fields = new ArrayList<>(sort.size());
        for (Map<String, Object> spec : sort) {
            fields.add(luceneSortField(spec, mappings));
        }
        return new Sort(fields.toArray(SortField[]::new));
    }

    private SortField luceneSortField(Map<String, Object> spec, Map<String, FieldMapping> mappings) {
        String field = sortField(spec);
        boolean descending = sortDescending(spec);
        if ("_score".equals(field)) {
            return new SortField(null, SortField.Type.SCORE, descending);
        }
        if ("_id".equals(field)) {
            return new SortField("_id", SortField.Type.STRING, descending, SortField.STRING_LAST);
        }
        FieldMapping mapping = mappings.get(field);
        if (mapping == null) {
            throw new IllegalArgumentException("sort field is not mapped: " + field);
        }
        if (!Boolean.TRUE.equals(mapping.docValues())) {
            throw new IllegalArgumentException("sort field requires doc_values: " + field);
        }
        if (mapping.keyword() || mapping.bool()) {
            if (mapping.multiValued()) {
                return new SortedSetSortField(
                        field,
                        descending,
                        descending ? SortedSetSelector.Type.MAX : SortedSetSelector.Type.MIN,
                        SortField.STRING_LAST
                );
            }
            return new SortField(field, SortField.Type.STRING, descending, SortField.STRING_LAST);
        }
        if (mapping.longNumber() || mapping.date()) {
            if (mapping.multiValued()) {
                return new SortedNumericSortField(
                        field,
                        SortField.Type.LONG,
                        descending,
                        descending ? SortedNumericSelector.Type.MAX : SortedNumericSelector.Type.MIN,
                        descending ? Long.MIN_VALUE : Long.MAX_VALUE
                );
            }
            return new SortField(field, SortField.Type.LONG, descending, descending ? Long.MIN_VALUE : Long.MAX_VALUE);
        }
        if (mapping.doubleNumber()) {
            if (mapping.multiValued()) {
                return new SortedNumericSortField(
                        field,
                        SortField.Type.DOUBLE,
                        descending,
                        descending ? SortedNumericSelector.Type.MAX : SortedNumericSelector.Type.MIN,
                        descending ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY
                );
            }
            return new SortField(
                    field,
                    SortField.Type.DOUBLE,
                    descending,
                    descending ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY
            );
        }
        throw new IllegalArgumentException("sort field type is not supported: " + field);
    }

    @Override
    public ByQueryResponse deleteByQuery(ShardId shardId, ByQueryRequest request) throws IOException {
        rememberMappings(shardId, request.mappings());
        ShardWriter shardWriter = writers.computeIfAbsent(shardId, this::openShardWriter);
        Query query = query(request.query(), request.mappings());
        long deleted;
        synchronized (shardWriter) {
            try (DirectoryReader reader = DirectoryReader.open(shardWriter.writer)) {
                deleted = new IndexSearcher(reader).count(query);
            }
            shardWriter.writer.deleteDocuments(query);
            finishWrite(shardWriter, deleted > 0 ? saturatedInt(deleted) : 0);
        }
        return new ByQueryResponse(null, "delete_by_query", "deleted=" + deleted);
    }

    @Override
    public ByQueryResponse updateByQuery(ShardId shardId, ByQueryRequest request) throws IOException {
        if (request.document() == null || request.document().isEmpty()) {
            throw new IllegalArgumentException("update_by_query requires a non-empty doc object");
        }
        rememberMappings(shardId, request.mappings());
        ShardWriter shardWriter = writers.computeIfAbsent(shardId, this::openShardWriter);
        Query query = query(request.query(), request.mappings());
        long updated = 0;
        synchronized (shardWriter) {
            try (DirectoryReader reader = DirectoryReader.open(shardWriter.writer)) {
                IndexSearcher searcher = new IndexSearcher(reader);
                var storedFields = searcher.storedFields();
                ScoreDoc after = null;
                while (true) {
                    TopDocs topDocs = searcher.searchAfter(after, query, UPDATE_BY_QUERY_BATCH_SIZE);
                    if (topDocs.scoreDocs.length == 0) {
                        break;
                    }
                    for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                        var old = storedFields.document(scoreDoc.doc);
                        Map<String, Object> merged = new HashMap<>(source(old));
                        merged.putAll(request.document());
                        Document document = document(old.get("_index"), shardId, old.get("_id"), merged, request.mappings());
                        shardWriter.writer.updateDocument(new Term("_id", document.get("_id")), document);
                        updated++;
                    }
                    after = topDocs.scoreDocs[topDocs.scoreDocs.length - 1];
                }
            }
            finishWrite(shardWriter, updated > 0 ? saturatedInt(updated) : 0);
        }
        return new ByQueryResponse(null, "update_by_query", "updated=" + updated);
    }

    @Override
    public void forceMerge(ShardId shardId, int maxNumSegments) throws IOException {
        ShardWriter shardWriter = writers.computeIfAbsent(shardId, this::openShardWriter);
        synchronized (shardWriter) {
            shardWriter.writer.forceMerge(Math.max(1, maxNumSegments));
            finishWrite(shardWriter, 0, true, true);
        }
    }

    @Override
    public void deleteIndex(String indexName, int numberOfShards) throws IOException {
        IOException failure = null;
        for (String pitId : List.copyOf(pits.keySet())) {
            PitContext pit = pits.get(pitId);
            if (pit != null && pit.indexName().equals(indexName)) {
                try {
                    closePointInTime(pitId);
                } catch (IOException e) {
                    failure = addFailure(failure, e);
                }
            }
        }
        for (ShardId shardId : List.copyOf(writers.keySet())) {
            if (shardId.indexName().equals(indexName)) {
                ShardWriter writer = writers.remove(shardId);
                if (writer != null) {
                    try {
                        synchronized (writer) {
                            writer.close();
                        }
                    } catch (IOException e) {
                        failure = addFailure(failure, e);
                    }
                }
            }
        }
        for (ShardId shardId : List.copyOf(mappingsByShard.keySet())) {
            if (shardId.indexName().equals(indexName)) {
                mappingsByShard.remove(shardId);
            }
        }
        for (int shard = 0; shard < numberOfShards; shard++) {
            closeRemoteSearchers(new ShardId(indexName, shard));
            String physicalIndexName = indexName + "__shard_" + shard;
            try {
                deleteRecursively(PathUtil.walDataPath(basePath, physicalIndexName));
                deleteRecursively(PathUtil.sharedDataPath(basePath, physicalIndexName));
                deleteRecursively(PathUtil.sharedTempPath(basePath, physicalIndexName));
            } catch (Exception e) {
                failure = addFailure(failure, e instanceof IOException ioException ? ioException : new IOException(e));
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    @Override
    public void retryPendingUploads(Collection<ShardId> shardIds) throws IOException {
        IOException failure = null;
        for (ShardId shardId : shardIds) {
            try {
                ShardWriter shardWriter = writers.get(shardId);
                if (shardWriter != null) {
                    synchronized (shardWriter) {
                        shardWriter.directory.publishLocalCommit()
                                .whenComplete((published, throwable) -> {
                                    if (throwable != null) {
                                        log.warn("failed to retry pending uploads for {}", shardId.routeKey(), throwable);
                                        return;
                                    }
                                    if (Boolean.TRUE.equals(published)) {
                                        markLatestSnapshotPublished(shardWriter);
                                        shardWriter.snapshotPublishPending.set(false);
                                        releaseReclaimableSnapshotCommits(shardWriter);
                                    }
                                });
                    }
                } else {
                    Path walPath = PathUtil.walDataPath(basePath, physicalIndexName(shardId));
                    if (!Files.isDirectory(walPath) || !hasCommittedSegmentFile(walPath)) {
                        discardPendingUploads(shardId);
                        continue;
                    }
                    try (S3CachingDirectory directory = openShardDirectory(shardId, remoteSnapshotStatuses())) {
                        directory.publishLocalCommit();
                    }
                    // Orphaned shard repaired: recompute the summary from a full scan so a drifted
                    // counter cannot pin readiness to owner-only reads forever.
                    try (ManifestManager manifestManager = openManifestManager()) {
                        manifestManager.reconcileShardSummary(physicalIndexName(shardId));
                    }
                }
            } catch (IOException e) {
                failure = addFailure(failure, e);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    @Override
    public Collection<ShardId> shardIdsWithPendingWrites() {
        return writers.entrySet()
                .stream()
                .filter(entry -> entry.getValue().uncommittedOperations.get() > 0)
                .map(Map.Entry::getKey)
                .toList();
    }

    @Override
    public Collection<ShardId> shardIdsWithPendingUploads() {
        return writers.keySet()
                .stream()
                .filter(this::hasPendingUploads)
                .toList();
    }

    private boolean hasPendingUploads(ShardId shardId) {
        // Summary key is the fast path (one GET); fall back to the authoritative scan when the
        // shard has no summary yet (legacy data or fresh namespace).
        ShardSummary summary = metadataManager.shardSummary(physicalIndexName(shardId));
        if (summary != null) {
            return summary.pendingCount() > 0;
        }
        return !metadataManager.listAll(physicalIndexName(shardId), List.of(
                IndexFileStatus.DIRTY,
                IndexFileStatus.UPLOADING
        )).isEmpty();
    }

    @Override
    public void reconcileWithClusterState(ClusterState state, String expectedOwnerNodeId) throws IOException {
        if (!retiringShards.isEmpty()) {
            // Previous reconcile round still draining; retry on the next tick.
            return;
        }
        IOException failure = null;
        for (Map.Entry<ShardId, ShardWriter> entry : List.copyOf(writers.entrySet())) {
            ShardId shardId = entry.getKey();
            RetirePlan plan = retirePlan(state, shardId, expectedOwnerNodeId);
            if (plan == RetirePlan.NONE || !retiringShards.add(shardId)) {
                continue;
            }
            try {
                if (plan == RetirePlan.QUARANTINE_ONLY) {
                    // Index deleted or pending deletion: the master reclaims remote data; keep no local lineage.
                    synchronized (entry.getValue()) {
                        closePitsFor(shardId);
                        if (writers.remove(shardId, entry.getValue())) {
                            entry.getValue().close();
                        }
                    }
                    quarantineShardDirectories(shardId);
                } else {
                    retireShardWriter(shardId);
                }
            } catch (IOException e) {
                failure = addFailure(failure, e);
            } finally {
                retiringShards.remove(shardId);
            }
        }
        for (ShardId shardId : localWalShardIds()) {
            if (writers.containsKey(shardId)
                    || retirePlan(state, shardId, expectedOwnerNodeId) == RetirePlan.NONE
                    || !retiringShards.add(shardId)) {
                continue;
            }
            try {
                drainAndQuarantine(shardId, true);
            } catch (IOException e) {
                failure = addFailure(failure, e);
            } finally {
                retiringShards.remove(shardId);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private enum RetirePlan {
        NONE,
        DRAIN,
        QUARANTINE_ONLY
    }

    private RetirePlan retirePlan(ClusterState state, ShardId shardId, String expectedOwnerNodeId) {
        IndexSettings settings = state.indices().get(shardId.indexName());
        if (settings == null || settings.deletePending()) {
            return RetirePlan.QUARANTINE_ONLY;
        }
        boolean owned = state.routingTable().stream()
                .filter(routing -> routing.shardId().equals(shardId))
                .findFirst()
                .map(routing -> routing.state() == ShardState.STARTED
                        && expectedOwnerNodeId.equals(routing.nodeId()))
                .orElse(false);
        return owned ? RetirePlan.NONE : RetirePlan.DRAIN;
    }

    @Override
    public void retireShardWriter(ShardId shardId) throws IOException {
        ShardWriter shardWriter = writers.get(shardId);
        if (shardWriter == null) {
            drainAndQuarantine(shardId, true);
            return;
        }
        synchronized (shardWriter) {
            // 1) Flush acknowledged-but-uncommitted operations to local WAL (fsync only, no manifest publish).
            if (shardWriter.uncommittedOperations.get() > 0) {
                shardWriter.writer.commit();
                shardWriter.uncommittedOperations.set(0);
                shardWriter.lastCommitNanos.set(System.nanoTime());
            }
            // 2) Drain committed content to S3. Upload only: publishing here would interleave a
            // divergent segment line into the snapshot sequence owned by the new history.
            boolean drained;
            try {
                drained = shardWriter.directory.uploadLocalCommit()
                        .get(manifestOptions.uploadWaitTimeout().toMillis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                drained = false;
            } catch (ExecutionException | TimeoutException e) {
                log.warn("failed to drain local uploads for retired shard {}", shardId.routeKey(), e);
                drained = false;
            }
            if (!drained) {
                // Keep the writer and local files; reconcile retries on the next tick. Never
                // quarantine while content may exist only on local disk.
                log.warn("deferred retire of shard {}: pending uploads did not drain", shardId.routeKey());
                return;
            }
            // 3) Close PITs pinned to this shard's local lineage, then close and remove the writer.
            closePitsFor(shardId);
            if (writers.remove(shardId, shardWriter)) {
                shardWriter.close();
            }
        }
        // 4) Quarantine the WAL directory: after reassignment the shard's history continues from the
        // remote snapshot, and stale same-name files would shadow the new content on openInput.
        quarantineShardDirectories(shardId);
    }

    private void drainAndQuarantine(ShardId shardId, boolean drainUploads) throws IOException {
        Path walPath = PathUtil.walDataPath(basePath, physicalIndexName(shardId));
        if (!Files.isDirectory(walPath)) {
            return;
        }
        if (drainUploads && hasCommittedSegmentFile(walPath)) {
            try (S3CachingDirectory directory = openShardDirectory(shardId, remoteSnapshotStatuses())) {
                boolean uploaded = directory.uploadLocalCommit()
                        .get(manifestOptions.uploadWaitTimeout().toMillis(), TimeUnit.MILLISECONDS);
                if (!uploaded) {
                    log.warn("deferred quarantine of {}: pending uploads did not drain", shardId.routeKey());
                    return;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (ExecutionException | TimeoutException e) {
                log.warn("failed to drain local uploads for {}: {}", shardId.routeKey(), e.toString());
                return;
            }
        }
        quarantineShardDirectories(shardId);
    }

    private void quarantineShardDirectories(ShardId shardId) throws IOException {
        String physical = physicalIndexName(shardId);
        Path orphanRoot = basePath.resolve("orphan-wal");
        IOException failure = quarantineDirectory(orphanRoot,
                basePath.resolve(Hierarchy.WAL.path).resolve(physical), physical);
        failure = quarantineDirectory(orphanRoot,
                basePath.resolve(Hierarchy.SHARED.path).resolve(physical), physical);
        if (failure != null) {
            throw failure;
        }
    }

    private IOException quarantineDirectory(Path orphanRoot, Path source, String physical) throws IOException {
        if (!Files.exists(source)) {
            return null;
        }
        try {
            Files.createDirectories(orphanRoot);
            // Same-volume rename: atomically removes the directory from wal/ scans while keeping the
            // data on disk for forensics. openInput prefers WAL files without checksum validation, so
            // leftover same-name files MUST not remain in place once ownership has moved on.
            Files.move(source, orphanRoot.resolve(physical + "-" + UUID.randomUUID()));
            log.info("quarantined local shard directory {} after ownership loss", source);
            return null;
        } catch (IOException e) {
            log.warn("failed to quarantine local shard directory {}", source, e);
            return e;
        }
    }

    private Collection<ShardId> localWalShardIds() throws IOException {
        Path walRoot = basePath.resolve(Hierarchy.WAL.path);
        if (!Files.isDirectory(walRoot)) {
            return List.of();
        }
        List<ShardId> shardIds = new ArrayList<>();
        try (var stream = Files.list(walRoot)) {
            for (Path dir : (Iterable<Path>) stream::iterator) {
                String name = dir.getFileName().toString();
                int suffix = name.lastIndexOf("__shard_");
                if (suffix <= 0) {
                    continue;
                }
                try {
                    shardIds.add(new ShardId(name.substring(0, suffix),
                            Integer.parseInt(name.substring(suffix + "__shard_".length()))));
                } catch (NumberFormatException ignored) {
                    // Not a physical shard directory.
                }
            }
        }
        return shardIds;
    }

    private void closePitsFor(ShardId shardId) {
        for (String pitId : List.copyOf(pits.keySet())) {
            PitContext pit = pits.get(pitId);
            if (pit != null && shardId.equals(pit.shardId())) {
                try {
                    closePointInTime(pitId);
                } catch (IOException e) {
                    log.warn("failed to close point in time {} for retired shard {}", pitId, shardId.routeKey(), e);
                }
            }
        }
    }

    @Override
    public int openPointInTimeCount() {
        cleanupExpiredPits();
        return pits.size();
    }

    private void discardPendingUploads(ShardId shardId) {
        try (ManifestManager manifestManager = openManifestManager()) {
            manifestManager.discardPendingUploads(physicalIndexName(shardId));
        }
    }

    private ShardWriter openShardWriter(ShardId shardId) {
        try {
            S3CachingDirectory directory = openShardDirectory(shardId, remoteSnapshotStatuses());
            Analyzer analyzer = new MappingAnalyzer(shardId, mappingsByShard, analyzerRegistry);
            IndexWriterConfig config = new IndexWriterConfig(analyzer);
            config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
            SnapshotDeletionPolicy deletionPolicy = new SnapshotDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy());
            config.setIndexDeletionPolicy(deletionPolicy);
            IndexWriter writer = new IndexWriter(directory, config);
            try {
                return new ShardWriter(
                        shardId,
                        directory,
                        writer,
                        new SearcherManager(writer, null),
                        analyzer,
                        deletionPolicy,
                        new ArrayList<>(),
                        new AtomicInteger(),
                        new AtomicInteger(),
                        new AtomicInteger(),
                        new AtomicLong(System.nanoTime()),
                        new AtomicLong(System.nanoTime()),
                        new AtomicBoolean(false),
                        new AtomicBoolean(false)
                );
            } catch (IOException | RuntimeException e) {
                try {
                    writer.close();
                } finally {
                    analyzer.close();
                    directory.close();
                }
                throw e;
            }
        } catch (IOException e) {
            throw new IllegalStateException("failed to open shard writer: " + shardId.routeKey(), e);
        }
    }

    private S3CachingDirectory openShardDirectory(ShardId shardId) throws IOException {
        return openShardDirectory(shardId, remoteSnapshotStatuses(), false);
    }

    private S3CachingDirectory openShardDirectory(ShardId shardId, List<IndexFileStatus> readableRemoteStatuses) throws IOException {
        return openShardDirectory(shardId, readableRemoteStatuses, true);
    }

    private S3CachingDirectory openShardDirectory(
            ShardId shardId,
            List<IndexFileStatus> readableRemoteStatuses,
            boolean includeWalFiles
    ) throws IOException {
        return openShardDirectory(shardId, readableRemoteStatuses, includeWalFiles, null);
    }

    private S3CachingDirectory openShardDirectory(
            ShardId shardId,
            List<IndexFileStatus> readableRemoteStatuses,
            boolean includeWalFiles,
            Long remoteSnapshotGeneration
    ) throws IOException {
        String physicalIndexName = physicalIndexName(shardId);
        return new S3CachingDirectory(
                new S3DirectoryOptions(basePath, physicalIndexName),
                new S3LockFactory(),
                openManifestManager(),
                readableRemoteStatuses,
                includeWalFiles,
                remoteSnapshotGeneration
        );
    }

    private ManifestManager openManifestManager() {
        return new ManifestManager(manifestOptions, remoteObjectStore, metadataManager, uploadWorkerPool);
    }

    private void finishWrite(ShardWriter shardWriter, int changedOperations) throws IOException {
        finishWrite(shardWriter, changedOperations, false, false);
    }

    private void finishWrite(
            ShardWriter shardWriter,
            int changedOperations,
            boolean forceCommit,
            boolean forceRefresh
    ) throws IOException {
        if (changedOperations > 0) {
            shardWriter.uncommittedOperations.addAndGet(changedOperations);
            shardWriter.refreshPending.set(true);
        }
        if (forceCommit || shouldCommit(shardWriter)) {
            commitAndPublish(shardWriter);
        }
        if (forceRefresh || writeOptions.refreshPolicy() == IndexWriteOptions.RefreshPolicy.IMMEDIATE) {
            refreshSearcher(shardWriter, forceRefresh);
        }
    }

    @Override
    public void runWriteMaintenance() throws IOException {
        runWriteMaintenance(List.copyOf(writers.keySet()));
    }

    @Override
    public void runWriteMaintenance(Collection<ShardId> shardIds) throws IOException {
        IOException failure = null;
        for (ShardId shardId : new LinkedHashSet<>(shardIds == null ? List.copyOf(writers.keySet()) : shardIds)) {
            ShardWriter shardWriter = writers.get(shardId);
            if (shardWriter == null) {
                continue;
            }
            synchronized (shardWriter) {
                try {
                    // Also re-drive the publish when the previous commit's snapshot has not been
                    // published yet (slow/failed upload): with async upload the local commit is
                    // already acknowledged, so shrinking the unpublished window shrinks the
                    // failover loss window.
                    if (shouldCommit(shardWriter) || shardWriter.snapshotPublishPending.get()) {
                        commitAndPublish(shardWriter);
                    }
                    if (shouldRefresh(shardWriter)) {
                        refreshSearcher(shardWriter);
                    }
                } catch (IOException e) {
                    failure = addFailure(failure, e);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private boolean shouldCommit(ShardWriter shardWriter) {
        int uncommittedOperations = shardWriter.uncommittedOperations.get();
        if (uncommittedOperations <= 0) {
            return false;
        }
        if (writeOptions.commitEveryRequest()) {
            return true;
        }
        if (writeOptions.commitAfterDocs() > 0 && uncommittedOperations >= writeOptions.commitAfterDocs()) {
            return true;
        }
        return !writeOptions.commitInterval().isZero()
                && System.nanoTime() - shardWriter.lastCommitNanos.get() >= writeOptions.commitInterval().toNanos();
    }

    private boolean shouldRefresh(ShardWriter shardWriter) {
        if (!shardWriter.refreshPending.get()
                || writeOptions.refreshPolicy() != IndexWriteOptions.RefreshPolicy.INTERVAL) {
            return false;
        }
        return System.nanoTime() - shardWriter.lastRefreshNanos.get() >= writeOptions.refreshInterval().toNanos();
    }

    private void refreshSearcher(ShardWriter shardWriter) throws IOException {
        refreshSearcher(shardWriter, false);
    }

    private void refreshSearcher(ShardWriter shardWriter, boolean force) throws IOException {
        if (!force && !shardWriter.refreshPending.get()) {
            return;
        }
        shardWriter.searcherManager.maybeRefreshBlocking();
        shardWriter.refreshPending.set(false);
        shardWriter.lastRefreshNanos.set(System.nanoTime());
    }

    private void commitAndPublish(ShardWriter shardWriter) throws IOException {
        long commitStarted = System.nanoTime();
        shardWriter.writer.commit();
        shardWriter.uncommittedOperations.set(0);
        shardWriter.lastCommitNanos.set(System.nanoTime());
        warnSlowCommit(shardWriter, commitStarted);
        IndexCommit commit = shardWriter.deletionPolicy.snapshot();
        RetainedSnapshotCommit retained = retainSnapshotCommit(shardWriter, commit);
        shardWriter.snapshotPublishPending.set(true);
        try {
            shardWriter.directory.publishIndexCommit(commit)
                    .whenComplete((published, throwable) -> {
                        if (throwable != null) {
                            log.warn("failed to publish snapshot for {}", shardWriter.shardId.routeKey(), throwable);
                        }
                        if (Boolean.TRUE.equals(published)) {
                            shardWriter.snapshotPublishPending.set(false);
                            markCleanSnapshotPublished(shardWriter, retained.sequence);
                        }
                        completeSnapshotPublish(shardWriter, retained);
                    });
        } catch (IOException | RuntimeException e) {
            releaseSnapshotCommit(shardWriter, retained);
            throw e;
        }
    }

    private void warnSlowCommit(ShardWriter shardWriter, long started) {
        long elapsedNanos = System.nanoTime() - started;
        if (elapsedNanos >= SLOW_COMMIT_WARN_NANOS) {
            log.warn(
                    "slow Lucene commit for shard {} took {}ms; this time is inside IndexWriter.commit() and usually means flush/fsync or merge pressure on local WAL",
                    shardWriter.shardId.routeKey(),
                    elapsedNanos / 1_000_000
            );
        }
    }

    private int saturatedInt(long value) {
        return value >= Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) value;
    }

    private RetainedSnapshotCommit retainSnapshotCommit(ShardWriter shardWriter, IndexCommit commit) {
        synchronized (shardWriter) {
            RetainedSnapshotCommit retained = new RetainedSnapshotCommit(
                    commit,
                    shardWriter.nextSnapshotSequence.incrementAndGet()
            );
            shardWriter.retainedSnapshotCommits.add(retained);
            releaseReclaimableSnapshotCommits(shardWriter);
            return retained;
        }
    }

    private void completeSnapshotPublish(ShardWriter shardWriter, RetainedSnapshotCommit retained) {
        synchronized (shardWriter) {
            retained.completed = true;
            releaseReclaimableSnapshotCommits(shardWriter);
        }
    }

    private void markCleanSnapshotPublished(ShardWriter shardWriter, int sequence) {
        synchronized (shardWriter) {
            shardWriter.cleanPublishedSnapshotSequence.accumulateAndGet(sequence, Math::max);
        }
    }

    private void markLatestSnapshotPublished(ShardWriter shardWriter) {
        markCleanSnapshotPublished(shardWriter, shardWriter.nextSnapshotSequence.get());
    }

    private void releaseSnapshotCommit(ShardWriter shardWriter, RetainedSnapshotCommit retained) {
        synchronized (shardWriter) {
            if (!shardWriter.retainedSnapshotCommits.remove(retained)) {
                return;
            }
            try {
                shardWriter.deletionPolicy.release(retained.commit);
                shardWriter.writer.deleteUnusedFiles();
            } catch (Exception e) {
                log.warn("failed to release snapshot for {}", shardWriter.shardId.routeKey(), e);
            }
        }
    }

    private void releaseReclaimableSnapshotCommits(ShardWriter shardWriter) {
        synchronized (shardWriter) {
            int latestSequence = shardWriter.nextSnapshotSequence.get();
            int cleanPublishedSequence = shardWriter.cleanPublishedSnapshotSequence.get();
            long nowNanos = System.nanoTime();
            boolean released = false;
            for (Iterator<RetainedSnapshotCommit> iterator = shardWriter.retainedSnapshotCommits.iterator(); iterator.hasNext(); ) {
                RetainedSnapshotCommit retained = iterator.next();
                if (!retained.completed) {
                    // The async publish future never completed (e.g. upload worker stalled or the
                    // process recovered after a crash). Force-release so the IndexCommit snapshot
                    // and its segment files do not leak forever.
                    if (retained.publishTimedOut(nowNanos)) {
                        log.warn("force-releasing timed-out snapshot publish for shard {} sequence {}",
                                shardWriter.shardId.routeKey(), retained.sequence);
                    } else {
                        continue;
                    }
                }
                if (retained.completed && retained.sequence >= latestSequence && retained.sequence > cleanPublishedSequence) {
                    continue;
                }
                try {
                    shardWriter.deletionPolicy.release(retained.commit);
                    iterator.remove();
                    released = true;
                } catch (Exception e) {
                    log.warn("failed to release snapshot for {}", shardWriter.shardId.routeKey(), e);
                }
            }
            if (released) {
                try {
                    shardWriter.writer.deleteUnusedFiles();
                } catch (Exception e) {
                    log.warn("failed to delete unused files for {}", shardWriter.shardId.routeKey(), e);
                }
            }
        }
    }

    private void rememberMappings(ShardId shardId, Map<String, FieldMapping> mappings) {
        if (shardId != null && mappings != null && !mappings.isEmpty()) {
            mappingsByShard.put(shardId, Map.copyOf(mappings));
        }
    }

    private List<IndexFileStatus> remoteSnapshotStatuses() {
        return List.of(
                IndexFileStatus.CLEAN,
                IndexFileStatus.PINNED
        );
    }

    private void deleteRecursively(Path path) throws IOException {
        if (!Files.exists(path)) {
            return;
        }
        IOException failure = null;
        for (int attempt = 0; attempt < 20; attempt++) {
            try {
                deleteRecursivelyOnce(path);
                return;
            } catch (IOException e) {
                failure = e;
                sleepBeforeDeleteRetry(attempt, path);
            }
        }
        throw failure;
    }

    private void deleteRecursivelyOnce(Path path) throws IOException {
        if (!Files.exists(path)) {
            return;
        }
        Files.walkFileTree(path, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                deleteIfExists(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException e) throws IOException {
                if (e instanceof NoSuchFileException || !Files.exists(file)) {
                    return FileVisitResult.CONTINUE;
                }
                throw e;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException e) throws IOException {
                if (e != null && !(e instanceof NoSuchFileException)) {
                    throw e;
                }
                deleteIfExists(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    private void deleteIfExists(Path path) throws IOException {
        try {
            Files.deleteIfExists(path);
        } catch (NoSuchFileException ignored) {
            // Another maintenance task or cache cleanup can remove cache files while index deletion is walking them.
        }
    }

    private void sleepBeforeDeleteRetry(int attempt, Path path) throws IOException {
        try {
            Thread.sleep(50L * (attempt + 1));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while retrying delete: " + path, e);
        }
    }

    private IOException addFailure(IOException failure, IOException e) {
        if (failure == null) {
            return e;
        }
        failure.addSuppressed(e);
        return failure;
    }

    private Document document(
            String indexName,
            ShardId shardId,
            String id,
            Map<String, Object> source,
            Map<String, FieldMapping> mappings
    ) {
        Document document = new Document();
        document.add(new StringField("_id", id, Field.Store.YES));
        document.add(new SortedDocValuesField("_id", new BytesRef(id)));
        document.add(new StringField("_index", indexName, Field.Store.YES));
        document.add(new StringField("_shard", shardId.routeKey(), Field.Store.YES));
        document.add(new StoredField("_source", new String(JsonUtil.writeValueAsBytes(source), StandardCharsets.UTF_8)));
        mappings.forEach((field, mapping) -> {
            Object value = source.get(field);
            if (value != null) {
                document.add(new StringField(FIELD_NAMES_FIELD, field, Field.Store.NO));
                addMappedField(document, field, value, mapping);
            }
        });
        return document;
    }

    private void addMappedField(Document document, String field, Object value, FieldMapping mapping) {
        if (mapping.denseVector() && Boolean.TRUE.equals(mapping.indexed())) {
            float[] vector = vectorValue(value);
            if (vector == null) {
                throw new IllegalArgumentException("dense_vector field must be a non-empty numeric array: " + field);
            }
            if (vector.length != mapping.dimension()) {
                throw new IllegalArgumentException("dense_vector field dimension mismatch: " + field);
            }
            document.add(new KnnFloatVectorField(field, vector, similarity(mapping)));
            return;
        }
        if (mapping.byteVector() && Boolean.TRUE.equals(mapping.indexed())) {
            byte[] vector = byteVectorValue(value);
            if (vector == null) {
                throw new IllegalArgumentException("byte_vector field must be a non-empty numeric array: " + field);
            }
            if (vector.length != mapping.dimension()) {
                throw new IllegalArgumentException("byte_vector field dimension mismatch: " + field);
            }
            document.add(new KnnByteVectorField(field, vector, similarity(mapping)));
            return;
        }
        if (mapping.keyword()) {
            for (Object item : values(value, mapping)) {
                String string = String.valueOf(item);
                if (Boolean.TRUE.equals(mapping.indexed())) {
                    document.add(new StringField(field, string, store(mapping)));
                }
                if (Boolean.TRUE.equals(mapping.docValues())) {
                    if (mapping.multiValued()) {
                        document.add(new SortedSetDocValuesField(field, new BytesRef(string)));
                    } else {
                        document.add(new SortedDocValuesField(field, new BytesRef(string)));
                    }
                }
            }
            return;
        }
        if (mapping.text()) {
            if (Boolean.TRUE.equals(mapping.indexed())) {
                document.add(new TextField(field, String.valueOf(value), store(mapping)));
            }
            return;
        }
        if (mapping.longNumber()) {
            for (Object item : values(value, mapping)) {
                Long number = longValue(item);
                if (number == null) {
                    continue;
                }
                if (Boolean.TRUE.equals(mapping.indexed())) {
                    document.add(new LongPoint(field, number));
                }
                if (Boolean.TRUE.equals(mapping.docValues())) {
                    if (mapping.multiValued()) {
                        document.add(new SortedNumericDocValuesField(field, number));
                    } else {
                        document.add(new NumericDocValuesField(field, number));
                    }
                }
                if (Boolean.TRUE.equals(mapping.stored())) {
                    document.add(new StoredField(field, number));
                }
            }
            return;
        }
        if (mapping.doubleNumber()) {
            for (Object item : values(value, mapping)) {
                Double number = doubleValue(item);
                if (number == null) {
                    continue;
                }
                if (Boolean.TRUE.equals(mapping.indexed())) {
                    document.add(new DoublePoint(field, number));
                }
                if (Boolean.TRUE.equals(mapping.docValues())) {
                    if (mapping.multiValued()) {
                        document.add(new SortedNumericDocValuesField(field, NumericUtils.doubleToSortableLong(number)));
                    } else {
                        document.add(new DoubleDocValuesField(field, number));
                    }
                }
                if (Boolean.TRUE.equals(mapping.stored())) {
                    document.add(new StoredField(field, number));
                }
            }
            return;
        }
        if (mapping.bool()) {
            for (Object item : values(value, mapping)) {
                String string = String.valueOf(booleanValue(item));
                if (Boolean.TRUE.equals(mapping.indexed())) {
                    document.add(new StringField(field, string, store(mapping)));
                }
                if (Boolean.TRUE.equals(mapping.docValues())) {
                    if (mapping.multiValued()) {
                        document.add(new SortedSetDocValuesField(field, new BytesRef(string)));
                    } else {
                        document.add(new SortedDocValuesField(field, new BytesRef(string)));
                    }
                }
            }
            return;
        }
        if (mapping.date()) {
            for (Object item : values(value, mapping)) {
                Long epoch = requiredDate(item, field, mapping);
                if (Boolean.TRUE.equals(mapping.indexed())) {
                    document.add(new LongPoint(field, epoch));
                }
                if (Boolean.TRUE.equals(mapping.docValues())) {
                    if (mapping.multiValued()) {
                        document.add(new SortedNumericDocValuesField(field, epoch));
                    } else {
                        document.add(new NumericDocValuesField(field, epoch));
                    }
                }
                if (Boolean.TRUE.equals(mapping.stored())) {
                    document.add(new StoredField(field, epoch));
                }
            }
            return;
        }
        if (mapping.ip()) {
            for (Object item : values(value, mapping)) {
                InetAddress address = requiredIp(item, field);
                if (Boolean.TRUE.equals(mapping.indexed())) {
                    document.add(new InetAddressPoint(field, address));
                }
                if (Boolean.TRUE.equals(mapping.docValues()) && !mapping.multiValued()) {
                    document.add(new BinaryDocValuesField(field, new BytesRef(InetAddressPoint.encode(address))));
                }
                if (Boolean.TRUE.equals(mapping.stored())) {
                    document.add(new StoredField(field, address.getHostAddress()));
                }
            }
            return;
        }
        if (mapping.binary()) {
            for (Object item : values(value, mapping)) {
                byte[] bytes = binaryValue(item);
                if (bytes == null) {
                    continue;
                }
                if (Boolean.TRUE.equals(mapping.indexed())) {
                    document.add(new BinaryPoint(field, bytes));
                }
                if (Boolean.TRUE.equals(mapping.docValues()) && !mapping.multiValued()) {
                    document.add(new BinaryDocValuesField(field, new BytesRef(bytes)));
                }
                if (Boolean.TRUE.equals(mapping.stored())) {
                    document.add(new StoredField(field, bytes));
                }
            }
            return;
        }
        if (mapping.geoPoint()) {
            double[] point = geoPointValue(value);
            if (point == null) {
                return;
            }
            if (Boolean.TRUE.equals(mapping.indexed())) {
                document.add(new LatLonPoint(field, point[0], point[1]));
            }
            if (Boolean.TRUE.equals(mapping.docValues())) {
                document.add(new LatLonDocValuesField(field, point[0], point[1]));
            }
            return;
        }
        if (mapping.longRange()) {
            long[] range = longRangeValue(value, mapping);
            if (range != null && Boolean.TRUE.equals(mapping.indexed())) {
                document.add(new LongRange(field, new long[]{range[0]}, new long[]{range[1]}));
            }
            return;
        }
        if (mapping.doubleRange()) {
            double[] range = doubleRangeValue(value);
            if (range != null && Boolean.TRUE.equals(mapping.indexed())) {
                document.add(new DoubleRange(field, new double[]{range[0]}, new double[]{range[1]}));
            }
            return;
        }
        if (mapping.ipRange()) {
            InetAddress[] range = ipRangeValue(value);
            if (range != null && Boolean.TRUE.equals(mapping.indexed())) {
                document.add(new InetAddressRange(field, range[0], range[1]));
            }
        }
    }

    private Map<String, Object> source(Document document) {
        String source = document.get("_source");
        return source == null || source.isBlank() ? Map.of() : JsonUtil.readValueAsMap(source);
    }

    private Object docValue(IndexSearcher searcher, int docId, String field, FieldMapping mapping) throws IOException {
        if (mapping.multiValued()) {
            return null;
        }
        LeafReaderContext leaf = leaf(searcher, docId);
        if (leaf == null) {
            return null;
        }
        int leafDocId = docId - leaf.docBase;
        if (mapping.keyword() || mapping.bool()) {
            SortedDocValues values = DocValues.getSorted(leaf.reader(), field);
            if (!values.advanceExact(leafDocId)) {
                return null;
            }
            return values.lookupOrd(values.ordValue()).utf8ToString();
        }
        if (mapping.longNumber() || mapping.date()) {
            NumericDocValues values = DocValues.getNumeric(leaf.reader(), field);
            return values.advanceExact(leafDocId) ? values.longValue() : null;
        }
        if (mapping.doubleNumber()) {
            NumericDocValues values = DocValues.getNumeric(leaf.reader(), field);
            return values.advanceExact(leafDocId) ? Double.longBitsToDouble(values.longValue()) : null;
        }
        if (mapping.ip() || mapping.binary()) {
            BinaryDocValues values = DocValues.getBinary(leaf.reader(), field);
            if (!values.advanceExact(leafDocId)) {
                return null;
            }
            BytesRef bytes = values.binaryValue();
            byte[] copy = Arrays.copyOfRange(bytes.bytes, bytes.offset, bytes.offset + bytes.length);
            if (mapping.ip()) {
                return InetAddressPoint.decode(copy).getHostAddress();
            }
            return Base64.getEncoder().encodeToString(copy);
        }
        return null;
    }

    private LeafReaderContext leaf(IndexSearcher searcher, int docId) {
        for (LeafReaderContext leaf : searcher.getIndexReader().leaves()) {
            if (docId >= leaf.docBase && docId < leaf.docBase + leaf.reader().maxDoc()) {
                return leaf;
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> mapValue(Object value) {
        return value instanceof Map<?, ?> map ? (Map<String, Object>) map : Map.of();
    }

    private String stringValue(Object value) {
        return value == null ? null : String.valueOf(value);
    }

    private Query searchQuery(SearchRequest request) {
        if (request.vector() == null) {
            return query(request.query(), request.mappings());
        }
        if (request.vector().field() == null || request.vector().field().isBlank()) {
            throw new IllegalArgumentException("knn field is required");
        }
        FieldMapping mapping = request.mappings().get(request.vector().field());
        if (mapping == null) {
            throw new IllegalArgumentException("knn field is not mapped: " + request.vector().field());
        }
        if (!mapping.denseVector() && !mapping.byteVector()) {
            throw new IllegalArgumentException("knn field is not a vector field: " + request.vector().field());
        }
        int k = Math.max(1, request.vector().k());
        Query filter = filterQuery(request.query(), request.mappings());
        if (mapping.byteVector()) {
            byte[] byteTarget = byteVectorValue(request.vector().vector());
            if (byteTarget == null) {
                throw new IllegalArgumentException("knn query_vector must be a non-empty byte array");
            }
            if (byteTarget.length != mapping.dimension()) {
                throw new IllegalArgumentException("knn query_vector dimension mismatch: " + request.vector().field());
            }
            return filter == null
                    ? KnnByteVectorField.newVectorQuery(request.vector().field(), byteTarget, k)
                    : new KnnByteVectorQuery(request.vector().field(), byteTarget, k, filter);
        }
        float[] target = vectorValue(request.vector().vector());
        if (target == null) {
            throw new IllegalArgumentException("knn query_vector must be a non-empty numeric array");
        }
        if (target.length != mapping.dimension()) {
            throw new IllegalArgumentException("knn query_vector dimension mismatch: " + request.vector().field());
        }
        return filter == null
                ? new KnnFloatVectorQuery(request.vector().field(), target, k)
                : new KnnFloatVectorQuery(request.vector().field(), target, k, filter);
    }

    private Query filterQuery(Map<String, Object> query, Map<String, FieldMapping> mappings) {
        if (query == null || query.isEmpty() || query.containsKey("match_all")) {
            return null;
        }
        return query(query, mappings);
    }

    private VectorSimilarityFunction similarity(FieldMapping mapping) {
        return switch (mapping.similarity()) {
            case "dot_product" -> VectorSimilarityFunction.DOT_PRODUCT;
            case "euclidean" -> VectorSimilarityFunction.EUCLIDEAN;
            case "maximum_inner_product" -> VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT;
            default -> VectorSimilarityFunction.COSINE;
        };
    }

    private float[] vectorValue(Object value) {
        if (!(value instanceof List<?> list) || list.isEmpty()) {
            return null;
        }
        float[] vector = new float[list.size()];
        for (int i = 0; i < list.size(); i++) {
            Object item = list.get(i);
            if (!(item instanceof Number number)) {
                return null;
            }
            vector[i] = number.floatValue();
        }
        return vector;
    }

    private byte[] byteVectorValue(Object value) {
        if (!(value instanceof List<?> list) || list.isEmpty()) {
            return null;
        }
        byte[] vector = new byte[list.size()];
        for (int i = 0; i < list.size(); i++) {
            Object item = list.get(i);
            if (!(item instanceof Number number)) {
                return null;
            }
            int intValue = number.intValue();
            if (intValue < Byte.MIN_VALUE || intValue > Byte.MAX_VALUE || number.doubleValue() != intValue) {
                return null;
            }
            vector[i] = (byte) intValue;
        }
        return vector;
    }

    private List<Object> values(Object value, FieldMapping mapping) {
        if (mapping.multiValued() && value instanceof List<?> list) {
            return list.stream().filter(Objects::nonNull).map(item -> (Object) item).toList();
        }
        return value == null ? List.of() : List.of(value);
    }

    private Query query(Map<String, Object> query, Map<String, FieldMapping> mappings) {
        if (query == null || query.isEmpty() || query.containsKey("match_all")) {
            return MatchAllDocsQuery.INSTANCE;
        }
        Object ids = query.get("ids");
        if (ids instanceof Map<?, ?> idsMap && idsMap.get("values") instanceof Iterable<?> values) {
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            for (Object value : values) {
                builder.add(new TermQuery(new Term("_id", String.valueOf(value))), BooleanClause.Occur.SHOULD);
            }
            return builder.build();
        }
        Object term = query.get("term");
        if (term instanceof Map<?, ?> termMap) {
            var entry = termMap.entrySet().stream().findFirst();
            if (entry.isPresent()) {
                return exactQuery(String.valueOf(entry.get().getKey()), entry.get().getValue(), mappings);
            }
        }
        Object terms = query.get("terms");
        if (terms instanceof Map<?, ?> termsMap) {
            var entry = termsMap.entrySet().stream().findFirst();
            if (entry.isPresent()) {
                return termsQuery(String.valueOf(entry.get().getKey()), entry.get().getValue(), mappings);
            }
        }
        Object prefix = query.get("prefix");
        if (prefix instanceof Map<?, ?> prefixMap) {
            var entry = prefixMap.entrySet().stream().findFirst();
            if (entry.isPresent()) {
                return prefixQuery(String.valueOf(entry.get().getKey()), entry.get().getValue(), mappings);
            }
        }
        Object exists = query.get("exists");
        if (exists instanceof Map<?, ?> existsMap) {
            return existsQuery(stringValue(existsMap.get("field")), mappings);
        }
        Object match = query.get("match");
        if (match instanceof Map<?, ?> matchMap) {
            var entry = matchMap.entrySet().stream().findFirst();
            if (entry.isPresent()) {
                return matchQuery(String.valueOf(entry.get().getKey()), entry.get().getValue(), mappings);
            }
        }
        Object bool = query.get("bool");
        if (bool instanceof Map<?, ?> boolMap) {
            return boolQuery(boolMap, mappings);
        }
        Object range = query.get("range");
        if (range instanceof Map<?, ?> rangeMap) {
            var entry = rangeMap.entrySet().stream().findFirst();
            if (entry.isPresent() && entry.get().getValue() instanceof Map<?, ?> bounds) {
                return rangeQuery(String.valueOf(entry.get().getKey()), bounds, mappings);
            }
        }
        Object geoDistance = query.get("geo_distance");
        if (geoDistance instanceof Map<?, ?> distanceMap) {
            return geoDistanceQuery(distanceMap, mappings);
        }
        Object geoBoundingBox = query.get("geo_bounding_box");
        if (geoBoundingBox instanceof Map<?, ?> boxMap) {
            return geoBoundingBoxQuery(boxMap, mappings);
        }
        throw new IllegalArgumentException("unsupported query; supported: match_all, ids.values, term, terms, prefix, exists, match, range, geo_distance, geo_bounding_box");
    }

    private Query termsQuery(String field, Object value, Map<String, FieldMapping> mappings) {
        if (!(value instanceof Iterable<?> values)) {
            throw new IllegalArgumentException("terms query requires an array for field: " + field);
        }
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        for (Object item : values) {
            builder.add(exactQuery(field, item, mappings), BooleanClause.Occur.SHOULD);
        }
        BooleanQuery built = builder.build();
        return built.clauses().isEmpty() ? new MatchNoDocsQuery("terms query has no values") : built;
    }

    private Query prefixQuery(String field, Object value, Map<String, FieldMapping> mappings) {
        Object prefixValue = value;
        if (value instanceof Map<?, ?> map) {
            prefixValue = map.get("value");
        }
        String prefix = stringValue(prefixValue);
        if (prefix == null) {
            throw new IllegalArgumentException("prefix query requires value for field: " + field);
        }
        if ("_id".equals(field)) {
            return new PrefixQuery(new Term("_id", prefix));
        }
        FieldMapping mapping = mappings.get(field);
        if (mapping == null || mapping.keyword() || mapping.text() || mapping.bool()) {
            return new PrefixQuery(new Term(field, prefix));
        }
        throw new IllegalArgumentException("prefix query is not supported for field: " + field);
    }

    private Query existsQuery(String field, Map<String, FieldMapping> mappings) {
        if (field == null || field.isBlank()) {
            throw new IllegalArgumentException("exists query requires field");
        }
        if ("_id".equals(field)) {
            return new FieldExistsQuery("_id");
        }
        FieldMapping mapping = mappings.get(field);
        if (mapping == null) {
            return new MatchNoDocsQuery("exists field is not mapped: " + field);
        }
        Query markerQuery = new TermQuery(new Term(FIELD_NAMES_FIELD, field));
        if (!fieldExistsQuerySupported(mapping)) {
            return markerQuery;
        }
        return new BooleanQuery.Builder()
                .add(markerQuery, BooleanClause.Occur.SHOULD)
                .add(new FieldExistsQuery(field), BooleanClause.Occur.SHOULD)
                .build();
    }

    private boolean fieldExistsQuerySupported(FieldMapping mapping) {
        return ((mapping.denseVector() || mapping.byteVector() || mapping.text()) && Boolean.TRUE.equals(mapping.indexed()))
                || Boolean.TRUE.equals(mapping.docValues());
    }

    private Query boolQuery(Map<?, ?> boolMap, Map<String, FieldMapping> mappings) {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        addBoolClauses(builder, boolMap.get("filter"), BooleanClause.Occur.FILTER, mappings);
        addBoolClauses(builder, boolMap.get("must"), BooleanClause.Occur.MUST, mappings);
        addBoolClauses(builder, boolMap.get("should"), BooleanClause.Occur.SHOULD, mappings);
        addBoolClauses(builder, boolMap.get("must_not"), BooleanClause.Occur.MUST_NOT, mappings);
        BooleanQuery built = builder.build();
        return built.clauses().isEmpty() ? MatchAllDocsQuery.INSTANCE : built;
    }

    private void addBoolClauses(
            BooleanQuery.Builder builder,
            Object value,
            BooleanClause.Occur occur,
            Map<String, FieldMapping> mappings
    ) {
        if (value == null) {
            return;
        }
        if (value instanceof List<?> list) {
            for (Object item : list) {
                if (item instanceof Map<?, ?> map) {
                    builder.add(query(mapValue(map), mappings), occur);
                }
            }
            return;
        }
        if (value instanceof Map<?, ?> map) {
            builder.add(query(mapValue(map), mappings), occur);
        }
    }

    private Query exactQuery(String field, Object value, Map<String, FieldMapping> mappings) {
        if ("_id".equals(field)) {
            return new TermQuery(new Term("_id", String.valueOf(value)));
        }
        FieldMapping mapping = mappings.get(field);
        if (mapping == null || mapping.keyword() || mapping.text() || mapping.bool()) {
            return new TermQuery(new Term(field, String.valueOf(value)));
        }
        if (mapping.longNumber()) {
            Long number = longValue(value);
            if (number == null) {
                throw new IllegalArgumentException("term value must be numeric for field: " + field);
            }
            return LongPoint.newExactQuery(field, number);
        }
        if (mapping.date()) {
            return LongPoint.newExactQuery(field, requiredDate(value, field, mapping));
        }
        if (mapping.doubleNumber()) {
            Double number = doubleValue(value);
            if (number == null) {
                throw new IllegalArgumentException("term value must be numeric for field: " + field);
            }
            return DoublePoint.newExactQuery(field, number);
        }
        if (mapping.ip()) {
            return InetAddressPoint.newExactQuery(field, requiredIp(value, field));
        }
        if (mapping.binary()) {
            return BinaryPoint.newExactQuery(field, requiredBinary(value, field));
        }
        throw new IllegalArgumentException("term query is not supported for field: " + field);
    }

    private Query matchQuery(String field, Object value, Map<String, FieldMapping> mappings) {
        FieldMapping mapping = mappings.get(field);
        if (mapping == null || !mapping.text()) {
            return exactQuery(field, value, mappings);
        }
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        for (String token : analyze(field, value, mapping)) {
            builder.add(new TermQuery(new Term(field, token)), BooleanClause.Occur.SHOULD);
        }
        BooleanQuery built = builder.build();
        return built.clauses().isEmpty() ? new MatchNoDocsQuery("match query produced no analyzer tokens") : built;
    }

    private List<String> analyze(String field, Object value, FieldMapping mapping) {
        String analyzerName = mapping.searchAnalyzer() == null ? mapping.analyzer() : mapping.searchAnalyzer();
        Analyzer analyzer = analyzerRegistry.analyzer(analyzerName);
        List<String> tokens = new ArrayList<>();
        try (TokenStream stream = analyzer.tokenStream(field, String.valueOf(value))) {
            CharTermAttribute term = stream.addAttribute(CharTermAttribute.class);
            stream.reset();
            while (stream.incrementToken()) {
                String token = term.toString();
                if (!token.isBlank()) {
                    tokens.add(token);
                }
            }
            stream.end();
        } catch (IOException e) {
            throw new IllegalArgumentException("failed to analyze query for field: " + field, e);
        }
        return tokens;
    }

    private Query rangeQuery(String field, Map<?, ?> bounds, Map<String, FieldMapping> mappings) {
        FieldMapping mapping = mappings.get(field);
        if (mapping == null) {
            throw new IllegalArgumentException("range field is not mapped: " + field);
        }
        Object gte = bounds.containsKey("gte") ? bounds.get("gte") : bounds.get("gt");
        Object lte = bounds.containsKey("lte") ? bounds.get("lte") : bounds.get("lt");
        if (mapping.longNumber()) {
            long min = bounds.containsKey("gt") ? requiredLong(gte, field) + 1 : longOrDefault(gte, Long.MIN_VALUE);
            long max = bounds.containsKey("lt") ? requiredLong(lte, field) - 1 : longOrDefault(lte, Long.MAX_VALUE);
            return LongPoint.newRangeQuery(field, min, max);
        }
        if (mapping.date()) {
            long min = bounds.containsKey("gt") ? requiredDate(gte, field, mapping) + 1 : dateOrDefault(gte, Long.MIN_VALUE, mapping);
            long max = bounds.containsKey("lt") ? requiredDate(lte, field, mapping) - 1 : dateOrDefault(lte, Long.MAX_VALUE, mapping);
            return LongPoint.newRangeQuery(field, min, max);
        }
        if (mapping.doubleNumber()) {
            double min = bounds.containsKey("gt") ? DoublePoint.nextUp(requiredDouble(gte, field)) : doubleOrDefault(gte, Double.NEGATIVE_INFINITY);
            double max = bounds.containsKey("lt") ? DoublePoint.nextDown(requiredDouble(lte, field)) : doubleOrDefault(lte, Double.POSITIVE_INFINITY);
            return DoublePoint.newRangeQuery(field, min, max);
        }
        if (mapping.ip()) {
            InetAddress min = bounds.containsKey("gt") ? InetAddressPoint.nextUp(requiredIp(gte, field)) : ipOrDefault(gte, InetAddressPoint.MIN_VALUE);
            InetAddress max = bounds.containsKey("lt") ? InetAddressPoint.nextDown(requiredIp(lte, field)) : ipOrDefault(lte, InetAddressPoint.MAX_VALUE);
            return InetAddressPoint.newRangeQuery(field, min, max);
        }
        if (mapping.longRange()) {
            long[] range = longRangeValue(bounds, mapping);
            if (range == null) {
                throw new IllegalArgumentException("range bound must be a valid range for field: " + field);
            }
            return longRangeQuery(field, range, stringValue(bounds.get("relation")));
        }
        if (mapping.doubleRange()) {
            double[] range = doubleRangeValue(bounds);
            if (range == null) {
                throw new IllegalArgumentException("range bound must be a valid range for field: " + field);
            }
            return doubleRangeQuery(field, range, stringValue(bounds.get("relation")));
        }
        if (mapping.ipRange()) {
            InetAddress[] range = ipRangeValue(bounds);
            if (range == null) {
                throw new IllegalArgumentException("range bound must be a valid range for field: " + field);
            }
            return ipRangeQuery(field, range, stringValue(bounds.get("relation")));
        }
        throw new IllegalArgumentException("range query is not supported for field: " + field);
    }

    private Query geoDistanceQuery(Map<?, ?> distanceMap, Map<String, FieldMapping> mappings) {
        String field = stringValue(distanceMap.get("field"));
        double[] point = null;
        Object distanceValue = distanceMap.containsKey("distance_meters")
                ? distanceMap.get("distance_meters")
                : distanceMap.get("distance");
        if (field != null && !field.isBlank()) {
            point = geoPointValue(distanceMap.containsKey("point") ? distanceMap.get("point") : distanceMap);
        } else {
            for (Map.Entry<?, ?> entry : distanceMap.entrySet()) {
                String key = String.valueOf(entry.getKey());
                if ("distance".equals(key) || "distance_meters".equals(key) || "distanceMeters".equals(key)) {
                    continue;
                }
                FieldMapping mapping = mappings.get(key);
                if (mapping != null && mapping.geoPoint()) {
                    field = key;
                    point = geoPointValue(entry.getValue());
                    break;
                }
            }
        }
        if (field == null || field.isBlank() || point == null) {
            throw new IllegalArgumentException("geo_distance query requires a geo_point field, point, and distance");
        }
        FieldMapping mapping = mappings.get(field);
        if (mapping == null || !mapping.geoPoint()) {
            throw new IllegalArgumentException("geo_distance field is not mapped as geo_point: " + field);
        }
        return LatLonPoint.newDistanceQuery(field, point[0], point[1], distanceMeters(distanceValue));
    }

    private Query geoBoundingBoxQuery(Map<?, ?> boxMap, Map<String, FieldMapping> mappings) {
        String field = stringValue(boxMap.get("field"));
        Map<?, ?> spec = boxMap;
        if (field == null || field.isBlank()) {
            for (Map.Entry<?, ?> entry : boxMap.entrySet()) {
                String key = String.valueOf(entry.getKey());
                FieldMapping mapping = mappings.get(key);
                if (mapping != null && mapping.geoPoint() && entry.getValue() instanceof Map<?, ?> valueMap) {
                    field = key;
                    spec = valueMap;
                    break;
                }
            }
        }
        if (field == null || field.isBlank()) {
            throw new IllegalArgumentException("geo_bounding_box query requires a geo_point field");
        }
        FieldMapping mapping = mappings.get(field);
        if (mapping == null || !mapping.geoPoint()) {
            throw new IllegalArgumentException("geo_bounding_box field is not mapped as geo_point: " + field);
        }
        Double top = doubleValue(spec.get("top"));
        Double bottom = doubleValue(spec.get("bottom"));
        Double left = doubleValue(spec.get("left"));
        Double right = doubleValue(spec.get("right"));
        double[] topLeft = geoPointValue(spec.get("top_left"));
        double[] bottomRight = geoPointValue(spec.get("bottom_right"));
        if (topLeft != null) {
            top = topLeft[0];
            left = topLeft[1];
        }
        if (bottomRight != null) {
            bottom = bottomRight[0];
            right = bottomRight[1];
        }
        if (top == null || bottom == null || left == null || right == null) {
            throw new IllegalArgumentException("geo_bounding_box query requires top, bottom, left, and right");
        }
        return LatLonPoint.newBoxQuery(field, bottom, top, left, right);
    }

    private Query longRangeQuery(String field, long[] range, String relation) {
        return switch (normalizeRelation(relation)) {
            case "contains" -> LongRange.newContainsQuery(field, new long[]{range[0]}, new long[]{range[1]});
            case "within" -> LongRange.newWithinQuery(field, new long[]{range[0]}, new long[]{range[1]});
            case "crosses" -> LongRange.newCrossesQuery(field, new long[]{range[0]}, new long[]{range[1]});
            default -> LongRange.newIntersectsQuery(field, new long[]{range[0]}, new long[]{range[1]});
        };
    }

    private Query doubleRangeQuery(String field, double[] range, String relation) {
        return switch (normalizeRelation(relation)) {
            case "contains" -> DoubleRange.newContainsQuery(field, new double[]{range[0]}, new double[]{range[1]});
            case "within" -> DoubleRange.newWithinQuery(field, new double[]{range[0]}, new double[]{range[1]});
            case "crosses" -> DoubleRange.newCrossesQuery(field, new double[]{range[0]}, new double[]{range[1]});
            default -> DoubleRange.newIntersectsQuery(field, new double[]{range[0]}, new double[]{range[1]});
        };
    }

    private Query ipRangeQuery(String field, InetAddress[] range, String relation) {
        return switch (normalizeRelation(relation)) {
            case "contains" -> InetAddressRange.newContainsQuery(field, range[0], range[1]);
            case "within" -> InetAddressRange.newWithinQuery(field, range[0], range[1]);
            case "crosses" -> InetAddressRange.newCrossesQuery(field, range[0], range[1]);
            default -> InetAddressRange.newIntersectsQuery(field, range[0], range[1]);
        };
    }

    private String normalizeRelation(String relation) {
        return relation == null || relation.isBlank() ? "intersects" : relation.trim().toLowerCase();
    }

    private Field.Store store(FieldMapping mapping) {
        return Boolean.TRUE.equals(mapping.stored()) ? Field.Store.YES : Field.Store.NO;
    }

    private Long longValue(Object value) {
        if (value instanceof Number number) {
            return number.longValue();
        }
        if (value instanceof String string && !string.isBlank()) {
            try {
                return Long.valueOf(string.trim());
            } catch (NumberFormatException ignored) {
                return null;
            }
        }
        return null;
    }

    private long longOrDefault(Object value, long defaultValue) {
        Long number = longValue(value);
        return number == null ? defaultValue : number;
    }

    private long requiredLong(Object value, String field) {
        Long number = longValue(value);
        if (number == null) {
            throw new IllegalArgumentException("range bound must be numeric for field: " + field);
        }
        return number;
    }

    private Double doubleValue(Object value) {
        if (value instanceof Number number) {
            return number.doubleValue();
        }
        if (value instanceof String string && !string.isBlank()) {
            try {
                return Double.valueOf(string.trim());
            } catch (NumberFormatException ignored) {
                return null;
            }
        }
        return null;
    }

    private double doubleOrDefault(Object value, double defaultValue) {
        Double number = doubleValue(value);
        return number == null ? defaultValue : number;
    }

    private double requiredDouble(Object value, String field) {
        Double number = doubleValue(value);
        if (number == null) {
            throw new IllegalArgumentException("range bound must be numeric for field: " + field);
        }
        return number;
    }

    private Boolean booleanValue(Object value) {
        if (value instanceof Boolean bool) {
            return bool;
        }
        if (value instanceof String string) {
            return Boolean.parseBoolean(string);
        }
        if (value instanceof Number number) {
            return number.intValue() != 0;
        }
        return false;
    }

    private Long dateValue(Object value, FieldMapping mapping) {
        if (value instanceof Number number) {
            return number.longValue();
        }
        if (!(value instanceof String string) || string.isBlank()) {
            return null;
        }
        String trimmed = string.trim();
        try {
            return Long.valueOf(trimmed);
        } catch (NumberFormatException ignored) {
        }
        try {
            Instant instant = Instant.parse(trimmed);
            return mapping != null && mapping.dateNanos()
                    ? Math.addExact(Math.multiplyExact(instant.getEpochSecond(), 1_000_000_000L), instant.getNano())
                    : instant.toEpochMilli();
        } catch (DateTimeParseException ignored) {
        } catch (ArithmeticException e) {
            return null;
        }
        try {
            Instant instant = OffsetDateTime.parse(trimmed).toInstant();
            return mapping != null && mapping.dateNanos()
                    ? Math.addExact(Math.multiplyExact(instant.getEpochSecond(), 1_000_000_000L), instant.getNano())
                    : instant.toEpochMilli();
        } catch (DateTimeParseException ignored) {
        } catch (ArithmeticException e) {
            return null;
        }
        try {
            Instant instant = LocalDate.parse(trimmed).atStartOfDay(ZoneOffset.UTC).toInstant();
            return mapping != null && mapping.dateNanos()
                    ? Math.multiplyExact(instant.getEpochSecond(), 1_000_000_000L)
                    : instant.toEpochMilli();
        } catch (DateTimeParseException ignored) {
            return null;
        } catch (ArithmeticException e) {
            return null;
        }
    }

    private long dateOrDefault(Object value, long defaultValue, FieldMapping mapping) {
        Long date = dateValue(value, mapping);
        return date == null ? defaultValue : date;
    }

    private long requiredDate(Object value, String field, FieldMapping mapping) {
        Long date = dateValue(value, mapping);
        if (date == null) {
            throw new IllegalArgumentException("date value must be epoch number, ISO instant, or ISO local date for field: " + field);
        }
        return date;
    }

    private InetAddress ipValue(Object value) {
        if (value instanceof InetAddress address) {
            return address;
        }
        if (!(value instanceof String string) || string.isBlank()) {
            return null;
        }
        try {
            return InetAddress.ofLiteral(string.trim());
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    private InetAddress ipOrDefault(Object value, InetAddress defaultValue) {
        InetAddress address = ipValue(value);
        return address == null ? defaultValue : address;
    }

    private InetAddress requiredIp(Object value, String field) {
        InetAddress address = ipValue(value);
        if (address == null) {
            throw new IllegalArgumentException("ip value must be a valid address for field: " + field);
        }
        return address;
    }

    private byte[] binaryValue(Object value) {
        if (value instanceof byte[] bytes) {
            return bytes;
        }
        if (value instanceof String string && !string.isBlank()) {
            try {
                return Base64.getDecoder().decode(string.trim());
            } catch (IllegalArgumentException ignored) {
                return null;
            }
        }
        if (value instanceof List<?> list && !list.isEmpty()) {
            byte[] bytes = new byte[list.size()];
            for (int i = 0; i < list.size(); i++) {
                Object item = list.get(i);
                if (!(item instanceof Number number)) {
                    return null;
                }
                int intValue = number.intValue();
                if (intValue < Byte.MIN_VALUE || intValue > 255 || number.doubleValue() != intValue) {
                    return null;
                }
                bytes[i] = (byte) intValue;
            }
            return bytes;
        }
        return null;
    }

    private byte[] requiredBinary(Object value, String field) {
        byte[] bytes = binaryValue(value);
        if (bytes == null) {
            throw new IllegalArgumentException("binary value must be base64 or byte array for field: " + field);
        }
        return bytes;
    }

    private double[] geoPointValue(Object value) {
        if (value instanceof Map<?, ?> map) {
            Double lat = doubleValue(map.get("lat"));
            Double lon = doubleValue(map.containsKey("lon") ? map.get("lon") : map.get("lng"));
            return lat == null || lon == null ? null : new double[]{lat, lon};
        }
        if (value instanceof List<?> list && list.size() == 2) {
            Double lat = doubleValue(list.get(0));
            Double lon = doubleValue(list.get(1));
            return lat == null || lon == null ? null : new double[]{lat, lon};
        }
        if (value instanceof String string && !string.isBlank()) {
            String[] parts = string.split(",");
            if (parts.length == 2) {
                Double lat = doubleValue(parts[0].trim());
                Double lon = doubleValue(parts[1].trim());
                return lat == null || lon == null ? null : new double[]{lat, lon};
            }
        }
        return null;
    }

    private double distanceMeters(Object value) {
        if (value instanceof Number number) {
            return number.doubleValue();
        }
        if (!(value instanceof String string) || string.isBlank()) {
            throw new IllegalArgumentException("geo_distance query requires distance");
        }
        String trimmed = string.trim().toLowerCase(Locale.ROOT).replace(" ", "");
        double multiplier = 1.0;
        if (trimmed.endsWith("km")) {
            multiplier = 1000.0;
            trimmed = trimmed.substring(0, trimmed.length() - 2);
        } else if (trimmed.endsWith("mi")) {
            multiplier = 1609.344;
            trimmed = trimmed.substring(0, trimmed.length() - 2);
        } else if (trimmed.endsWith("m")) {
            trimmed = trimmed.substring(0, trimmed.length() - 1);
        }
        Double distance = doubleValue(trimmed);
        if (distance == null) {
            throw new IllegalArgumentException("geo_distance query requires numeric distance");
        }
        return distance * multiplier;
    }

    private long[] longRangeValue(Object value, FieldMapping mapping) {
        if (value instanceof Map<?, ?> map) {
            Object minValue = firstPresent(map, "gte", "from", "min", "lower", "gt");
            Object maxValue = firstPresent(map, "lte", "to", "max", "upper", "lt");
            long min = mapping != null && "date_range".equals(mapping.type())
                    ? dateOrDefault(minValue, Long.MIN_VALUE, mapping)
                    : longOrDefault(minValue, Long.MIN_VALUE);
            long max = mapping != null && "date_range".equals(mapping.type())
                    ? dateOrDefault(maxValue, Long.MAX_VALUE, mapping)
                    : longOrDefault(maxValue, Long.MAX_VALUE);
            if (map.containsKey("gt") && min != Long.MAX_VALUE) {
                min++;
            }
            if (map.containsKey("lt") && max != Long.MIN_VALUE) {
                max--;
            }
            return min <= max ? new long[]{min, max} : null;
        }
        if (value instanceof List<?> list && list.size() == 2) {
            Long min = mapping != null && "date_range".equals(mapping.type()) ? dateValue(list.get(0), mapping) : longValue(list.get(0));
            Long max = mapping != null && "date_range".equals(mapping.type()) ? dateValue(list.get(1), mapping) : longValue(list.get(1));
            return min == null || max == null || min > max ? null : new long[]{min, max};
        }
        return null;
    }

    private double[] doubleRangeValue(Object value) {
        if (value instanceof Map<?, ?> map) {
            Object minValue = firstPresent(map, "gte", "from", "min", "lower", "gt");
            Object maxValue = firstPresent(map, "lte", "to", "max", "upper", "lt");
            double min = doubleOrDefault(minValue, Double.NEGATIVE_INFINITY);
            double max = doubleOrDefault(maxValue, Double.POSITIVE_INFINITY);
            if (map.containsKey("gt")) {
                min = DoublePoint.nextUp(min);
            }
            if (map.containsKey("lt")) {
                max = DoublePoint.nextDown(max);
            }
            return min <= max ? new double[]{min, max} : null;
        }
        if (value instanceof List<?> list && list.size() == 2) {
            Double min = doubleValue(list.get(0));
            Double max = doubleValue(list.get(1));
            return min == null || max == null || min > max ? null : new double[]{min, max};
        }
        return null;
    }

    private InetAddress[] ipRangeValue(Object value) {
        if (value instanceof Map<?, ?> map) {
            Object minValue = firstPresent(map, "gte", "from", "min", "lower", "gt");
            Object maxValue = firstPresent(map, "lte", "to", "max", "upper", "lt");
            InetAddress min = ipOrDefault(minValue, InetAddressPoint.MIN_VALUE);
            InetAddress max = ipOrDefault(maxValue, InetAddressPoint.MAX_VALUE);
            if (map.containsKey("gt")) {
                min = InetAddressPoint.nextUp(min);
            }
            if (map.containsKey("lt")) {
                max = InetAddressPoint.nextDown(max);
            }
            return new InetAddress[]{min, max};
        }
        if (value instanceof List<?> list && list.size() == 2) {
            InetAddress min = ipValue(list.get(0));
            InetAddress max = ipValue(list.get(1));
            return min == null || max == null ? null : new InetAddress[]{min, max};
        }
        return null;
    }

    private Object firstPresent(Map<?, ?> map, String... keys) {
        for (String key : keys) {
            if (map.containsKey(key)) {
                return map.get(key);
            }
        }
        return null;
    }

    private String physicalIndexName(ShardId shardId) {
        return shardId.indexName() + "__shard_" + shardId.shardNumber();
    }

    private boolean hasCommittedSegmentFile(Path walPath) throws IOException {
        try (var paths = Files.list(walPath)) {
            return paths
                    .map(path -> path.getFileName().toString())
                    .anyMatch(this::isCommittedSegmentFile);
        }
    }

    private boolean isCommittedSegmentFile(String name) {
        return name.startsWith("segments_") && !name.startsWith("pending_segments_");
    }

    @Override
    public void close() throws IOException {
        IOException failure = null;
        for (String pitId : List.copyOf(pits.keySet())) {
            try {
                closePointInTime(pitId);
            } catch (IOException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        try {
            flushPendingWrites();
        } catch (IOException e) {
            failure = addFailure(failure, e);
        }
        for (ShardWriter writer : writers.values()) {
            try {
                writer.close();
            } catch (IOException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        writers.clear();
        closeRemoteSearchers();
        analyzerRegistry.close();
        uploadWorkerPool.shutdown();
        try {
            if (!uploadWorkerPool.awaitTermination(30, TimeUnit.SECONDS)) {
                uploadWorkerPool.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            uploadWorkerPool.shutdownNow();
        }
        if (failure != null) {
            throw failure;
        }
    }

    private void flushPendingWrites() throws IOException {
        IOException failure = null;
        for (ShardWriter shardWriter : List.copyOf(writers.values())) {
            synchronized (shardWriter) {
                try {
                    if (shardWriter.uncommittedOperations.get() > 0) {
                        commitAndPublish(shardWriter);
                    }
                    refreshSearcher(shardWriter);
                } catch (IOException e) {
                    failure = addFailure(failure, e);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private record RemoteSearcherKey(ShardId shardId, long generation) {
    }

    private static final class CachedRemoteSearcher implements AutoCloseable {
        private final S3CachingDirectory directory;
        private final DirectoryReader reader;
        private final IndexSearcher searcher;
        private final AtomicInteger references = new AtomicInteger();
        private volatile boolean closed;
        private volatile long lastAccessNanos = System.nanoTime();

        private CachedRemoteSearcher(
                S3CachingDirectory directory,
                DirectoryReader reader,
                IndexSearcher searcher
        ) {
            this.directory = directory;
            this.reader = reader;
            this.searcher = searcher;
        }

        private synchronized boolean acquire() {
            if (closed) {
                return false;
            }
            references.incrementAndGet();
            long now = System.nanoTime();
            lastAccessNanos = now;
            return true;
        }

        private void release() {
            references.decrementAndGet();
        }

        private IndexSearcher searcher() {
            return searcher;
        }

        private boolean expired(long now) {
            return references.get() == 0 && now - lastAccessNanos > REMOTE_SEARCHER_CACHE_TTL.toNanos();
        }

        @Override
        public synchronized void close() throws IOException {
            if (closed) {
                return;
            }
            closed = true;
            IOException failure = null;
            try {
                reader.close();
            } catch (IOException e) {
                failure = e;
            }
            try {
                directory.close();
            } catch (IOException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
            if (failure != null) {
                throw failure;
            }
        }
    }

    private record ShardWriter(
            ShardId shardId,
            S3CachingDirectory directory,
            IndexWriter writer,
            SearcherManager searcherManager,
            Analyzer analyzer,
            SnapshotDeletionPolicy deletionPolicy,
            List<RetainedSnapshotCommit> retainedSnapshotCommits,
            AtomicInteger nextSnapshotSequence,
            AtomicInteger cleanPublishedSnapshotSequence,
            AtomicInteger uncommittedOperations,
            AtomicLong lastCommitNanos,
            AtomicLong lastRefreshNanos,
            AtomicBoolean refreshPending,
            AtomicBoolean snapshotPublishPending
    ) implements AutoCloseable {
        @Override
        public void close() throws IOException {
            IOException failure = null;
            failure = close(failure, searcherManager);
            failure = close(failure, writer);
            analyzer.close();
            failure = close(failure, directory);
            if (failure != null) {
                throw failure;
            }
        }

        private IOException close(IOException failure, AutoCloseable closeable) {
            try {
                closeable.close();
            } catch (Exception e) {
                IOException ioException = e instanceof IOException io ? io : new IOException(e);
                if (failure == null) {
                    return ioException;
                }
                failure.addSuppressed(ioException);
            }
            return failure;
        }
    }

    private static final class RetainedSnapshotCommit {
        private static final long MAX_PUBLISH_NANOS = TimeUnit.MINUTES.toNanos(2);

        private final IndexCommit commit;
        private final int sequence;
        private final long createdAtNanos = System.nanoTime();
        private boolean completed;

        private RetainedSnapshotCommit(IndexCommit commit, int sequence) {
            this.commit = commit;
            this.sequence = sequence;
        }

        private boolean publishTimedOut(long nowNanos) {
            return !completed && (nowNanos - createdAtNanos) >= MAX_PUBLISH_NANOS;
        }
    }

    private record PitContext(
            ShardId shardId,
            String indexName,
            DirectoryReader reader,
            S3CachingDirectory directory,
            Instant expiresAt
    ) {
    }
}
