package com.github.wxk6b1203.index;

import com.github.wxk6b1203.cluster.FieldMapping;
import com.github.wxk6b1203.util.JsonUtil;
import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Streaming aggregation over the matching documents. The previous implementation materialized
 * every matching document (doc ids plus parsed _source maps) in memory before aggregating — an
 * OOM hazard once a query matched hundreds of thousands of docs. A single collector pass now
 * feeds per-leaf docValues directly into per-aggregation accumulators; stored _source is only
 * read (per doc, transiently) for fields without usable docValues — text fields, multi-valued
 * fields and mappings with doc_values disabled — preserving the old extraction semantics.
 */
final class LuceneAggregationExecutor {
    Map<String, Object> aggregate(
            IndexSearcher searcher,
            Query query,
            List<Map<String, Object>> aggregations,
            Map<String, FieldMapping> mappings
    ) throws IOException {
        if (aggregations == null || aggregations.isEmpty()) {
            return Map.of();
        }
        List<Aggregation> plans = new ArrayList<>();
        for (Map<String, Object> aggregation : aggregations) {
            String name = stringValue(aggregation.get("name"));
            if (name == null || name.isBlank()) {
                continue;
            }
            Aggregation plan = plan(name, aggregation, mappings);
            if (plan != null) {
                plans.add(plan);
            }
        }
        if (plans.isEmpty()) {
            return Map.of();
        }
        // Accumulators live in `plans` (shared across segment collectors); each segment gets a
        // collector whose only state is its per-leaf docValues readers.
        searcher.search(query, new org.apache.lucene.search.CollectorManager<SimpleCollector, Void>() {
            @Override
            public SimpleCollector newCollector() {
                return new SimpleCollector() {
                    private Map<String, FieldValues> readers = Map.of();

                    @Override
                    protected void doSetNextReader(LeafReaderContext context) throws IOException {
                        Map<String, FieldValues> built = new HashMap<>();
                        for (Aggregation plan : plans) {
                            String f = plan.field();
                            if (!built.containsKey(f)) {
                                built.put(f, fieldValues(context.reader(), f, plan.mapping()));
                            }
                        }
                        this.readers = built;
                    }

                    @Override
                    public void collect(int doc) throws IOException {
                        for (Aggregation plan : plans) {
                            plan.accumulate(readers.get(plan.field()).values(doc));
                        }
                    }

                    @Override
                    public org.apache.lucene.search.ScoreMode scoreMode() {
                        return org.apache.lucene.search.ScoreMode.COMPLETE_NO_SCORES;
                    }
                };
            }

            @Override
            public Void reduce(java.util.Collection<SimpleCollector> collectors) {
                return null;
            }
        });
        Map<String, Object> results = new LinkedHashMap<>();
        for (Aggregation plan : plans) {
            results.put(plan.name(), plan.result());
        }
        return results;
    }

    private Aggregation plan(String name, Map<String, Object> aggregation, Map<String, FieldMapping> mappings) {
        if (aggregation.get("terms") instanceof Map<?, ?> terms) {
            return new TermsAggregation(name, mapValue(terms), mappings);
        }
        if (aggregation.get("range") instanceof Map<?, ?> range) {
            return new RangeAggregation(name, mapValue(range), mappings);
        }
        if (aggregation.get("min") instanceof Map<?, ?> min) {
            return new MetricAggregation(name, mapValue(min), mappings, "min");
        }
        if (aggregation.get("max") instanceof Map<?, ?> max) {
            return new MetricAggregation(name, mapValue(max), mappings, "max");
        }
        if (aggregation.get("sum") instanceof Map<?, ?> sum) {
            return new MetricAggregation(name, mapValue(sum), mappings, "sum");
        }
        if (aggregation.get("avg") instanceof Map<?, ?> avg) {
            return new MetricAggregation(name, mapValue(avg), mappings, "avg");
        }
        if (aggregation.get("value_count") instanceof Map<?, ?> valueCount) {
            return new MetricAggregation(name, mapValue(valueCount), mappings, "value_count");
        }
        return null;
    }

    private abstract static class Aggregation {
        private final String name;
        private final String field;
        private final FieldMapping mapping;

        protected Aggregation(String name, String field, FieldMapping mapping) {
            this.name = name;
            this.field = field;
            this.mapping = mapping;
        }

        String name() {
            return name;
        }

        String field() {
            return field;
        }

        FieldMapping mapping() {
            return mapping;
        }

        abstract void accumulate(List<Object> values);

        abstract Map<String, Object> result();
    }

    private static final class TermsAggregation extends Aggregation {
        private final Map<String, Long> counts = new HashMap<>();
        private final Object missing;
        private final int size;
        private final long minDocCount;
        private final Map<String, Object> spec;

        TermsAggregation(String name, Map<String, Object> spec, Map<String, FieldMapping> mappings) {
            super(name, stringValue(spec.get("field")), mappings.get(spec.get("field")));
            this.spec = spec;
            this.size = Math.max(1, intValue(spec.get("size"), 10));
            this.minDocCount = Math.max(0, longOrDefault(spec.get("min_doc_count"), 1));
            this.missing = spec.get("missing");
        }

        @Override
        void accumulate(List<Object> values) {
            if (values.isEmpty() && missing != null) {
                values = List.of(missing);
            }
            for (Object item : values) {
                counts.merge(String.valueOf(item), 1L, Long::sum);
            }
        }

        @Override
        Map<String, Object> result() {
            List<Map<String, Object>> buckets = counts.entrySet().stream()
                    .sorted(termsComparator(spec))
                    .map(entry -> Map.<String, Object>of(
                            "key", entry.getKey(),
                            "doc_count", entry.getValue()
                    ))
                    .toList();
            return Map.of(
                    "type", "terms",
                    "field", field(),
                    "size", size,
                    "min_doc_count", minDocCount,
                    "order", termsOrder(spec),
                    "buckets", buckets
            );
        }
    }

    private static final class RangeAggregation extends Aggregation {
        private final List<RangeBucket> ranges;
        private final long[] counts;

        RangeAggregation(String name, Map<String, Object> spec, Map<String, FieldMapping> mappings) {
            super(name, stringValue(spec.get("field")), mappings.get(spec.get("field")));
            this.ranges = rangeBuckets(spec, mapping());
            this.counts = new long[ranges.size()];
        }

        @Override
        void accumulate(List<Object> values) {
            boolean[] matched = new boolean[ranges.size()];
            for (Object item : values) {
                Double value = aggregationNumber(item, mapping());
                if (value == null) {
                    continue;
                }
                for (int range = 0; range < ranges.size(); range++) {
                    if (ranges.get(range).contains(value)) {
                        matched[range] = true;
                    }
                }
            }
            for (int range = 0; range < matched.length; range++) {
                if (matched[range]) {
                    counts[range]++;
                }
            }
        }

        @Override
        Map<String, Object> result() {
            List<Map<String, Object>> buckets = new ArrayList<>(ranges.size());
            for (int i = 0; i < ranges.size(); i++) {
                RangeBucket range = ranges.get(i);
                Map<String, Object> bucket = new LinkedHashMap<>();
                bucket.put("key", range.key());
                if (range.from() != null) {
                    bucket.put("from", range.fromValue());
                }
                if (range.to() != null) {
                    bucket.put("to", range.toValue());
                }
                bucket.put("doc_count", counts[i]);
                buckets.add(bucket);
            }
            return Map.of(
                    "type", "range",
                    "field", field(),
                    "buckets", buckets
            );
        }
    }

    private static final class MetricAggregation extends Aggregation {
        private final String type;
        private double sum;
        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;
        private long count;

        MetricAggregation(String name, Map<String, Object> spec, Map<String, FieldMapping> mappings, String type) {
            super(name, stringValue(spec.get("field")), mappings.get(spec.get("field")));
            this.type = type;
        }

        @Override
        void accumulate(List<Object> values) {
            for (Object item : values) {
                Double value = doubleValue(item);
                if (value == null) {
                    continue;
                }
                sum += value;
                min = Math.min(min, value);
                max = Math.max(max, value);
                count++;
            }
        }

        @Override
        Map<String, Object> result() {
            Object value = switch (type) {
                case "min" -> count == 0 ? null : min;
                case "max" -> count == 0 ? null : max;
                case "sum" -> sum;
                case "avg" -> count == 0 ? null : sum / count;
                case "value_count" -> count;
                default -> null;
            };
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("type", type);
            result.put("field", field());
            result.put("value", value);
            result.put("count", count);
            if ("avg".equals(type)) {
                result.put("sum", sum);
            }
            return result;
        }
    }

    /** Per-leaf value extraction for one field: docValues when usable, stored _source otherwise. */
    private interface FieldValues {
        List<Object> values(int doc) throws IOException;
    }

    private FieldValues fieldValues(LeafReader reader, String field, FieldMapping mapping) throws IOException {
        FieldValues docValuesReader = typedDocValues(reader, field, mapping);
        if (docValuesReader == null) {
            return sourceValues(reader, field);
        }
        // Per-document fallback (matching the previous implementation): a document without a
        // docValues entry for this field — e.g. written before the mapping tightened, or a
        // list value stored into a single-valued field — is read back from stored _source.
        FieldValues sourceReader = sourceValues(reader, field);
        return doc -> {
            List<Object> values = docValuesReader.values(doc);
            return values.isEmpty() ? sourceReader.values(doc) : values;
        };
    }

    /** DocValues reader for the supported single-valued types; null when unusable for the field. */
    private FieldValues typedDocValues(LeafReader reader, String field, FieldMapping mapping) throws IOException {
        if (mapping == null || !Boolean.TRUE.equals(mapping.docValues()) || mapping.multiValued()) {
            return null;
        }
        if (mapping.keyword() || mapping.bool()) {
            SortedDocValues values = DocValues.getSorted(reader, field);
            return doc -> values.advanceExact(doc)
                    ? List.of(values.lookupOrd(values.ordValue()).utf8ToString())
                    : List.of();
        }
        if (mapping.longNumber() || mapping.date()) {
            NumericDocValues values = DocValues.getNumeric(reader, field);
            return doc -> values.advanceExact(doc) ? List.of(values.longValue()) : List.of();
        }
        if (mapping.doubleNumber()) {
            NumericDocValues values = DocValues.getNumeric(reader, field);
            return doc -> values.advanceExact(doc)
                    ? List.of(Double.longBitsToDouble(values.longValue()))
                    : List.of();
        }
        if (mapping.ip() || mapping.binary()) {
            BinaryDocValues values = DocValues.getBinary(reader, field);
            return doc -> {
                if (!values.advanceExact(doc)) {
                    return List.of();
                }
                BytesRef bytes = values.binaryValue();
                byte[] copy = Arrays.copyOfRange(bytes.bytes, bytes.offset, bytes.offset + bytes.length);
                if (mapping.ip()) {
                    return List.of(InetAddressPoint.decode(copy).getHostAddress());
                }
                return List.of(Base64.getEncoder().encodeToString(copy));
            };
        }
        // Unsupported docValues type (e.g. text with doc_values=true): same _source fallback as
        // the previous implementation.
        return null;
    }

    private FieldValues sourceValues(LeafReader reader, String field) throws IOException {
        var storedFields = reader.storedFields();
        return doc -> {
            String source = storedFields.document(doc).get("_source");
            if (source == null || source.isBlank()) {
                return List.of();
            }
            return aggregationValues(JsonUtil.readValueAsMap(source).get(field));
        };
    }

    private static Comparator<Map.Entry<String, Long>> termsComparator(Map<String, Object> spec) {
        Map<String, Object> order = mapValue(spec.get("order"));
        if (order.isEmpty()) {
            return Comparator
                    .comparing((Map.Entry<String, Long> entry) -> entry.getValue()).reversed()
                    .thenComparing(Map.Entry::getKey);
        }
        String by = order.keySet().iterator().next();
        boolean desc = !"asc".equalsIgnoreCase(String.valueOf(order.get(by)));
        Comparator<Map.Entry<String, Long>> comparator = "_key".equals(by)
                ? Map.Entry.comparingByKey()
                : Comparator.comparing(Map.Entry::getValue);
        if (desc) {
            comparator = comparator.reversed();
        }
        return comparator.thenComparing(Map.Entry::getKey);
    }

    private static Map<String, Object> termsOrder(Map<String, Object> spec) {
        Map<String, Object> order = mapValue(spec.get("order"));
        if (order.isEmpty()) {
            return Map.of("_count", "desc");
        }
        return Map.copyOf(order);
    }

    private static List<RangeBucket> rangeBuckets(Map<String, Object> spec, FieldMapping mapping) {
        String field = stringValue(spec.get("field"));
        List<Object> ranges = objectList(spec.get("ranges"));
        if (ranges.isEmpty()) {
            throw new IllegalArgumentException("range aggregation requires ranges");
        }
        List<RangeBucket> buckets = new ArrayList<>(ranges.size());
        for (Object value : ranges) {
            Map<String, Object> range = mapValue(value);
            boolean hasFrom = range.containsKey("from");
            boolean hasTo = range.containsKey("to");
            Object fromValue = range.get("from");
            Object toValue = range.get("to");
            Double from = aggregationBound(fromValue, hasFrom, field, "from", mapping);
            Double to = aggregationBound(toValue, hasTo, field, "to", mapping);
            String key = stringValue(range.get("key"));
            if (key == null || key.isBlank()) {
                key = (!hasFrom ? "*" : String.valueOf(fromValue))
                        + "-"
                        + (!hasTo ? "*" : String.valueOf(toValue));
            }
            buckets.add(new RangeBucket(key, from, to, fromValue, toValue));
        }
        return buckets;
    }

    private static Double aggregationBound(
            Object value,
            boolean present,
            String field,
            String bound,
            FieldMapping mapping
    ) {
        if (!present) {
            return null;
        }
        Double number = aggregationNumber(value, mapping);
        if (number == null) {
            throw new IllegalArgumentException("range aggregation " + bound
                    + " bound must be numeric or date for field: " + field);
        }
        return number;
    }

    private static Double aggregationNumber(Object value, FieldMapping mapping) {
        if (value == null) {
            return null;
        }
        if (mapping != null && mapping.date()) {
            Long date = dateValue(value, mapping);
            return date == null ? null : date.doubleValue();
        }
        return doubleValue(value);
    }

    private static String stringValue(Object value) {
        return value == null ? null : String.valueOf(value);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> mapValue(Object value) {
        return value instanceof Map<?, ?> map ? (Map<String, Object>) map : Map.of();
    }

    private static List<Object> objectList(Object value) {
        if (!(value instanceof List<?> list)) {
            return List.of();
        }
        return list.stream().map(item -> (Object) item).toList();
    }

    private static int intValue(Object value, int defaultValue) {
        return value instanceof Number number ? number.intValue() : defaultValue;
    }

    private static long longOrDefault(Object value, long defaultValue) {
        Long number = longValue(value);
        return number == null ? defaultValue : number;
    }

    private static Long longValue(Object value) {
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

    private static Double doubleValue(Object value) {
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

    private static Long dateValue(Object value, FieldMapping mapping) {
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

    private static List<Object> aggregationValues(Object value) {
        if (value == null) {
            return List.of();
        }
        if (value instanceof Iterable<?> iterable) {
            List<Object> values = new ArrayList<>();
            for (Object item : iterable) {
                if (item != null) {
                    values.add(item);
                }
            }
            return values;
        }
        return List.of(value);
    }

    private record RangeBucket(
            String key,
            Double from,
            Double to,
            Object fromValue,
            Object toValue
    ) {
        private boolean contains(double value) {
            return (from == null || value >= from) && (to == null || value < to);
        }
    }
}
