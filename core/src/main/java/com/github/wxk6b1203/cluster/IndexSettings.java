package com.github.wxk6b1203.cluster;

import java.time.Instant;
import java.util.Map;

public record IndexSettings(
        String name,
        int numberOfShards,
        String lifecyclePolicy,
        Instant createdAt,
        Map<String, FieldMapping> mappings,
        Boolean deletePending,
        Instant deleteStartedAt
) {
    // ES-style: lowercase alphanumerics plus '.', '_' and '-'; starts with a letter or digit so
    // hidden/system-style names and '-'-prefixed URL paths are rejected. The name is embedded in
    // physical index names ({name}__shard_{n}), S3 object keys and etcd keys, so characters that
    // would collide with those structures (slash, wildcard, whitespace) must never get in.
    private static final java.util.regex.Pattern NAME_PATTERN =
            java.util.regex.Pattern.compile("[a-z0-9][a-z0-9._-]{0,199}");
    public IndexSettings(String name, int numberOfShards, String lifecyclePolicy, Instant createdAt) {
        this(name, numberOfShards, lifecyclePolicy, createdAt, Map.of());
    }

    public IndexSettings(String name, int numberOfShards, String lifecyclePolicy, Instant createdAt, Map<String, FieldMapping> mappings) {
        this(name, numberOfShards, lifecyclePolicy, createdAt, mappings, false, null);
    }

    public IndexSettings {
        if (name == null || !NAME_PATTERN.matcher(name).matches()) {
            throw new IllegalArgumentException(
                    "invalid index name [" + name + "]: must be 1-200 characters of lowercase letters, "
                            + "digits, '.', '_' or '-', and start with a letter or digit");
        }
        if (numberOfShards <= 0) {
            throw new IllegalArgumentException("numberOfShards must be positive");
        }
        mappings = mappings == null ? Map.of() : Map.copyOf(mappings);
        deletePending = deletePending != null && deletePending;
        if (!deletePending) {
            deleteStartedAt = null;
        } else if (deleteStartedAt == null) {
            deleteStartedAt = Instant.now();
        }
    }

    public IndexSettings withMappings(Map<String, FieldMapping> mappings) {
        return new IndexSettings(name, numberOfShards, lifecyclePolicy, createdAt, mappings, deletePending, deleteStartedAt);
    }

    public IndexSettings withLifecyclePolicy(String lifecyclePolicy) {
        return new IndexSettings(name, numberOfShards, lifecyclePolicy, createdAt, mappings, deletePending, deleteStartedAt);
    }

    public IndexSettings markDeleting(Instant now) {
        return new IndexSettings(name, numberOfShards, lifecyclePolicy, createdAt, mappings, true, now);
    }
}
