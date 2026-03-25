package org.apache.flink.connector.milvus;

import java.io.Serializable;

/** Serializable configuration for {@link MilvusSink}. */
public class MilvusSinkConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    // ---- default values (referenced by both Builder and MilvusSinkOptions) ----
    public static final String DEFAULT_DATABASE_NAME = "default";
    public static final int DEFAULT_BATCH_SIZE = 1000;
    public static final long DEFAULT_FLUSH_INTERVAL_MS = 1000L;
    public static final String DEFAULT_CONSISTENCY_LEVEL = "BOUNDED";
    public static final int DEFAULT_MAX_RETRIES = 3;
    public static final long DEFAULT_RETRY_INTERVAL_MS = 1000L;

    private final String uri;
    private final String token;
    private final String collectionName;
    private final String databaseName;
    private final int batchSize;
    private final long flushIntervalMs;
    private final String consistencyLevel;
    /**
     * Optional partition name. Only applicable when the target collection does NOT use a scalar
     * field as Partition Key. If the collection has Partition Key enabled, Milvus routes documents
     * automatically and this field must be left null.
     */
    private final String partitionName;
    /** Maximum number of retries after a flush failure (0 = no retry). */
    private final int maxRetries;
    /** Wait interval between retries in milliseconds. */
    private final long retryIntervalMs;

    private MilvusSinkConfig(Builder builder) {
        this.uri = builder.uri;
        this.token = builder.token;
        this.collectionName = builder.collectionName;
        this.databaseName = builder.databaseName;
        this.batchSize = builder.batchSize;
        this.flushIntervalMs = builder.flushIntervalMs;
        this.consistencyLevel = builder.consistencyLevel;
        this.partitionName = builder.partitionName;
        this.maxRetries = builder.maxRetries;
        this.retryIntervalMs = builder.retryIntervalMs;
    }

    public String getUri() { return uri; }
    public String getToken() { return token; }
    public String getCollectionName() { return collectionName; }
    public String getDatabaseName() { return databaseName; }
    public int getBatchSize() { return batchSize; }
    public long getFlushIntervalMs() { return flushIntervalMs; }
    public String getConsistencyLevel() { return consistencyLevel; }
    public String getPartitionName() { return partitionName; }
    public int getMaxRetries() { return maxRetries; }
    public long getRetryIntervalMs() { return retryIntervalMs; }

    public static Builder builder() {
        return new Builder();
    }

    /** Builder for {@link MilvusSinkConfig}. */
    public static class Builder {
        private String uri;
        private String token = "";
        private String collectionName;
        private String databaseName = DEFAULT_DATABASE_NAME;
        private int batchSize = DEFAULT_BATCH_SIZE;
        private long flushIntervalMs = DEFAULT_FLUSH_INTERVAL_MS;
        private String consistencyLevel = DEFAULT_CONSISTENCY_LEVEL;
        private String partitionName = null;
        private int maxRetries = DEFAULT_MAX_RETRIES;
        private long retryIntervalMs = DEFAULT_RETRY_INTERVAL_MS;

        public Builder uri(String uri) { this.uri = uri; return this; }
        public Builder token(String token) { this.token = token; return this; }
        public Builder collectionName(String collectionName) { this.collectionName = collectionName; return this; }
        public Builder databaseName(String databaseName) { this.databaseName = databaseName; return this; }
        public Builder batchSize(int batchSize) { this.batchSize = batchSize; return this; }
        public Builder flushIntervalMs(long flushIntervalMs) { this.flushIntervalMs = flushIntervalMs; return this; }
        public Builder consistencyLevel(String consistencyLevel) { this.consistencyLevel = consistencyLevel; return this; }
        public Builder partitionName(String partitionName) { this.partitionName = partitionName; return this; }
        public Builder maxRetries(int maxRetries) { this.maxRetries = maxRetries; return this; }
        public Builder retryIntervalMs(long retryIntervalMs) { this.retryIntervalMs = retryIntervalMs; return this; }

        public MilvusSinkConfig build() {
            if (uri == null || uri.isEmpty()) {
                throw new IllegalArgumentException("uri must not be null or empty");
            }
            if (collectionName == null || collectionName.isEmpty()) {
                throw new IllegalArgumentException("collectionName must not be null or empty");
            }
            if (batchSize <= 0) {
                throw new IllegalArgumentException("batchSize must be positive");
            }
            if (maxRetries < 0) {
                throw new IllegalArgumentException("maxRetries must not be negative");
            }
            return new MilvusSinkConfig(this);
        }
    }
}
