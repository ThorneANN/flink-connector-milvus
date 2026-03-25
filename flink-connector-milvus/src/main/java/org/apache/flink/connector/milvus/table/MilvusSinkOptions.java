package org.apache.flink.connector.milvus.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.connector.milvus.MilvusSinkConfig;

/** SQL {@link ConfigOption} definitions for the Milvus connector. */
public class MilvusSinkOptions {

    public static final ConfigOption<String> URI =
            ConfigOptions.key("uri")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The URI of the Milvus service, e.g. http://localhost:19530");

    public static final ConfigOption<String> TOKEN =
            ConfigOptions.key("token")
                    .stringType()
                    .defaultValue("")
                    .withDescription("Authentication token, e.g. root:Milvus");

    public static final ConfigOption<String> COLLECTION_NAME =
            ConfigOptions.key("collection-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The name of the Milvus collection to write to");

    public static final ConfigOption<String> DATABASE_NAME =
            ConfigOptions.key("database-name")
                    .stringType()
                    .defaultValue(MilvusSinkConfig.DEFAULT_DATABASE_NAME)
                    .withDescription("The name of the Milvus database");

    public static final ConfigOption<Integer> BATCH_SIZE =
            ConfigOptions.key("batch-size")
                    .intType()
                    .defaultValue(MilvusSinkConfig.DEFAULT_BATCH_SIZE)
                    .withDescription("Number of records to buffer before flushing to Milvus");

    public static final ConfigOption<Long> FLUSH_INTERVAL_MS =
            ConfigOptions.key("flush-interval-ms")
                    .longType()
                    .defaultValue(MilvusSinkConfig.DEFAULT_FLUSH_INTERVAL_MS)
                    .withDescription("Interval in milliseconds between periodic flushes (0 = disabled)");

    public static final ConfigOption<String> CONSISTENCY_LEVEL =
            ConfigOptions.key("consistency-level")
                    .stringType()
                    .defaultValue(MilvusSinkConfig.DEFAULT_CONSISTENCY_LEVEL)
                    .withDescription("Milvus consistency level: STRONG, BOUNDED, SESSION, EVENTUALLY");

    /**
     * Optional partition name. Only set this when the collection does NOT use a scalar field as
     * Partition Key. If Partition Key is enabled on the collection, Milvus routes documents
     * automatically — setting a partition name in that case will cause an error.
     */
    public static final ConfigOption<String> PARTITION_NAME =
            ConfigOptions.key("partition-name")
                    .stringType()
                    .defaultValue("")
                    .withDescription(
                            "Target partition name. Leave empty when the collection uses a scalar "
                            + "field as Partition Key (Milvus routes automatically in that case).");

    public static final ConfigOption<Integer> MAX_RETRIES =
            ConfigOptions.key("max-retries")
                    .intType()
                    .defaultValue(MilvusSinkConfig.DEFAULT_MAX_RETRIES)
                    .withDescription("Maximum number of retry attempts after a flush failure (0 = no retry)");

    public static final ConfigOption<Long> RETRY_INTERVAL_MS =
            ConfigOptions.key("retry-interval-ms")
                    .longType()
                    .defaultValue(MilvusSinkConfig.DEFAULT_RETRY_INTERVAL_MS)
                    .withDescription("Wait time in milliseconds between flush retries");

    private MilvusSinkOptions() {}
}
