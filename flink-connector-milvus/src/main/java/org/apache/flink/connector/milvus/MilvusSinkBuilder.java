package org.apache.flink.connector.milvus;

/**
 * Fluent builder for {@link MilvusSink}.
 *
 * @param <T> the type of input records
 */
public class MilvusSinkBuilder<T> {

    private MilvusSinkConfig config;
    private MilvusRecordConverter<T> converter;

    MilvusSinkBuilder() {}

    /**
     * Sets the sink configuration.
     *
     * @param config the Milvus sink configuration
     * @return this builder
     */
    public MilvusSinkBuilder<T> config(MilvusSinkConfig config) {
        this.config = config;
        return this;
    }

    /**
     * Sets the record converter that transforms input records into Milvus JSON objects.
     *
     * @param converter the record converter
     * @return this builder
     */
    public MilvusSinkBuilder<T> converter(MilvusRecordConverter<T> converter) {
        this.converter = converter;
        return this;
    }

    /**
     * Builds the {@link MilvusSink}.
     *
     * @return the configured sink
     * @throws IllegalArgumentException if required fields are missing
     */
    public MilvusSink<T> build() {
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        if (converter == null) {
            throw new IllegalArgumentException("converter must not be null");
        }
        return new MilvusSink<>(config, converter);
    }
}
