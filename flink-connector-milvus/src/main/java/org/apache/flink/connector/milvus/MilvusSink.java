package org.apache.flink.connector.milvus;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;

import java.io.IOException;

/**
 * Flink DataStream API sink for writing data to Milvus vector database.
 *
 * <p>Usage example:
 * <pre>{@code
 * MilvusSink<MyType> sink = MilvusSink.<MyType>builder()
 *     .config(MilvusSinkConfig.builder()
 *         .uri("http://localhost:19530")
 *         .token("root:Milvus")
 *         .collectionName("my_collection")
 *         .writeMode(WriteMode.UPSERT)
 *         .batchSize(1000)
 *         .build())
 *     .converter(record -> {
 *         JsonObject json = new JsonObject();
 *         json.addProperty("id", record.getId());
 *         // ... add other fields
 *         return json;
 *     })
 *     .build();
 *
 * dataStream.sinkTo(sink);
 * }</pre>
 *
 * @param <T> the type of input records
 */
public class MilvusSink<T> implements Sink<T> {

    private static final long serialVersionUID = 1L;

    private final MilvusSinkConfig config;
    private final MilvusRecordConverter<T> converter;

    MilvusSink(MilvusSinkConfig config, MilvusRecordConverter<T> converter) {
        this.config = config;
        this.converter = converter;
    }

    @Override
    public SinkWriter<T> createWriter(InitContext context) throws IOException {
        return new MilvusSinkWriter<>(config, converter);
    }

    /**
     * Creates a new {@link MilvusSinkBuilder}.
     *
     * @param <T> the type of input records
     * @return a new builder
     */
    public static <T> MilvusSinkBuilder<T> builder() {
        return new MilvusSinkBuilder<>();
    }
}
