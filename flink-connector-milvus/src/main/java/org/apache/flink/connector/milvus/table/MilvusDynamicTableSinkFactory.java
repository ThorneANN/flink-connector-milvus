package org.apache.flink.connector.milvus.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.connector.milvus.MilvusSinkConfig;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.connector.milvus.table.MilvusSinkOptions.*;

/**
 * Flink SQL factory for the Milvus connector.
 *
 * <p>Register in SQL DDL using: {@code 'connector' = 'milvus'}
 *
 * <p>Example:
 * <pre>{@code
 * CREATE TABLE milvus_sink (
 *     id        BIGINT,
 *     text      STRING,
 *     embedding ARRAY<FLOAT>,
 *     PRIMARY KEY (id) NOT ENFORCED
 * ) WITH (
 *     'connector'      = 'milvus',
 *     'uri'            = 'http://localhost:19530',
 *     'token'          = 'root:Milvus',
 *     'collection-name'= 'my_collection'
 * );
 * }</pre>
 */
public class MilvusDynamicTableSinkFactory implements DynamicTableSinkFactory {

    public static final String IDENTIFIER = "milvus";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return new HashSet<>(Arrays.asList(URI, COLLECTION_NAME));
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return new HashSet<>(Arrays.asList(
                TOKEN,
                DATABASE_NAME,
                BATCH_SIZE,
                FLUSH_INTERVAL_MS,
                CONSISTENCY_LEVEL,
                PARTITION_NAME,
                MAX_RETRIES,
                RETRY_INTERVAL_MS));
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();

        String partitionName = helper.getOptions().get(PARTITION_NAME);

        MilvusSinkConfig config = MilvusSinkConfig.builder()
                .uri(helper.getOptions().get(URI))
                .token(helper.getOptions().get(TOKEN))
                .collectionName(helper.getOptions().get(COLLECTION_NAME))
                .databaseName(helper.getOptions().get(DATABASE_NAME))
                .batchSize(helper.getOptions().get(BATCH_SIZE))
                .flushIntervalMs(helper.getOptions().get(FLUSH_INTERVAL_MS))
                .consistencyLevel(helper.getOptions().get(CONSISTENCY_LEVEL))
                .partitionName(partitionName == null || partitionName.isEmpty() ? null : partitionName)
                .maxRetries(helper.getOptions().get(MAX_RETRIES))
                .retryIntervalMs(helper.getOptions().get(RETRY_INTERVAL_MS))
                .build();

        return new MilvusDynamicTableSink(
                config,
                context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType());
    }
}
