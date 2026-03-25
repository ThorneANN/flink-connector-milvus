package org.apache.flink.connector.milvus.table;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/** Unit tests for {@link MilvusDynamicTableSinkFactory}. */
class MilvusDynamicTableSinkFactoryTest {

    private final MilvusDynamicTableSinkFactory factory = new MilvusDynamicTableSinkFactory();

    @Test
    void testFactoryIdentifier() {
        assertEquals("milvus", factory.factoryIdentifier());
    }

    @Test
    void testRequiredOptionsContainsUriAndCollectionName() {
        var required = factory.requiredOptions();
        assertEquals(2, required.size());
        assertTrue(required.stream().anyMatch(o -> o.key().equals("uri")));
        assertTrue(required.stream().anyMatch(o -> o.key().equals("collection-name")));
    }

    @Test
    void testOptionalOptionsContainsAllExpectedKeys() {
        var optional = factory.optionalOptions();
        assertTrue(optional.stream().anyMatch(o -> o.key().equals("token")));
        assertTrue(optional.stream().anyMatch(o -> o.key().equals("database-name")));
        assertTrue(optional.stream().anyMatch(o -> o.key().equals("batch-size")));
        assertTrue(optional.stream().anyMatch(o -> o.key().equals("flush-interval-ms")));
        assertTrue(optional.stream().anyMatch(o -> o.key().equals("consistency-level")));
        assertTrue(optional.stream().anyMatch(o -> o.key().equals("partition-name")));
        assertTrue(optional.stream().anyMatch(o -> o.key().equals("max-retries")));
        assertTrue(optional.stream().anyMatch(o -> o.key().equals("retry-interval-ms")));
        // write-mode must NOT be present
        assertTrue(optional.stream().noneMatch(o -> o.key().equals("write-mode")));
    }

    @Test
    void testCreateSinkWithMinimalOptions() {
        DynamicTableSink sink = createSink(Map.of(
                "connector", "milvus",
                "uri", "http://localhost:19530",
                "collection-name", "items"
        ));
        assertInstanceOf(MilvusDynamicTableSink.class, sink);
        assertTrue(sink.asSummaryString().contains("items"));
    }

    @Test
    void testCreateSinkWithPartitionAndRetryOptions() {
        DynamicTableSink sink = createSink(Map.of(
                "connector", "milvus",
                "uri", "http://milvus:19530",
                "collection-name", "vecs",
                "partition-name", "partition_a",
                "max-retries", "5",
                "retry-interval-ms", "500"
        ));
        assertInstanceOf(MilvusDynamicTableSink.class, sink);
    }

    // ---- helper ----

    private DynamicTableSink createSink(Map<String, String> options) {
        ResolvedSchema schema = ResolvedSchema.of(
                Column.physical("id", DataTypes.BIGINT()),
                Column.physical("vec", DataTypes.ARRAY(DataTypes.FLOAT()))
        );

        ResolvedCatalogTable catalogTable = mock(ResolvedCatalogTable.class);
        when(catalogTable.getOptions()).thenReturn(new HashMap<>(options));
        when(catalogTable.getResolvedSchema()).thenReturn(schema);

        DynamicTableFactory.Context context = mock(DynamicTableFactory.Context.class);
        when(context.getCatalogTable()).thenReturn(catalogTable);
        when(context.getConfiguration()).thenReturn(new Configuration());
        when(context.getClassLoader()).thenReturn(Thread.currentThread().getContextClassLoader());
        when(context.getObjectIdentifier()).thenReturn(
                ObjectIdentifier.of("default_catalog", "default_db", "milvus_test"));
        when(context.isTemporary()).thenReturn(false);

        return factory.createDynamicTableSink(context);
    }
}
