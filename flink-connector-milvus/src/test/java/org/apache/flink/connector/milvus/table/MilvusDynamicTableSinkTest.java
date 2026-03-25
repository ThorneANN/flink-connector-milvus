package org.apache.flink.connector.milvus.table;

import org.apache.flink.connector.milvus.MilvusSinkConfig;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

/** Unit tests for {@link MilvusDynamicTableSink}. */
class MilvusDynamicTableSinkTest {

    private static final DataType ROW_TYPE = DataTypes.ROW(
            DataTypes.FIELD("id", DataTypes.BIGINT()),
            DataTypes.FIELD("vec", DataTypes.ARRAY(DataTypes.FLOAT()))
    );

    private MilvusDynamicTableSink sink() {
        MilvusSinkConfig config = MilvusSinkConfig.builder()
                .uri("http://localhost:19530")
                .collectionName("test_col")
                .build();
        return new MilvusDynamicTableSink(config, ROW_TYPE);
    }

    @Test
    void testChangelogModeAcceptsAllRowKinds() {
        ChangelogMode mode = sink().getChangelogMode(ChangelogMode.insertOnly());
        assertTrue(mode.contains(RowKind.INSERT));
        assertTrue(mode.contains(RowKind.UPDATE_BEFORE));
        assertTrue(mode.contains(RowKind.UPDATE_AFTER));
        assertTrue(mode.contains(RowKind.DELETE));
    }

    @Test
    void testCopyReturnsFunctionalEquivalentInstance() {
        MilvusDynamicTableSink original = sink();
        DynamicTableSink copy = original.copy();

        assertNotNull(copy);
        assertNotSame(original, copy);
        assertEquals(original.asSummaryString(), copy.asSummaryString());
    }

    @Test
    void testAsSummaryStringContainsCollectionName() {
        String summary = sink().asSummaryString();
        assertTrue(summary.contains("test_col"));
    }

    @Test
    void testGetSinkRuntimeProviderReturnsSinkV2Provider() {
        DynamicTableSink.Context context = mock(DynamicTableSink.Context.class);
        DynamicTableSink.SinkRuntimeProvider provider = sink().getSinkRuntimeProvider(context);
        assertInstanceOf(SinkV2Provider.class, provider);
    }
}
