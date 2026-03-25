package org.apache.flink.connector.milvus.table;

import org.apache.flink.connector.milvus.MilvusSink;
import org.apache.flink.connector.milvus.MilvusSinkConfig;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import java.util.List;

/**
 * Flink Table API {@link DynamicTableSink} implementation for Milvus.
 *
 * <p>Write semantics are driven by {@link RowKind}:
 * <ul>
 *   <li>INSERT / UPDATE_AFTER → Milvus upsert</li>
 *   <li>UPDATE_BEFORE / DELETE → Milvus delete by primary key</li>
 * </ul>
 */
public class MilvusDynamicTableSink implements DynamicTableSink {

    private final MilvusSinkConfig config;
    private final DataType physicalRowDataType;

    public MilvusDynamicTableSink(MilvusSinkConfig config, DataType physicalRowDataType) {
        this.config = config;
        this.physicalRowDataType = physicalRowDataType;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        // Accept the full changelog: INSERT and UPDATE_AFTER go to upsert,
        // DELETE goes to delete, UPDATE_BEFORE is consumed but skipped.
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.UPDATE_BEFORE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .addContainedKind(RowKind.DELETE)
                .build();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        RowType rowType = (RowType) physicalRowDataType.getLogicalType();
        List<String> fieldNames = rowType.getFieldNames();
        List<LogicalType> fieldTypes = rowType.getChildren();

        MilvusRowDataConverter converter = new MilvusRowDataConverter(fieldNames, fieldTypes);

        MilvusSink<RowData> sink = MilvusSink.<RowData>builder()
                .config(config)
                .converter(converter)
                .build();

        return SinkV2Provider.of(sink);
    }

    @Override
    public DynamicTableSink copy() {
        return new MilvusDynamicTableSink(config, physicalRowDataType);
    }

    @Override
    public String asSummaryString() {
        return String.format("MilvusSink(collection=%s)", config.getCollectionName());
    }
}
