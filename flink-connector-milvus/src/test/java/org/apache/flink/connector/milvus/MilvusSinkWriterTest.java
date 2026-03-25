package org.apache.flink.connector.milvus;

import com.google.gson.JsonObject;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.service.vector.request.DeleteReq;
import io.milvus.v2.service.vector.request.UpsertReq;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/** Unit tests for {@link MilvusSinkWriter}. */
class MilvusSinkWriterTest {

    private MilvusClientV2 mockClient;
    private MilvusConnectionProvider mockProvider;

    private static final MilvusSinkConfig BASE_CONFIG = MilvusSinkConfig.builder()
            .uri("http://localhost:19530")
            .collectionName("test_col")
            .batchSize(100)
            .flushIntervalMs(0)
            .maxRetries(0)
            .build();

    @BeforeEach
    void setUp() {
        mockClient = mock(MilvusClientV2.class);
        mockProvider = mock(MilvusConnectionProvider.class);
        when(mockProvider.getOrCreateClient()).thenReturn(mockClient);
    }

    // ---- RowKind routing ----

    @Test
    void testInsertRowKindGoesToUpsert() throws Exception {
        MilvusSinkWriter<GenericRowData> writer = rowDataWriter(BASE_CONFIG);

        writer.write(rowOf(RowKind.INSERT, 1L, "a"), null);
        writer.flush(false);

        verify(mockClient, times(1)).upsert(any(UpsertReq.class));
        verify(mockClient, never()).delete(any());
    }

    @Test
    void testUpdateAfterRowKindGoesToUpsert() throws Exception {
        MilvusSinkWriter<GenericRowData> writer = rowDataWriter(BASE_CONFIG);

        writer.write(rowOf(RowKind.UPDATE_AFTER, 2L, "b"), null);
        writer.flush(false);

        verify(mockClient, times(1)).upsert(any(UpsertReq.class));
        verify(mockClient, never()).delete(any());
    }

    @Test
    void testUpdateBeforeRowKindGoesToDelete() throws Exception {
        MilvusSinkWriter<GenericRowData> writer = rowDataWriter(BASE_CONFIG);

        writer.write(rowOf(RowKind.UPDATE_BEFORE, 3L, "c"), null);
        writer.flush(false);

        // UPDATE_BEFORE carries the old primary key — must be deleted from Milvus
        verify(mockClient, times(1)).delete(any(DeleteReq.class));
        verify(mockClient, never()).upsert(any());
    }

    @Test
    void testDeleteRowKindGoesToDelete() throws Exception {
        MilvusSinkWriter<GenericRowData> writer = rowDataWriter(BASE_CONFIG);

        writer.write(rowOf(RowKind.DELETE, 4L, "d"), null);
        writer.flush(false);

        verify(mockClient, times(1)).delete(any(DeleteReq.class));
        verify(mockClient, never()).upsert(any());
    }

    @Test
    void testNonRowDataDefaultsToUpsert() throws Exception {
        MilvusSinkWriter<String> writer = new MilvusSinkWriter<>(BASE_CONFIG, r -> {
            JsonObject j = new JsonObject();
            j.addProperty("id", 1L);
            return j;
        }, mockProvider);

        writer.write("any string", null);
        writer.flush(false);

        verify(mockClient, times(1)).upsert(any(UpsertReq.class));
    }

    // ---- batching ----

    @Test
    void testBatchSizeTriggerAutoFlush() throws Exception {
        MilvusSinkConfig config = MilvusSinkConfig.builder()
                .uri("http://localhost:19530")
                .collectionName("test_col")
                .batchSize(3)
                .flushIntervalMs(0)
                .maxRetries(0)
                .build();
        MilvusSinkWriter<GenericRowData> writer = rowDataWriter(config);

        writer.write(rowOf(RowKind.INSERT, 1L, "a"), null);
        writer.write(rowOf(RowKind.INSERT, 2L, "b"), null);
        verify(mockClient, never()).upsert(any());

        writer.write(rowOf(RowKind.INSERT, 3L, "c"), null);
        verify(mockClient, times(1)).upsert(any(UpsertReq.class));
    }

    @Test
    void testCloseFlushesRemainingRecords() throws Exception {
        MilvusSinkWriter<GenericRowData> writer = rowDataWriter(BASE_CONFIG);

        writer.write(rowOf(RowKind.INSERT, 1L, "x"), null);
        verify(mockClient, never()).upsert(any());

        writer.close();
        verify(mockClient, times(1)).upsert(any(UpsertReq.class));
    }

    @Test
    void testFlushEmptyBufferDoesNotCallClient() throws Exception {
        MilvusSinkWriter<GenericRowData> writer = rowDataWriter(BASE_CONFIG);

        writer.flush(false);
        writer.flush(true);

        verify(mockClient, never()).upsert(any());
        verify(mockClient, never()).delete(any());
    }

    // ---- error handling ----

    @Test
    void testConverterExceptionWrappedAsIOException() {
        MilvusSinkWriter<String> writer = new MilvusSinkWriter<>(BASE_CONFIG, r -> {
            throw new RuntimeException("conversion failed");
        }, mockProvider);

        assertThrows(IOException.class, () -> writer.write("bad", null));
    }

    @Test
    void testFlushRetriesOnFailureThenSucceeds() throws Exception {
        MilvusSinkConfig config = MilvusSinkConfig.builder()
                .uri("http://localhost:19530")
                .collectionName("test_col")
                .batchSize(100)
                .flushIntervalMs(0)
                .maxRetries(2)
                .retryIntervalMs(1)  // minimal sleep in tests
                .build();
        MilvusSinkWriter<GenericRowData> writer = rowDataWriter(config);

        // First call throws, second call succeeds
        when(mockClient.upsert(any(UpsertReq.class)))
                .thenThrow(new RuntimeException("transient error"))
                .thenReturn(null);

        writer.write(rowOf(RowKind.INSERT, 1L, "a"), null);
        writer.flush(false); // should not throw: retried once then succeeded

        verify(mockClient, times(2)).upsert(any(UpsertReq.class));
    }

    @Test
    void testFlushThrowsAfterExhaustedRetries() throws Exception {
        MilvusSinkConfig config = MilvusSinkConfig.builder()
                .uri("http://localhost:19530")
                .collectionName("test_col")
                .batchSize(100)
                .flushIntervalMs(0)
                .maxRetries(2)
                .retryIntervalMs(1)
                .build();
        MilvusSinkWriter<GenericRowData> writer = rowDataWriter(config);

        doThrow(new RuntimeException("persistent error")).when(mockClient).upsert(any());

        writer.write(rowOf(RowKind.INSERT, 1L, "a"), null);

        IOException ex = assertThrows(IOException.class, () -> writer.flush(false));
        assertTrue(ex.getMessage().contains("attempt"));
        verify(mockClient, times(3)).upsert(any()); // 1 original + 2 retries
    }

    // ---- helpers ----

    /** Creates a writer that converts GenericRowData using a simple id+text JsonObject. */
    private MilvusSinkWriter<GenericRowData> rowDataWriter(MilvusSinkConfig config) {
        return new MilvusSinkWriter<>(config, row -> {
            JsonObject j = new JsonObject();
            j.addProperty("id", row.getLong(0));
            j.addProperty("text", row.getString(1).toString());
            return j;
        }, mockProvider);
    }

    /** Creates a two-field GenericRowData with the given RowKind, id (BIGINT), and text (STRING). */
    private GenericRowData rowOf(RowKind kind, long id, String text) {
        GenericRowData row = new GenericRowData(kind, 2);
        row.setField(0, id);
        row.setField(1, StringData.fromString(text));
        return row;
    }
}
