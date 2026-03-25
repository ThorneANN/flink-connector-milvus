package org.apache.flink.connector.milvus;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/** Unit tests for {@link MilvusSinkConfig}. */
class MilvusSinkConfigTest {

    @Test
    void testBuildWithRequiredFields() {
        MilvusSinkConfig config = MilvusSinkConfig.builder()
                .uri("http://localhost:19530")
                .collectionName("test_collection")
                .build();

        assertEquals("http://localhost:19530", config.getUri());
        assertEquals("test_collection", config.getCollectionName());
        assertEquals("", config.getToken());
        assertEquals("default", config.getDatabaseName());
        assertEquals(1000, config.getBatchSize());
        assertEquals(1000L, config.getFlushIntervalMs());
        assertEquals("BOUNDED", config.getConsistencyLevel());
        assertNull(config.getPartitionName());
        assertEquals(3, config.getMaxRetries());
        assertEquals(1000L, config.getRetryIntervalMs());
    }

    @Test
    void testBuildWithAllFields() {
        MilvusSinkConfig config = MilvusSinkConfig.builder()
                .uri("http://milvus:19530")
                .token("user:password")
                .collectionName("my_collection")
                .databaseName("my_db")
                .batchSize(500)
                .flushIntervalMs(2000L)
                .consistencyLevel("STRONG")
                .partitionName("partition_a")
                .maxRetries(5)
                .retryIntervalMs(500L)
                .build();

        assertEquals("http://milvus:19530", config.getUri());
        assertEquals("user:password", config.getToken());
        assertEquals("my_collection", config.getCollectionName());
        assertEquals("my_db", config.getDatabaseName());
        assertEquals(500, config.getBatchSize());
        assertEquals(2000L, config.getFlushIntervalMs());
        assertEquals("STRONG", config.getConsistencyLevel());
        assertEquals("partition_a", config.getPartitionName());
        assertEquals(5, config.getMaxRetries());
        assertEquals(500L, config.getRetryIntervalMs());
    }

    @Test
    void testMissingUriThrows() {
        assertThrows(IllegalArgumentException.class, () ->
                MilvusSinkConfig.builder()
                        .collectionName("test")
                        .build());
    }

    @Test
    void testMissingCollectionNameThrows() {
        assertThrows(IllegalArgumentException.class, () ->
                MilvusSinkConfig.builder()
                        .uri("http://localhost:19530")
                        .build());
    }

    @Test
    void testInvalidBatchSizeThrows() {
        assertThrows(IllegalArgumentException.class, () ->
                MilvusSinkConfig.builder()
                        .uri("http://localhost:19530")
                        .collectionName("test")
                        .batchSize(0)
                        .build());
    }

    @Test
    void testNegativeMaxRetriesThrows() {
        assertThrows(IllegalArgumentException.class, () ->
                MilvusSinkConfig.builder()
                        .uri("http://localhost:19530")
                        .collectionName("test")
                        .maxRetries(-1)
                        .build());
    }

    @Test
    void testZeroMaxRetriesAllowed() {
        MilvusSinkConfig config = MilvusSinkConfig.builder()
                .uri("http://localhost:19530")
                .collectionName("test")
                .maxRetries(0)
                .build();
        assertEquals(0, config.getMaxRetries());
    }
}
