package org.apache.flink.connector.milvus;

import com.google.gson.JsonObject;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/** Unit tests for {@link MilvusSinkBuilder}. */
class MilvusSinkBuilderTest {

    private final MilvusSinkConfig validConfig = MilvusSinkConfig.builder()
            .uri("http://localhost:19530")
            .collectionName("test")
            .build();

    @Test
    void testBuildSucceeds() {
        MilvusSink<String> sink = MilvusSink.<String>builder()
                .config(validConfig)
                .converter(r -> new JsonObject())
                .build();
        assertNotNull(sink);
    }

    @Test
    void testBuildWithoutConfigThrows() {
        MilvusSinkBuilder<String> builder = MilvusSink.<String>builder()
                .converter(r -> new JsonObject());
        assertThrows(IllegalArgumentException.class, builder::build);
    }

    @Test
    void testBuildWithoutConverterThrows() {
        MilvusSinkBuilder<String> builder = MilvusSink.<String>builder()
                .config(validConfig);
        assertThrows(IllegalArgumentException.class, builder::build);
    }
}
