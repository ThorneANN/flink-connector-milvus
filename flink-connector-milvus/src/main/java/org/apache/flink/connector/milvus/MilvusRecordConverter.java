package org.apache.flink.connector.milvus;

import com.google.gson.JsonObject;

import java.io.Serializable;

/**
 * Interface for converting records of type {@code T} to {@link JsonObject} for writing to Milvus.
 *
 * @param <T> the type of input records
 */
@FunctionalInterface
public interface MilvusRecordConverter<T> extends Serializable {

    /**
     * Converts the given record to a {@link JsonObject} that can be inserted into Milvus.
     *
     * @param record the input record to convert
     * @return a {@link JsonObject} representing the record
     * @throws Exception if conversion fails
     */
    JsonObject convert(T record) throws Exception;
}
