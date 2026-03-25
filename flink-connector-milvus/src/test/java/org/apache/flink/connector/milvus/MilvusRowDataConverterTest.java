package org.apache.flink.connector.milvus;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.apache.flink.connector.milvus.table.MilvusRowDataConverter;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** Unit tests for {@link MilvusRowDataConverter}. */
class MilvusRowDataConverterTest {

    @Test
    void testConvertPrimitiveTypes() throws Exception {
        List<String> names = Arrays.asList("b", "i", "l", "f", "d", "s");
        List<LogicalType> types = Arrays.asList(
                new BooleanType(),
                new IntType(),
                new BigIntType(),
                new FloatType(),
                new DoubleType(),
                new VarCharType());

        GenericRowData row = new GenericRowData(6);
        row.setField(0, true);
        row.setField(1, 42);
        row.setField(2, 100L);
        row.setField(3, 3.14f);
        row.setField(4, 2.718);
        row.setField(5, StringData.fromString("hello"));

        MilvusRowDataConverter converter = new MilvusRowDataConverter(names, types);
        JsonObject result = converter.convert(row);

        assertTrue(result.get("b").getAsBoolean());
        assertEquals(42, result.get("i").getAsInt());
        assertEquals(100L, result.get("l").getAsLong());
        assertEquals(3.14f, result.get("f").getAsFloat(), 0.001f);
        assertEquals(2.718, result.get("d").getAsDouble(), 0.0001);
        assertEquals("hello", result.get("s").getAsString());
    }

    @Test
    void testConvertFloatArray() throws Exception {
        List<String> names = Arrays.asList("embedding");
        List<LogicalType> types = Arrays.asList(new ArrayType(new FloatType()));

        GenericRowData row = new GenericRowData(1);
        row.setField(0, new GenericArrayData(new float[]{0.1f, 0.2f, 0.3f}));

        MilvusRowDataConverter converter = new MilvusRowDataConverter(names, types);
        JsonObject result = converter.convert(row);

        JsonArray arr = result.get("embedding").getAsJsonArray();
        assertEquals(3, arr.size());
        assertEquals(0.1f, arr.get(0).getAsFloat(), 0.001f);
        assertEquals(0.2f, arr.get(1).getAsFloat(), 0.001f);
        assertEquals(0.3f, arr.get(2).getAsFloat(), 0.001f);
    }

    @Test
    void testConvertBinaryField() throws Exception {
        List<String> names = Arrays.asList("vec");
        List<LogicalType> types = Arrays.asList(new VarBinaryType());

        GenericRowData row = new GenericRowData(1);
        row.setField(0, new byte[]{0x01, 0x02, (byte) 0xFF});

        MilvusRowDataConverter converter = new MilvusRowDataConverter(names, types);
        JsonObject result = converter.convert(row);

        JsonArray arr = result.get("vec").getAsJsonArray();
        assertEquals(3, arr.size());
        assertEquals(1, arr.get(0).getAsInt());
        assertEquals(2, arr.get(1).getAsInt());
        assertEquals(255, arr.get(2).getAsInt());
    }

    @Test
    void testNullFieldProducesNull() throws Exception {
        List<String> names = Arrays.asList("id", "name");
        List<LogicalType> types = Arrays.asList(new BigIntType(), new VarCharType());

        GenericRowData row = new GenericRowData(2);
        row.setField(0, 1L);
        row.setField(1, null);  // null field

        MilvusRowDataConverter converter = new MilvusRowDataConverter(names, types);
        JsonObject result = converter.convert(row);

        assertEquals(1L, result.get("id").getAsLong());
        assertTrue(result.get("name").isJsonNull());
    }
}
