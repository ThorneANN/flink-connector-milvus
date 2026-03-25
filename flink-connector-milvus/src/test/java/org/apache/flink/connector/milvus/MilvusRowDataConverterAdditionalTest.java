package org.apache.flink.connector.milvus;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.apache.flink.connector.milvus.table.MilvusRowDataConverter;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.*;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** Additional unit tests for {@link MilvusRowDataConverter} covering more types and edge cases. */
class MilvusRowDataConverterAdditionalTest {

    @Test
    void testConvertTinyIntAndSmallInt() throws Exception {
        List<String> names = Arrays.asList("tiny", "small");
        List<LogicalType> types = Arrays.asList(new TinyIntType(), new SmallIntType());

        GenericRowData row = new GenericRowData(2);
        row.setField(0, (byte) 127);
        row.setField(1, (short) 32000);

        JsonObject result = new MilvusRowDataConverter(names, types).convert(row);

        assertEquals(127, result.get("tiny").getAsInt());
        assertEquals(32000, result.get("small").getAsInt());
    }

    @Test
    void testConvertIntArray() throws Exception {
        List<String> names = Arrays.asList("ids");
        List<LogicalType> types = Arrays.asList(new ArrayType(new IntType()));

        GenericRowData row = new GenericRowData(1);
        row.setField(0, new GenericArrayData(new int[]{10, 20, 30}));

        JsonObject result = new MilvusRowDataConverter(names, types).convert(row);

        JsonArray arr = result.get("ids").getAsJsonArray();
        assertEquals(3, arr.size());
        assertEquals(10, arr.get(0).getAsInt());
        assertEquals(20, arr.get(1).getAsInt());
        assertEquals(30, arr.get(2).getAsInt());
    }

    @Test
    void testConvertDoubleArray() throws Exception {
        List<String> names = Arrays.asList("scores");
        List<LogicalType> types = Arrays.asList(new ArrayType(new DoubleType()));

        GenericRowData row = new GenericRowData(1);
        row.setField(0, new GenericArrayData(new double[]{1.1, 2.2, 3.3}));

        JsonObject result = new MilvusRowDataConverter(names, types).convert(row);

        JsonArray arr = result.get("scores").getAsJsonArray();
        assertEquals(3, arr.size());
        assertEquals(1.1, arr.get(0).getAsDouble(), 0.001);
        assertEquals(2.2, arr.get(1).getAsDouble(), 0.001);
        assertEquals(3.3, arr.get(2).getAsDouble(), 0.001);
    }

    @Test
    void testConvertAllFieldsNull() throws Exception {
        List<String> names = Arrays.asList("a", "b", "c");
        List<LogicalType> types = Arrays.asList(new BigIntType(), new VarCharType(), new FloatType());

        GenericRowData row = new GenericRowData(3); // all fields null by default

        JsonObject result = new MilvusRowDataConverter(names, types).convert(row);

        assertTrue(result.get("a").isJsonNull());
        assertTrue(result.get("b").isJsonNull());
        assertTrue(result.get("c").isJsonNull());
    }

    @Test
    void testConvertEmptyRow() throws Exception {
        JsonObject result = new MilvusRowDataConverter(
                Arrays.asList(), Arrays.asList()).convert(new GenericRowData(0));
        assertNotNull(result);
        assertEquals(0, result.size());
    }

    @Test
    void testConvertNestedRow() throws Exception {
        RowType innerType = new RowType(Arrays.asList(
                new RowType.RowField("name", new VarCharType()),
                new RowType.RowField("score", new FloatType())
        ));

        GenericRowData inner = new GenericRowData(2);
        inner.setField(0, StringData.fromString("alice"));
        inner.setField(1, 9.5f);

        GenericRowData outer = new GenericRowData(1);
        outer.setField(0, inner);

        JsonObject result = new MilvusRowDataConverter(
                Arrays.asList("meta"), Arrays.asList(innerType)).convert(outer);

        JsonObject meta = result.getAsJsonObject("meta");
        assertNotNull(meta);
        assertEquals("alice", meta.get("name").getAsString());
        assertEquals(9.5f, meta.get("score").getAsFloat(), 0.001f);
    }
}
