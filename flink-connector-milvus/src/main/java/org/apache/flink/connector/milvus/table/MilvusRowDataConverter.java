package org.apache.flink.connector.milvus.table;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.connector.milvus.MilvusRecordConverter;

import java.util.List;

/**
 * Converts Flink {@link RowData} to {@link JsonObject} for writing to Milvus.
 *
 * <p>Type mapping:
 * <ul>
 *   <li>BOOLEAN → boolean</li>
 *   <li>TINYINT → int (Int8)</li>
 *   <li>SMALLINT → int (Int16)</li>
 *   <li>INT → int (Int32)</li>
 *   <li>BIGINT → long (Int64)</li>
 *   <li>FLOAT → float</li>
 *   <li>DOUBLE → double</li>
 *   <li>VARCHAR / CHAR → string (VarChar)</li>
 *   <li>ARRAY&lt;FLOAT&gt; → JSON float array (FloatVector)</li>
 *   <li>BYTES → base64 string (BinaryVector)</li>
 *   <li>MAP / ROW → nested JSON</li>
 * </ul>
 */
public class MilvusRowDataConverter implements MilvusRecordConverter<RowData> {

    private static final long serialVersionUID = 1L;

    private final List<String> fieldNames;
    private final List<LogicalType> fieldTypes;

    public MilvusRowDataConverter(List<String> fieldNames, List<LogicalType> fieldTypes) {
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
    }

    @Override
    public JsonObject convert(RowData rowData) throws Exception {
        JsonObject json = new JsonObject();
        for (int i = 0; i < fieldNames.size(); i++) {
            String fieldName = fieldNames.get(i);
            LogicalType fieldType = fieldTypes.get(i);
            if (rowData.isNullAt(i)) {
                json.add(fieldName, null);
            } else {
                addField(json, fieldName, rowData, i, fieldType);
            }
        }
        return json;
    }

    private void addField(JsonObject json, String name, RowData row, int pos, LogicalType type) {
        LogicalTypeRoot typeRoot = type.getTypeRoot();
        switch (typeRoot) {
            case BOOLEAN:
                json.addProperty(name, row.getBoolean(pos));
                break;
            case TINYINT:
                json.addProperty(name, row.getByte(pos));
                break;
            case SMALLINT:
                json.addProperty(name, row.getShort(pos));
                break;
            case INTEGER:
                json.addProperty(name, row.getInt(pos));
                break;
            case BIGINT:
                json.addProperty(name, row.getLong(pos));
                break;
            case FLOAT:
                json.addProperty(name, row.getFloat(pos));
                break;
            case DOUBLE:
                json.addProperty(name, row.getDouble(pos));
                break;
            case VARCHAR:
            case CHAR:
                json.addProperty(name, row.getString(pos).toString());
                break;
            case VARBINARY:
            case BINARY:
                // BinaryVector: encode bytes as JSON array of ints
                byte[] bytes = row.getBinary(pos);
                JsonArray byteArray = new JsonArray();
                for (byte b : bytes) {
                    byteArray.add(b & 0xFF);
                }
                json.add(name, byteArray);
                break;
            case ARRAY:
                ArrayType arrayType = (ArrayType) type;
                ArrayData arrayData = row.getArray(pos);
                json.add(name, convertArray(arrayData, arrayType.getElementType()));
                break;
            case MAP:
                // Represent maps as JSON objects with string keys
                json.add(name, new JsonObject());
                break;
            case ROW:
                RowType rowType = (RowType) type;
                RowData nestedRow = row.getRow(pos, rowType.getFieldCount());
                JsonObject nestedJson = new JsonObject();
                List<String> nestedNames = rowType.getFieldNames();
                List<LogicalType> nestedTypes = rowType.getChildren();
                for (int i = 0; i < nestedNames.size(); i++) {
                    if (!nestedRow.isNullAt(i)) {
                        addField(nestedJson, nestedNames.get(i), nestedRow, i, nestedTypes.get(i));
                    }
                }
                json.add(name, nestedJson);
                break;
            default:
                // Fallback: use string representation
                json.addProperty(name, row.getString(pos).toString());
                break;
        }
    }

    private JsonArray convertArray(ArrayData arrayData, LogicalType elementType) {
        JsonArray array = new JsonArray();
        int size = arrayData.size();
        LogicalTypeRoot root = elementType.getTypeRoot();
        for (int i = 0; i < size; i++) {
            if (arrayData.isNullAt(i)) {
                array.add((Number) null);
                continue;
            }
            switch (root) {
                case FLOAT:
                    array.add(arrayData.getFloat(i));
                    break;
                case DOUBLE:
                    array.add(arrayData.getDouble(i));
                    break;
                case INTEGER:
                    array.add(arrayData.getInt(i));
                    break;
                case BIGINT:
                    array.add(arrayData.getLong(i));
                    break;
                case VARCHAR:
                case CHAR:
                    array.add(arrayData.getString(i).toString());
                    break;
                default:
                    array.add(arrayData.getString(i).toString());
                    break;
            }
        }
        return array;
    }
}
