package cn.com.my.common.utils;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import java.nio.charset.Charset;
import java.util.Objects;


@Slf4j
public class GsonUtil {
    private final static Gson gson = new Gson();

    public static <T> T fromJson(String value, Class<T> type) {
        return gson.fromJson(value, type);
    }

    public static String toJson(Object value) {
        return gson.toJson(value);
    }

    public static byte[] toJSONBytes(Object value) {
        return gson.toJson(value).getBytes(Charset.forName("UTF-8"));
    }

    public static byte[] toJSONBytes(Row record, TableSchema esSchema, RowTypeInfo rowTypeInfo) {

        if (Objects.isNull(esSchema) || esSchema.getFieldCount() == 0) {
            return "".getBytes(Charset.forName("UTF-8"));
        }

        JsonObject result = new JsonObject();
        for (int index = 0; index < esSchema.getFieldCount(); index++) {

            Class<?> typeClass = esSchema.getFieldTypes()[index].getTypeClass();
            String key = esSchema.getFieldNames()[index];
            int fieldIndex = rowTypeInfo.getFieldIndex(key);
            Object value = record.getField(fieldIndex);
            Object type = typeClass.cast(value);

            log.info("key: {}, fieldIndex: {}, value: {}", key, fieldIndex, value);
            if (type instanceof Integer) {
                result.addProperty(key, (int) value);
            } else if (type instanceof Long) {
                result.addProperty(key, (long) value);
            } else if (type instanceof Float) {
                result.addProperty(key, (float) value);
            } else if (type instanceof Double) {
                result.addProperty(key, (double) value);
            } else if (type instanceof String) {
                result.addProperty(key, (String) value);
            } else {
                String errorMessage = String.format(
                        "field index: %s, field value: %s, field type: %s.", index, key, type);
                throw new ClassCastException(errorMessage);
            }

        }
        return result.toString().getBytes(Charset.forName("UTF-8"));
    }
}
