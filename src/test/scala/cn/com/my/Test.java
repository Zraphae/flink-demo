package cn.com.my;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.Optional;


@Slf4j
public class Test {

    public static void main(String[] args) {

        String[] fieldNames = { "id", "num", "ts", "timestampOp"};
        TypeInformation<?>[] dataTypes = { Types.INT, Types.LONG, Types.STRING, Types.LONG };
        TypeInformation<Row> dataRow = Types.ROW_NAMED(fieldNames, dataTypes);

        log.info("dataRow: {}", dataRow);

        TableSchema tableSchema = TableSchema.fromTypeInfo(dataRow);
        log.info("tableSchema: {}", tableSchema);

        log.info("===>{}", Arrays.toString(tableSchema.getFieldNames()));
        log.info("===>{}", Arrays.toString(tableSchema.getFieldTypes()));

        Optional<TypeInformation<?>> fieldType = tableSchema.getFieldType(0);
        Class<?> typeClass = fieldType.get().getTypeClass();

        Object a = 123;
        log.info("===>{}", typeClass.cast(a));
        log.info("===>{}", typeClass.cast(a) instanceof Integer);
    }
}