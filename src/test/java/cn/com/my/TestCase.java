package cn.com.my;

import cn.com.my.common.model.OGGMessage;
import cn.com.my.common.utils.GsonUtil;
import cn.com.my.common.utils.OrcBatchReader;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.beanutils.DynaBean;
import org.apache.commons.beanutils.LazyDynaBean;
import org.apache.commons.beanutils.LazyDynaClass;
import org.apache.commons.beanutils.MutableDynaClass;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.orc.TypeDescription;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Set;


@Slf4j
public class TestCase {

    @Test
    public void test1() {

        String[] fieldNames = {"id", "num", "ts", "timestampOp"};
        TypeInformation<?>[] dataTypes = {Types.INT, Types.LONG, Types.STRING, Types.LONG};
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

    @Test
    public void test2() {

        MutableDynaClass dynaClass = new LazyDynaClass();    // create DynaClass
        dynaClass.add("price", java.lang.Integer.class);  // add property
        DynaBean dynaBean = new LazyDynaBean(dynaClass);     // Create DynaBean with associated DynaClass

        dynaBean.set("price", 123);

        log.info("==>{}", dynaBean);
    }

    @Test
    public void test3() {

        DynaBean dynaBean = new LazyDynaBean();                // Create LazyDynaBean
        MutableDynaClass dynaClass =
                (MutableDynaClass) dynaBean.getDynaClass();    // get DynaClass
        dynaClass.add("price", java.lang.Integer.class);    // add property

        dynaBean.set("price", 123);
        log.info("===>{}", dynaBean.get("price"));

    }

    @Test
    public void test4() {

        JsonObject result = new JsonObject();
        result.addProperty("id", "123");
        result.addProperty("num", 1001);

        log.info("==>{}", result);

        String[] fieldNames = {"id", "num", "ts", "timestampOp"};
        TypeInformation<?>[] dataTypes = {Types.INT, Types.LONG, Types.STRING, Types.LONG};
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

    @Test
    public void test5() {

        String[] fieldNames = {"id", "num", "ts", "timestampOp"};
        TypeInformation<?>[] dataTypes = {Types.INT, Types.LONG, Types.STRING, Types.LONG};
        TypeInformation<Row> dataRow = Types.ROW_NAMED(fieldNames, dataTypes);

        String[] esFieldNames = {"id", "ts"};

        Row record = new Row(4);
        record.setField(0, 1);
        record.setField(1, 123123L);
        record.setField(2, "1sdkjk");
        record.setField(3, 2L);

        byte[] bytes = GsonUtil.toJSONBytes(record, esFieldNames, (RowTypeInfo) dataRow);

        log.info("==>{}", new String(bytes));
    }

    @Test
    public void testFlatSchemaToTypeInfo1() {

        String schema =
                "struct<" +
                        "boolean1:boolean," +
                        "byte1:tinyint," +
                        "short1:smallint," +
                        "int1:int," +
                        "long1:bigint," +
                        "float1:float," +
                        "double1:double," +
                        "bytes1:binary," +
                        "string1:string," +
                        "date1:date," +
                        "timestamp1:timestamp," +
                        "decimal1:decimal(5,2)" +
                        ">";
        TypeInformation typeInfo = OrcBatchReader.schemaToTypeInfo(TypeDescription.fromString(schema));

        log.info("==>{}", typeInfo);

        Assert.assertNotNull(typeInfo);
        Assert.assertTrue(typeInfo instanceof RowTypeInfo);
        RowTypeInfo rowTypeInfo = (RowTypeInfo) typeInfo;

        // validate field types
        Assert.assertArrayEquals(
                new TypeInformation[]{
                        Types.BOOLEAN, Types.BYTE, Types.SHORT, Types.INT, Types.LONG, Types.FLOAT, Types.DOUBLE,
                        PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO, Types.STRING,
                        Types.SQL_DATE, Types.SQL_TIMESTAMP, BasicTypeInfo.BIG_DEC_TYPE_INFO
                },
                rowTypeInfo.getFieldTypes());

        // validate field names
        Assert.assertArrayEquals(
                new String[]{
                        "boolean1", "byte1", "short1", "int1", "long1", "float1", "double1",
                        "bytes1", "string1", "date1", "timestamp1", "decimal1"
                },
                rowTypeInfo.getFieldNames());

    }

    @Test
    public void testTableSchemaBuilder() {
        String[] fieldNames = {"id", "num", "ts", "timestampOp"};
        DataType[] dataTypes = {DataTypes.STRING(), DataTypes.BIGINT(), DataTypes.STRING(), DataTypes.BIGINT()};
        TableSchema.Builder builder = TableSchema.builder();
        for (int index = 0; index < fieldNames.length; index++) {
            builder.field(fieldNames[index], dataTypes[index]);
        }
        log.info("===>{}", builder.build());

    }


    @Test
    public void testOGGMessageParse() {
        OGGMessage message = OGGMessage.builder()
                .table("test")
                .opType("I")
                .opTs("2020-02-26 02:31:31")
                .pos("13123123123")
                .data("{\"is_hit\":\"N\",\"seq_no\":\"123878646264264\"}")
                .build();

        String json = GsonUtil.toJson(message);
        log.info("==>jsonStr: {}", json);

        JsonObject jsonObject = GsonUtil.parse2JsonObj(message.getData());
        log.info("==>dataJsonObject: {}", jsonObject);

        Set<Map.Entry<String, JsonElement>> entries = jsonObject.entrySet();
        entries.forEach(element ->
                log.info("element.key: {}, element.value: {}", element.getKey(), element.getValue().getAsString()));

    }
}