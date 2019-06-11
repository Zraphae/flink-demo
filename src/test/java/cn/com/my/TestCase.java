package cn.com.my;

import cn.com.my.common.utils.GsonUtil;
import com.google.gson.JsonObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.beanutils.DynaBean;
import org.apache.commons.beanutils.LazyDynaBean;
import org.apache.commons.beanutils.LazyDynaClass;
import org.apache.commons.beanutils.MutableDynaClass;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.util.Arrays;
import java.util.Optional;


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
}