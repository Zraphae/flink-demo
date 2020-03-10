package cn.com.my.hbase;

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.types.Row;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;
import java.util.Objects;

@Slf4j
@Builder
public class HBaseWriter4J extends RichSinkFunction<List<Row>> implements SinkFunction<List<Row>> {

    private String rowKeyFiled;
    private String tableNameStr;
    private RowTypeInfo dataRow;
    private String hbaseZookeeperQuorum;
    private String hbaseZookeeperClientPort;

    private Connection conn;
    private BufferedMutator mutator;
    private String family;
    private int count;
    private long delay;
    private long start;


    @Override
    public void open(Configuration parameters) throws Exception {
        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
        conf.set(HConstants.ZOOKEEPER_QUORUM, this.hbaseZookeeperQuorum);
        conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, this.hbaseZookeeperClientPort);
        conf.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 30000);
        conf.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 30000);
        this.conn = ConnectionFactory.createConnection(conf);

        TableName tableName = TableName.valueOf(this.tableNameStr);
        BufferedMutatorParams params = new BufferedMutatorParams(tableName);
        //设置缓存1m，当达到1m时数据会自动刷到hbase
        params.writeBufferSize(1024 * 1024); //设置缓存的大小
        this.mutator = conn.getBufferedMutator(params);
        this.count = 0;
        this.delay = 0;
        this.start = System.currentTimeMillis();
    }


    @Override
    public void invoke(List<Row> records, Context context) throws Exception {

        log.info("=========>records: {}", records);

        for (Row record : records) {
            if (Objects.isNull(this.dataRow) || this.dataRow.getArity() != record.getArity()) {
                log.warn("Column SQL types array doesn't match arity of passed Row! Check the passed array...");
                continue;
            }

            int idIndex = this.dataRow.getFieldIndex(this.rowKeyFiled);
            Object idFiled = record.getField(idIndex);

            Put put = new Put(Bytes.toBytes(idFiled.toString()));
            for (int index = 0; index < this.dataRow.getArity(); index++) {

                Class<?> typeClass = this.dataRow.getFieldTypes()[index].getTypeClass();
                String key = this.dataRow.getFieldNames()[index];
                Object value = record.getField(index);
                Object type = typeClass.cast(value);

                if (log.isDebugEnabled()) {
                    log.info("key: {}, value: {}, type: {}", key, value, typeClass);
                }
                if (!Objects.isNull(value)) {
                    if (type instanceof Integer) {
                        put.addColumn(Bytes.toBytes(this.family), Bytes.toBytes(key), Bytes.toBytes((int) value));
                    } else if (type instanceof Long) {
                        put.addColumn(Bytes.toBytes(this.family), Bytes.toBytes(key), Bytes.toBytes((long) value));
                    } else if (type instanceof Float) {
                        put.addColumn(Bytes.toBytes(this.family), Bytes.toBytes(key), Bytes.toBytes((float) value));
                    } else if (type instanceof Double) {
                        put.addColumn(Bytes.toBytes(this.family), Bytes.toBytes(key), Bytes.toBytes((double) value));
                    } else if (type instanceof String) {
                        put.addColumn(Bytes.toBytes(this.family), Bytes.toBytes(key), Bytes.toBytes((String) value));
                    } else {
                        String errorMessage = String.format(
                                "field index: %s, field value: %s, field type: %s.", index, key, type);
                        throw new ClassCastException(errorMessage);
                    }
                }
            }
            this.mutator.mutate(put);
        }
        log.info("==>hbase mutator flush, flush size: {}", records.size());
        this.mutator.flush();
    }


    @Override
    public void close() throws Exception {
        if (!Objects.isNull(mutator)) mutator.close();
        if (!Objects.isNull(conn)) conn.close();
    }


}
