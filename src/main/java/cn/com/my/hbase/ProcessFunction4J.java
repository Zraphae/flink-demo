package cn.com.my.hbase;

import com.google.common.collect.Lists;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;
import java.util.Objects;

@Slf4j
@Builder
public class ProcessFunction4J extends ProcessFunction<List<Row>, List<String>> {

    private String rowKeyFiled;
    private String tableNameStr;
    private RowTypeInfo dataRow;
    private String hbaseZookeeperQuorum;
    private String hbaseZookeeperClientPort;

    private Connection conn;
    private Table table;
    private String family;


    @Override
    public void open(Configuration parameters) throws Exception {
        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
        conf.set(HConstants.ZOOKEEPER_QUORUM, this.hbaseZookeeperQuorum);
        conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, this.hbaseZookeeperClientPort);
        conf.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 30000);
        conf.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 30000);
        this.conn = ConnectionFactory.createConnection(conf);

        TableName tableName = TableName.valueOf(this.tableNameStr);

        this.table = conn.getTable(tableName);

    }

    @Override
    public void processElement(List<Row> rows, Context ctx, Collector<List<String>> out) throws Exception {
        log.info("======>input: {}", rows);
        List<String> kafkaMsgs = Lists.newArrayList();
//        Get get = new Get(Bytes.toBytes("135"));
//        Result result = this.table.get(get);
//        byte[] tsValue = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("ts"));
//        log.info("======>output: {}", Bytes.toString(tsValue));
//        kafkaMsgs.add(Bytes.toString(tsValue));
        kafkaMsgs.add("123123");
        kafkaMsgs.add("456789");
        out.collect(kafkaMsgs);
    }

    @Override
    public void close() throws Exception {
        if (Objects.isNull(table)) table.close();
        if (Objects.isNull(conn)) conn.close();
    }


}
