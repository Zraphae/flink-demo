package cn.com.my.hbase;

import cn.com.my.common.model.OGGMessage;
import com.google.common.collect.Lists;
import com.google.gson.JsonObject;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Slf4j
@Builder
public class ProcessFunction4JV2 extends ProcessFunction<List<OGGMessage>, List<String>> {

    private String rowKeyFiled;
    private String tableNameStr;
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
    public void processElement(List<OGGMessage> input, Context ctx, Collector<List<String>> out) throws Exception {

        if (log.isDebugEnabled()) {
            log.debug("====>input: {}", input);
        }

        List<String> kafkaMsgs = Lists.newArrayList();
        List<Get> gets = Lists.newArrayList();
        for (OGGMessage oggMessage : input) {
            Get get = new Get(Bytes.toBytes(oggMessage.getKey()));
            gets.add(get);

        }
        Result[] results = this.table.get(gets);
        for (Result result : results) {
            List<Cell> cells = result.listCells();
            JsonObject jsonObject = new JsonObject();
            for (Cell cell : cells) {
                String key = Bytes.toString(cell.getQualifierArray());
                String value = Bytes.toString(cell.getValueArray());
                jsonObject.addProperty(key, value);
            }
            kafkaMsgs.add(jsonObject.toString());
        }
        out.collect(kafkaMsgs);
    }

    @Override
    public void close() throws Exception {
        if (Objects.isNull(table)) table.close();
        if (Objects.isNull(conn)) conn.close();
    }


}
