package cn.com.my.hbase;

import cn.com.my.common.model.OGGMessage;
import cn.com.my.common.utils.GsonUtil;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.gson.JsonObject;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

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
    private String primaryKeyName;


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
            JsonObject jsonObject = GsonUtil.parse2JsonObj(oggMessage.getData().toString());
            String[] primaryKeys = primaryKeyName.split(",");

            String[] keyValues = new String[primaryKeys.length];
            for(int index=0; index<primaryKeys.length; index++){
                String keyValue = jsonObject.get(primaryKeys[index]).getAsString();
                keyValues[index] = keyValue;
            }
            String primaryKeyValue = Joiner.on("_").join(keyValues);
            Get get = new Get(Bytes.toBytes(primaryKeyValue));
            gets.add(get);
        }
        Result[] results = this.table.get(gets);

        Preconditions.checkArgument(input.size() == results.length, "input.size() != results.length");

        for(int index=0; index<results.length; index++){
            List<Cell> cells = results[index].listCells();
            JsonObject jsonObject = new JsonObject();
            for (Cell cell : cells) {
                String key = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                jsonObject.addProperty(key, value);
            }
            OGGMessage oggMessage = input.get(index);
            oggMessage.setData(jsonObject.toString());
            kafkaMsgs.add(GsonUtil.toJson(oggMessage));
        }

        out.collect(kafkaMsgs);
    }

    @Override
    public void close() throws Exception {
        if (!Objects.isNull(table)) table.close();
        if (!Objects.isNull(conn)) conn.close();
    }


}
