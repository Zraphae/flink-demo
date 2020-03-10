package cn.com.my.hbase;

import cn.com.my.common.model.OGGMessage;
import cn.com.my.common.utils.GsonUtil;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
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
import java.util.Map;
import java.util.Objects;
import java.util.Set;

@Slf4j
@Builder
public class HBaseWriter4JV2 extends RichSinkFunction<List<OGGMessage>> implements SinkFunction<List<OGGMessage>> {

    private String tableNameStr;
    private RowTypeInfo dataRow;
    private String hbaseZookeeperQuorum;
    private String hbaseZookeeperClientPort;
    private Connection conn;
    private BufferedMutator mutator;
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
        BufferedMutatorParams params = new BufferedMutatorParams(tableName);
        //设置缓存1m，当达到1m时数据会自动刷到hbase
        params.writeBufferSize(1024 * 1024); //设置缓存的大小
        this.mutator = conn.getBufferedMutator(params);

    }


    @Override
    public void invoke(List<OGGMessage> records, Context context) throws Exception {

        if (log.isDebugEnabled()) {
            log.debug("=========>records: {}", records);
        }

        for (OGGMessage record : records) {
            JsonObject jsonObject = GsonUtil.parse2JsonObj(record.getData().toString());
            Set<Map.Entry<String, JsonElement>> entries = jsonObject.entrySet();
            Put put = new Put(Bytes.toBytes(jsonObject.get(primaryKeyName).getAsString()));
            for (Map.Entry<String, JsonElement> entry : entries) {
                put.addColumn(Bytes.toBytes(this.family), Bytes.toBytes(entry.getKey()),Bytes.toBytes(entry.getValue().getAsString()));
            }
            this.mutator.mutate(put);
        }
        log.info("==>hbase mutator flush, flush size: {}", records.size());
        this.mutator.flush();
    }


    @Override
    public void close() throws Exception {
        if (Objects.isNull(mutator)) mutator.close();
        if (Objects.isNull(conn)) conn.close();
    }


}
