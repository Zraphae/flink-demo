package cn.com.my;

import cn.com.my.common.schemas.RowSchema;
import cn.com.my.common.utils.ExecutionEnvUtil;
import cn.com.my.common.utils.GsonUtil;
import cn.com.my.common.utils.KafkaConfigUtil;
import cn.com.my.es.ElasticSearchSinkUtil;
import cn.com.my.hbase.HBaseOutputFormat4J;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.table.api.TableSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.http.HttpHost;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.List;

import static cn.com.my.common.constant.PropertiesConstants.*;

@Slf4j
public class Main {

    public static void main(String[] args) throws Exception {

        // Read parameters from command line
//        final ParameterTool params = ParameterTool.fromArgs(args);
//        if(params.getNumberOfParameters() < 4) {
//            log.error("Usage: FlinkReadKafka " +
//                    "--read-topic <topic> " +
//                    "--write-topic <topic> " +
//                    "--bootstrap.servers <kafka brokers> " +
//                    "--group.id <groupid>");
//            return;
//        }

        final ParameterTool params = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(params);
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
        env.enableCheckpointing(300000); // 300 seconds

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // define a schema
        String[] fieldNames = { "id", "num", "ts", "timestampOp"};
        TypeInformation<?>[] dataTypes = { Types.STRING, Types.LONG, Types.STRING, Types.LONG };
        TypeInformation<Row> dataRow = Types.ROW_NAMED(fieldNames, dataTypes);

        DataStreamSource kafkaStream = KafkaConfigUtil.buildSource(env, new SimpleStringSchema());
//        DataStreamSource<MetricEvent> kafkaStream = KafkaConfigUtil.buildSource(env);


        // convert DataStream to Table
//        Table kafkaTable = tEnv.fromDataStream(kafkaStream, "id, num, ts, timestampOp");

        // register DataStream as Table
        tEnv.registerDataStream("kafkaTable", kafkaStream);
        Table result = tEnv.sqlQuery("SELECT id, num+1 as num, ts, timestampOp FROM kafkaTable");

        DataStream<Row> rowDataStream = tEnv.toAppendStream(result, dataRow);
        rowDataStream.print();
//        tEnv.toAppendStream(result, MetricEvent.class).print();


//        tEnv.toAppendStream(result, dataRow).addSink(new HBaseSink4J(configuration, dataRow));
//        tEnv.toAppendStream(result, dataRow).writeUsingOutputFormat(new HBaseOutputFormat());

//        HBaseOutputFormat4J hBaseOutputFormat4J = HBaseOutputFormat4J.builder()
//                .hbaseZookeeperQuorum("localhost")
//                .hbaseZookeeperClientPort("2181")
//                .dataRow((RowTypeInfo)dataRow)
//                .tableNameStr("test")
//                .family("info")
//                .rowKeyFiled("id")
//                .build();
//        rowDataStream.writeUsingOutputFormat(hBaseOutputFormat4J);

//        List<HttpHost> esAddresses = ElasticSearchSinkUtil.getEsAddresses(params.get(ELASTICSEARCH_HOSTS));
//        int bulkSize = params.getInt(ELASTICSEARCH_BULK_FLUSH_MAX_ACTIONS, 40);
//        int sinkParallelism = params.getInt(STREAM_SINK_PARALLELISM, 5);
//
//        log.info("-----esAddresses: {}, parameterTool: {}, ", esAddresses, params);
//
//
//        ElasticSearchSinkUtil.addSink(esAddresses, bulkSize, sinkParallelism, rowDataStream,
//                (Row record, RuntimeContext runtimeContext, RequestIndexer requestIndexer) ->
//                        requestIndexer.add(Requests.indexRequest()
//                        .index(getEsIndex(record))
//                        .type(getEsType(record))
//                        .source(GsonUtil.toJSONBytes(record), XContentType.JSON)));

        env.execute("flink demo");
    }

    private static String getEsType(Row record) {
        return record.getField(0) + "";
    }

    private static String getEsIndex(Row record) {
        return record.getField(0) + "";
    }
}
