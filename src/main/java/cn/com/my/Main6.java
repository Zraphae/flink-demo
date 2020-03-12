package cn.com.my;

import cn.com.my.common.constant.PropertiesConstants;
import cn.com.my.common.model.OGGMessage;
import cn.com.my.common.schemas.OGGMessageSchema;
import cn.com.my.common.utils.ElasticSearchUtils;
import cn.com.my.common.utils.ExecutionEnvUtil;
import cn.com.my.es.MyESSinkFunction;
import cn.com.my.es.RetryRejectedExecutionFailureHandler;
import cn.com.my.hbase.HBaseWriter4JV2;
import cn.com.my.hbase.ProcessFunction4JV3;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.http.HttpHost;

import java.util.List;
import java.util.Properties;

import static cn.com.my.common.constant.PropertiesConstants.ELASTICSEARCH_BULK_FLUSH_MAX_ACTIONS;
import static cn.com.my.common.constant.PropertiesConstants.STREAM_SINK_PARALLELISM;


@Slf4j
public class Main6 {

    private static final String FLINK_CHECKPOINT_PATH = "hdfs://pengzhaos-MacBook-Pro.local:9000/checkpoints-data/";

    public static void main(String[] args) throws Exception {

        // Read parameters from command line
        final ParameterTool params = ParameterTool.fromArgs(args);

//        if (params.getNumberOfParameters() < 13) {
//            log.info("\nUsage: FlinkKafka " +
//                    "--app.name <appName> " +
//                    "--read-topic <readTopic> " +
//                    "--write-topic <writeTopic> " +
//                    "--read.bootstrap.servers <readBootstrapServers> " +
//                    "--write.bootstrap.servers <writeBootstrapServers> " +
//                    "--read.group.id <readGroupId> " +
//                    "--write.group.id <writeGroupId> " +
//                    "--hbase.zookeeper.quorum <hbaseZkQuorum> " +
//                    "--hbase.zookeeper.client.port <hbaseZookeeperClientPort> " +
//                    "--hbase.table.name <hbaseTableName> " +
//                    "--hbase.family.name <hbaseFamilyName> " +
//                    "--primary.key.name <primaryKeyName> " +
//                    "--flink.window.delay <flinkWindowDelay>");
//            return;
//        }

        String appName = params.get("app.name", "test");
        String readTopic = params.get("read.topic", "test");
        String writeTopic = params.get("write.topic", "my-topic");
        String readBootstrapServers = params.get("read.bootstrap.servers", "localhost:9092");
        String writeBootstrapServers = params.get("write.bootstrap.servers", "localhost:9092");
        String readGroupId = params.get("read.group.id", "test");
        String writeGroupId = params.get("write.group.id", "test");
        String hbaseZkQuorum = params.get("hbase.zookeeper.quorum", "localhost");
        String hbaseZookeeperClientPort = params.get("hbase.zookeeper.client.port", "2181");
        String hbaseTableName = params.get("hbase.table.name", "test");
        String hbaseFamilyName = params.get("hbase.family.name", "info");
        String primaryKeyName = params.get("primary.key.name", "seq_no");
        int flinkWindowDelay = params.getInt("flink.window.delay", 20);

        String esHosts = params.get("es.hosts", "127.0.0.1:9200");
        String esIndexFields = params.get("es.index.fields", "seq_no,is_hit");
        String esIndexName = params.get("es.index.name", "test_index");
        int bulkFlushMaxActions = params.getInt(ELASTICSEARCH_BULK_FLUSH_MAX_ACTIONS, 40);
        int sinkParallelism = params.getInt(STREAM_SINK_PARALLELISM, 5);
        long esBulkFlushInterval = params.getLong("es.bulk.flush.interval", 60 * 1000L);



        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(params);

        // start a checkpoint every 5000 ms
        env.enableCheckpointing(5 * 1000L);

        String appCheckpointPath = FLINK_CHECKPOINT_PATH + "/" + readTopic;
        env.setStateBackend((StateBackend) new RocksDBStateBackend(appCheckpointPath, true));
        // set mode to exactly-once (this is the default)
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // make sure 500 ms of progress happen between checkpoints
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);

        // checkpoints have to complete within one minute, or are discarded
        env.getCheckpointConfig().setCheckpointTimeout(60000);

        // allow only one checkpoint to be in progress at the same time
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        env.setParallelism(2);

        // enable externalized checkpoints which are retained after job cancellation
        env.getCheckpointConfig()
                .enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        env.getConfig()
                .setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10 * 1000L));

        Properties readKafkaPro = new Properties();
        readKafkaPro.setProperty(PropertiesConstants.BOOTSTRAP_SERVERS, readBootstrapServers);
        readKafkaPro.setProperty(PropertiesConstants.GROUP_ID, readGroupId);
        FlinkKafkaConsumer<OGGMessage> flinkKafkaConsumer = new FlinkKafkaConsumer<>(
                readTopic,
                new OGGMessageSchema(),
                readKafkaPro);
        flinkKafkaConsumer.setStartFromGroupOffsets();
//        flinkKafkaConsumer.setStartFromLatest();  //for test

        DataStream<OGGMessage> stream = env.addSource(flinkKafkaConsumer);
//        stream.print();

        SingleOutputStreamOperator<List<OGGMessage>> apply = stream.keyBy((KeySelector<OGGMessage, String>) oggMessage ->
                String.valueOf(oggMessage.getPartition())
        ).window(TumblingProcessingTimeWindows.of(Time.seconds(flinkWindowDelay)))
                .apply(new WindowFunction<OGGMessage, List<OGGMessage>, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<OGGMessage> input,
                                      Collector<List<OGGMessage>> out) {
                        List<OGGMessage> oggMessages = Lists.newArrayList();
                        input.forEach(oggMessage -> oggMessages.add(oggMessage));
                        out.collect(oggMessages);
                    }
                });
//        apply.print();

        HBaseWriter4JV2 hBaseWriter = HBaseWriter4JV2.builder()
                .hbaseZookeeperQuorum(hbaseZkQuorum)
                .hbaseZookeeperClientPort(hbaseZookeeperClientPort)
                .tableNameStr(hbaseTableName)
                .family(hbaseFamilyName)
                .primaryKeyName(primaryKeyName)
                .build();
        apply.addSink(hBaseWriter);



        List<HttpHost> esAddresses = ElasticSearchUtils.getEsAddresses(esHosts);
        log.info("-----esAddresses: {}, parameterTool: {}, ", esAddresses, params);

        MyESSinkFunction esSinkFunction = MyESSinkFunction.builder().
                indexName(esIndexName)
                .esIndexFields(esIndexFields)
                .primaryKeys(primaryKeyName)
                .build();

        ElasticsearchSink.Builder<List<OGGMessage>> esSinkBuilder = new ElasticsearchSink.Builder<>(esAddresses, esSinkFunction);
        esSinkBuilder.setBulkFlushMaxActions(bulkFlushMaxActions);
        esSinkBuilder.setBulkFlushInterval(esBulkFlushInterval);
        esSinkBuilder.setFailureHandler(new RetryRejectedExecutionFailureHandler());
        ElasticsearchSink<List<OGGMessage>> esSink = esSinkBuilder.build();

        apply.addSink(esSink);


        ProcessFunction4JV3 processFunction = ProcessFunction4JV3.builder().build();
        SingleOutputStreamOperator<List<String>> process = apply.process(processFunction);
//        process.print();

        SingleOutputStreamOperator<String> stringSingleOutputStreamOperator = process
                .flatMap((FlatMapFunction<List<String>, String>) (value, out) ->
                        value.forEach(record -> out.collect(record)))
                .returns(Types.STRING);
//        stringSingleOutputStreamOperator.print();

        Properties writeKafkaPro = new Properties();
        writeKafkaPro.setProperty(PropertiesConstants.BOOTSTRAP_SERVERS, writeBootstrapServers);
        writeKafkaPro.setProperty(PropertiesConstants.GROUP_ID, writeGroupId);
        writeKafkaPro.setProperty(PropertiesConstants.TRANSACTION_TIMEOUT_MS, String.valueOf(5 * 60 * 1000L));
        FlinkKafkaProducer flinkKafkaProducer = new FlinkKafkaProducer(
                writeTopic,
                new OGGMessageSchema(writeTopic, primaryKeyName),
                writeKafkaPro,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);

        stringSingleOutputStreamOperator.addSink(flinkKafkaProducer);

        env.execute(appName);
    }


}