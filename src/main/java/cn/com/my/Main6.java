package cn.com.my;

import cn.com.my.common.model.OGGMessage;
import cn.com.my.common.utils.ExecutionEnvUtil;
import cn.com.my.hbase.HBaseWriter4JV2;
import cn.com.my.hbase.ProcessFunction4JV2;
import cn.com.my.kafka.OGGMessageDeserializationSchema;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
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
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Properties;


@Slf4j
public class Main6 {

    private static final String FLINK_CHECKPOINT_PATH = "hdfs://pengzhaos-MacBook-Pro.local:9000/checkpoints-data/";

    public static void main(String[] args) throws Exception {

        // Read parameters from command line
        final ParameterTool params = ParameterTool.fromArgs(args);
//
//        if (params.getNumberOfParameters() < 4) {
//            log.info("\nUsage: FlinkReadKafka " +
//                    "--read-topic <topic> " +
//                    "--write-topic <topic> " +
//                    "--bootstrap.servers <kafka brokers> " +
//                    "--group.id <groupid>");
//            return;
//        }
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(params);

        // start a checkpoint every 5000 ms
        env.enableCheckpointing(5000);

        env.setStateBackend((StateBackend) new RocksDBStateBackend(FLINK_CHECKPOINT_PATH, true));
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

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);


        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test");
        FlinkKafkaConsumer<OGGMessage> flinkKafkaConsumer = new FlinkKafkaConsumer<>(
                "test",
                new OGGMessageDeserializationSchema(),
                properties);
        flinkKafkaConsumer.setStartFromGroupOffsets();

        DataStream<OGGMessage> stream = env.addSource(flinkKafkaConsumer);

        if (log.isDebugEnabled()) {
            stream.print();
        }
        stream.print();

        SingleOutputStreamOperator<List<OGGMessage>> apply = stream.keyBy(new KeySelector<OGGMessage, String>() {
            @Override
            public String getKey(OGGMessage value) throws Exception {
                return value.getKey();
            }
        }).window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .apply(new WindowFunction<OGGMessage, List<OGGMessage>, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<OGGMessage> input, Collector<List<OGGMessage>> out) {
                        List<OGGMessage> oggMessages = Lists.newArrayList();
                        input.forEach(oggMessage -> oggMessages.add(oggMessage));
                        out.collect(oggMessages);
                    }
                });

        HBaseWriter4JV2 hBaseWriter = HBaseWriter4JV2.builder()
                .hbaseZookeeperQuorum("localhost")
                .hbaseZookeeperClientPort("2181")
                .tableNameStr("test")
                .family("info")
                .build();
        apply.addSink(hBaseWriter);

        ProcessFunction4JV2 processFunction = ProcessFunction4JV2.builder()
                .hbaseZookeeperQuorum("localhost")
                .hbaseZookeeperClientPort("2181")
                .tableNameStr("test")
                .family("info")
                .build();
        SingleOutputStreamOperator<List<String>> process = apply.process(processFunction);
        process.print();

        SingleOutputStreamOperator<String> stringSingleOutputStreamOperator = process
                .flatMap(new FlatMapFunction<List<String>, String>() {
                    @Override
                    public void flatMap(List<String> value, Collector<String> out) throws Exception {
                        value.forEach(record -> out.collect(record));
                    }
                });
        stringSingleOutputStreamOperator.print();

        Properties properties1 = new Properties();
        FlinkKafkaProducer flinkKafkaProducer = new FlinkKafkaProducer(
                "my-topic",
                (KafkaSerializationSchema) new SimpleStringSchema(),
                properties1,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);

        stringSingleOutputStreamOperator.addSink(flinkKafkaProducer);

        env.execute("flink demo");
    }


}