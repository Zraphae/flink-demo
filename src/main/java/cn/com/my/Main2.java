package cn.com.my;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Rowtime;
import org.apache.flink.table.descriptors.Schema;


public class Main2 {
    public static void main(String[] args) throws Exception {

        // Read parameters from command line
        final ParameterTool params = ParameterTool.fromArgs(args);

        if(params.getNumberOfParameters() < 4) {
            System.out.println("\nUsage: FlinkReadKafka " +
                    "--read-topic <topic> " +
                    "--write-topic <topic> " +
                    "--bootstrap.servers <kafka brokers> " +
                    "--group.id <groupid>");
            return;
        }


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
        env.enableCheckpointing(300000); // 300 seconds
        env.getConfig().setGlobalJobParameters(params);

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        tableEnvironment
                // declare the external system to connect to
                .connect(
                        new Kafka()
                                .version("0.10")
                                .topic("test-input")
                                .startFromEarliest()
                                .property("zookeeper.connect", "localhost:2181")
                                .property("bootstrap.servers", "localhost:9092")
                )

                // declare a format for this system
//                .withFormat(
//                        new Json
//                                .(
//                                        "{" +
//                                                "  \"namespace\": \"org.myorganization\"," +
//                                                "  \"type\": \"record\"," +
//                                                "  \"name\": \"UserMessage\"," +
//                                                "    \"fields\": [" +
//                                                "      {\"name\": \"timestamp\", \"type\": \"string\"}," +
//                                                "      {\"name\": \"user\", \"type\": \"long\"}," +
//                                                "      {\"name\": \"message\", \"type\": [\"string\", \"null\"]}" +
//                                                "    ]" +
//                                                "}"
//                                )
//                )
                // declare the schema of the table
                .withSchema(
                        new Schema()
                                .field("rowtime", Types.SQL_TIMESTAMP)
                                .rowtime(new Rowtime()
                                        .timestampsFromField("timestamp")
                                        .watermarksPeriodicBounded(60000)
                                )
                                .field("user", Types.LONG)
                                .field("message", Types.STRING)
                )

                // specify the update-mode for streaming tables
                .inAppendMode()

                // register as source, sink, or both and under a name
                .registerTableSource("MyUserTable");
        env.execute("FlinkReadWriteKafkaJSON");
    }
}