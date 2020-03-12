package cn.com.my.es;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.flink.types.Row;
import org.apache.http.HttpHost;

import java.util.List;


@Deprecated
@Slf4j
public class ElasticSearchSinkUtil {

    @Deprecated
    public static void addSink(List<HttpHost> hosts, int bulkFlushMaxActions, int parallelism,
                               DataStream<Row> data, ElasticsearchSinkFunction<Row> func) {
        ElasticsearchSink.Builder<Row> esSinkBuilder = new ElasticsearchSink.Builder<>(hosts, func);
        esSinkBuilder.setBulkFlushMaxActions(bulkFlushMaxActions);
        esSinkBuilder.setFailureHandler(new RetryRejectedExecutionFailureHandler());
        data.addSink(esSinkBuilder.build()).setParallelism(parallelism);
    }


}
