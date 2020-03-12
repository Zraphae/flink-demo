package cn.com.my.es;

import cn.com.my.common.model.OGGMessage;
import cn.com.my.common.utils.GsonUtil;
import cn.com.my.common.utils.HBaseUtils;
import lombok.Builder;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.List;


@Builder
public class MyESSinkFunction implements ElasticsearchSinkFunction<List<OGGMessage>> {

    private String indexName;
    private String esIndexFields;
    private String primaryKeys;


    @Override
    public void process(List<OGGMessage> element, RuntimeContext ctx, RequestIndexer indexer) {

//        BulkRequest bulkRequest = Requests.bulkRequest();
        element.forEach(oggMessage -> {
            IndexRequest indexRequest = Requests.indexRequest()
                    .index(this.indexName)
                    .id(HBaseUtils.getHBaseRowKey(oggMessage, this.primaryKeys))
                    .source(GsonUtil.toJSONBytes(oggMessage, this.esIndexFields), XContentType.JSON);
            indexer.add(indexRequest);
//            bulkRequest.add(indexRequest);
        });

//        indexer.add(bulkRequest);
    }
}
