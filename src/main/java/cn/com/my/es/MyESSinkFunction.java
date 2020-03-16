package cn.com.my.es;

import cn.com.my.common.constant.OGGOpType;
import cn.com.my.common.model.OGGMessage;
import cn.com.my.common.utils.GsonUtil;
import cn.com.my.common.utils.HBaseUtils;
import com.google.gson.JsonObject;
import lombok.Builder;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.List;
import java.util.Map;


@Builder
public class MyESSinkFunction implements ElasticsearchSinkFunction<List<OGGMessage>> {

    private String indexName;
    private String esIndexFields;
    private String primaryKeys;


    @Override
    public void process(List<OGGMessage> element, RuntimeContext ctx, RequestIndexer indexer) {

        element.forEach(oggMessage -> {

            OGGOpType oggOpType = OGGOpType.valueOf(oggMessage.getOpType());
            switch (oggOpType) {
                case U:
                case D:
                    indexer.add(this.getEsUpdateIndex(oggMessage));
                    break;
                case I:
                    indexer.add(this.getEsIndex(oggMessage));
                    break;
            }
        });

    }

    private UpdateRequest getEsUpdateIndex(OGGMessage oggMessage) {
        UpdateRequest updateRequest = new UpdateRequest().index(this.indexName)
//                .type("test_type")
                .id(HBaseUtils.getHBaseRowKey(oggMessage, this.primaryKeys))
                .doc(this.toJSONBytes(oggMessage, this.esIndexFields), XContentType.JSON);
        return updateRequest;

    }

    private IndexRequest getEsIndex(OGGMessage oggMessage) {
        IndexRequest indexRequest = Requests.indexRequest()
                .index(this.indexName)
//                    .type("test_type")
                .id(HBaseUtils.getHBaseRowKey(oggMessage, this.primaryKeys))
                .source(this.toJSONBytes(oggMessage, this.esIndexFields), XContentType.JSON);
        return indexRequest;

    }

    private byte[] toJSONBytes(OGGMessage oggMessage, String esIndexFields) {

        if (StringUtils.isBlank(esIndexFields)) {
            return "".getBytes(GsonUtil.DEFAULT_CHARSET);
        }

        JsonObject result = new JsonObject();
        if (StringUtils.equals(oggMessage.getOpType(), OGGOpType.D.getValue())) {
            result.addProperty(HBaseUtils.DELETE_FLAG, String.valueOf(true));
            return result.toString().getBytes(GsonUtil.DEFAULT_CHARSET);
        }

        String[] fields = esIndexFields.split(",");
        Map<String, String> keyValues = oggMessage.getKeyValues();
        for (int index = 0; index < fields.length; index++) {
            String key = fields[index];
            String value = keyValues.get(key);

            if (StringUtils.equals(oggMessage.getOpType(), OGGOpType.I.getValue())) {
                if (StringUtils.isBlank(value)) {
                    value = HBaseUtils.NULL_STRING;
                }
                result.addProperty(key, value);
            } else {
                if (!keyValues.containsKey(key)) {
                    continue;
                } else {
                    if (StringUtils.isBlank(value)) {
                        value = HBaseUtils.NULL_STRING;
                    }
                    result.addProperty(key, value);
                }
            }
        }

        return result.toString().getBytes(GsonUtil.DEFAULT_CHARSET);
    }

}
