package cn.com.my.hbase;

import cn.com.my.common.constant.OGGOpType;
import cn.com.my.common.model.OGGMessage;
import cn.com.my.common.utils.DateUtil;
import cn.com.my.common.utils.HBaseUtils;
import com.google.common.collect.Lists;
import com.google.gson.JsonObject;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;


import java.util.Date;
import java.util.List;
import java.util.Map;


@Slf4j
@Builder
public class ProcessFunction4JV3 extends ProcessFunction<List<OGGMessage>, List<String>> {


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void processElement(List<OGGMessage> input, Context ctx, Collector<List<String>> out) {

        if (log.isDebugEnabled()) {
            log.debug("====>input: {}", input);
        }

        List<String> kafkaMsgs = Lists.newArrayList();

        JsonObject jsonObject = new JsonObject();
        for (OGGMessage oggMessage : input) {

            Map<String, String> keyValues = oggMessage.getKeyValues();
            keyValues.forEach((key, value) -> {
                if (StringUtils.isBlank(value)) {
                    value = HBaseUtils.NULL_STRING;
                }
                jsonObject.addProperty(key, value);
            });


            if (StringUtils.equals(OGGOpType.DELETE.getValue(), oggMessage.getOpType())) {
                jsonObject.addProperty(HBaseUtils.DELETE_FLAG, String.valueOf(true));
            }

            jsonObject.addProperty("op_topic", oggMessage.getTopicName());
            jsonObject.addProperty("op_key", oggMessage.getKey());
            jsonObject.addProperty("op_offset", String.valueOf(oggMessage.getOffset()));
            jsonObject.addProperty("op_partition", String.valueOf(oggMessage.getPartition()));
            jsonObject.addProperty("op_time", DateUtil.format(new Date(), DateUtil.YYYY_MM_DD_HH_MM_SS));

            kafkaMsgs.add(jsonObject.toString());
        }

        out.collect(kafkaMsgs);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }


}
