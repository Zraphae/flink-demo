package cn.com.my.kafka;

import cn.com.my.common.model.OGGMessage;
import cn.com.my.common.utils.GsonUtil;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.nio.charset.Charset;
import java.util.Objects;

public class OGGMessageDeserializationSchema implements KafkaDeserializationSchema<OGGMessage> {

    @Override
    public boolean isEndOfStream(OGGMessage nextElement) {
        return false;
    }

    @Override
    public OGGMessage deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {

        OGGMessage oggMessage = new OGGMessage();
        if(!Objects.isNull(record)){
            oggMessage = GsonUtil.fromJson(new String(record.value(), Charset.forName("UTF-8")), OGGMessage.class);
            oggMessage.setOffset(record.offset());
            oggMessage.setTopic(record.topic());
            oggMessage.setPartition(record.partition());
            oggMessage.setKey(new String(record.key(), Charset.forName("UTF-8")));
        }
        return oggMessage;
    }

    @Override
    public TypeInformation<OGGMessage> getProducedType() {
        return null;
    }
}
