package cn.com.my.kafka;

import cn.com.my.common.model.OGGMessage;
import cn.com.my.common.utils.GsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.nio.charset.Charset;
import java.util.Objects;

@Deprecated
@Slf4j
public class OGGMessageDeserializationSchema implements KafkaDeserializationSchema<OGGMessage> {

    @Override
    public boolean isEndOfStream(OGGMessage nextElement) {
        return false;
    }

    @Override
    public OGGMessage deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {

        byte[] value = record.value();
        byte[] key = record.key();
        OGGMessage oggMessage = new OGGMessage();
        if (!Objects.isNull(record)) {
            if (!Objects.isNull(value)) {
                if (log.isDebugEnabled()) {
                    log.debug("=====>json: {}", new String(value, Charset.forName("UTF-8")));
                }
                oggMessage = GsonUtil.fromJson(new String(value, Charset.forName("UTF-8")), OGGMessage.class);
                oggMessage.setOffset(record.offset());
                oggMessage.setTopicName(record.topic());
                oggMessage.setPartition(record.partition());
            }
            if (!Objects.isNull(key)) {
                if (log.isDebugEnabled()) {
                    log.debug("=====>key: {}", new String(key, Charset.forName("UTF-8")));
                }
                oggMessage.setKey(new String(key, Charset.forName("UTF-8")));
            }

        }
        return oggMessage;
    }

    @Override
    public TypeInformation<OGGMessage> getProducedType() {
        return TypeExtractor.getForClass(OGGMessage.class);
    }
}
